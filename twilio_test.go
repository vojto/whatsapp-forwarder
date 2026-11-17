package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
	"time"
)

const testAccountSID = "AC123"
const testAuthToken = "secret-token-xyz"
const testFromNumber = "+15550001111"
const testRecipientNumber = "+421900000000"

type recordedTwilioRequest struct {
	path     string
	username string
	password string
	from     string
	to       string
	body     string
}

type twilioReply struct {
	httpStatus int
	body       string
}

// fakeTwilio answers each POST with the reply registered for its From field, and each GET of a
// message with the reply registered for its SID.
type fakeTwilio struct {
	server           *httptest.Server
	mutex            sync.Mutex
	repliesByFrom    map[string]twilioReply
	repliesBySID     map[string]twilioReply
	recordedRequests []recordedTwilioRequest
}

func newFakeTwilio(t *testing.T, repliesByFrom map[string]twilioReply) *fakeTwilio {
	fake := &fakeTwilio{repliesByFrom: repliesByFrom}
	fake.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if err := request.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}
		username, password, _ := request.BasicAuth()
		record := recordedTwilioRequest{
			path:     request.URL.Path,
			username: username,
			password: password,
			from:     request.PostForm.Get("From"),
			to:       request.PostForm.Get("To"),
			body:     request.PostForm.Get("Body"),
		}
		fake.mutex.Lock()
		reply, hasReply := fake.repliesByFrom[record.from]
		if request.Method == http.MethodGet {
			reply, hasReply = fake.repliesBySID[strings.TrimSuffix(path.Base(record.path), ".json")]
		} else {
			fake.recordedRequests = append(fake.recordedRequests, record)
		}
		fake.mutex.Unlock()
		if !hasReply {
			t.Errorf("unexpected %s %s From %q", request.Method, record.path, record.from)
			reply = errorReply(http.StatusInternalServerError, 0, "unexpected")
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(reply.httpStatus)
		writer.Write([]byte(reply.body))
	}))
	t.Cleanup(fake.server.Close)
	return fake
}

func (fake *fakeTwilio) requests() []recordedTwilioRequest {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	return append([]recordedTwilioRequest(nil), fake.recordedRequests...)
}

func newTestTwilioSender(apiBaseURL string) *TwilioSender {
	sender := NewTwilioSender(Config{
		TwilioAccountSID:   testAccountSID,
		TwilioAuthToken:    testAuthToken,
		TwilioFromNumber:   testFromNumber,
		SMSRecipientNumber: testRecipientNumber,
	})
	sender.apiBaseURL = apiBaseURL
	return sender
}

func successReply(sid string) twilioReply {
	return twilioReply{httpStatus: http.StatusCreated, body: fmt.Sprintf(`{"sid":%q,"status":"queued"}`, sid)}
}

func errorReply(httpStatus int, code int, message string) twilioReply {
	return twilioReply{
		httpStatus: httpStatus,
		body:       fmt.Sprintf(`{"code":%d,"message":%q,"status":%d}`, code, message, httpStatus),
	}
}

func assertFromSequence(t *testing.T, requests []recordedTwilioRequest, expectedFroms ...string) {
	t.Helper()
	if len(requests) != len(expectedFroms) {
		t.Fatalf("got %d requests, want %d", len(requests), len(expectedFroms))
	}
	for index, request := range requests {
		if request.from != expectedFroms[index] {
			t.Errorf("request %d From = %q, want %q", index, request.from, expectedFroms[index])
		}
	}
}

func assertNoAuthToken(t *testing.T, err error) {
	t.Helper()
	if err != nil && strings.Contains(err.Error(), testAuthToken) {
		t.Errorf("error leaks auth token: %v", err)
	}
}

func TestTwilioSendSuccessFromSenderID(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{"Mom": successReply("SM1")})

	sid, err := newTestTwilioSender(fake.server.URL).Send(context.Background(), "Mom", "hello")
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if sid != "SM1" {
		t.Errorf("sid = %q, want SM1", sid)
	}
	requests := fake.requests()
	assertFromSequence(t, requests, "Mom")
	request := requests[0]
	if request.path != "/2010-04-01/Accounts/"+testAccountSID+"/Messages.json" {
		t.Errorf("path = %q", request.path)
	}
	if request.username != testAccountSID || request.password != testAuthToken {
		t.Errorf("basic auth = %q/%q", request.username, request.password)
	}
	if request.to != testRecipientNumber || request.body != "hello" {
		t.Errorf("To/Body = %q/%q", request.to, request.body)
	}
}

func TestTwilioSendWithoutNameUsesFixedSenderID(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{fallbackSenderID: successReply("SM2")})

	sid, err := newTestTwilioSender(fake.server.URL).Send(context.Background(), "", "hello")
	if err != nil || sid != "SM2" {
		t.Fatalf("Send = %q, %v", sid, err)
	}
	assertFromSequence(t, fake.requests(), fallbackSenderID)
}

func TestTwilioSendRejectedNameFallsBackToFixedSenderID(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{
		"Mom":            errorReply(http.StatusBadRequest, 21612, "The 'From' is not valid for this destination"),
		fallbackSenderID: successReply("SM3"),
	})

	sid, err := newTestTwilioSender(fake.server.URL).Send(context.Background(), "Mom", "hello")
	if err != nil || sid != "SM3" {
		t.Fatalf("Send = %q, %v", sid, err)
	}
	assertFromSequence(t, fake.requests(), "Mom", fallbackSenderID)
}

func TestTwilioSendBothSenderIDsRejectedFallsBackToNumber(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{
		"Mom":            errorReply(http.StatusBadRequest, 21612, "The 'From' is not valid"),
		fallbackSenderID: errorReply(http.StatusBadRequest, 21612, "The 'From' is not valid"),
		testFromNumber:   successReply("SM3"),
	})

	sid, err := newTestTwilioSender(fake.server.URL).Send(context.Background(), "Mom", "hello")
	if err != nil || sid != "SM3" {
		t.Fatalf("Send = %q, %v", sid, err)
	}
	assertFromSequence(t, fake.requests(), "Mom", fallbackSenderID, testFromNumber)
}

// 21212 means the account can't use alphanumeric sender IDs at all: skip them for an hour, log once.
func TestTwilioSendInvalidFromNumberDisablesSenderIDsForCooldown(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{
		"Mom":            errorReply(http.StatusBadRequest, 21212, "Invalid From Number (caller ID): Mom"),
		"Dad":            errorReply(http.StatusBadRequest, 21212, "Invalid From Number (caller ID): Dad"),
		fallbackSenderID: errorReply(http.StatusBadRequest, 21212, "Invalid From Number (caller ID): WA"),
		testFromNumber:   successReply("SM4"),
	})
	var output bytes.Buffer
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(io.Discard) })
	clock := &fakeClock{now: startTime}
	sender := newTestTwilioSender(fake.server.URL)
	sender.now = clock.Now

	for _, senderID := range []string{"Mom", "Dad", ""} {
		if sid, err := sender.Send(context.Background(), senderID, "hello"); err != nil || sid != "SM4" {
			t.Fatalf("Send(%q) = %q, %v", senderID, sid, err)
		}
		clock.advance(10 * time.Minute)
	}
	assertFromSequence(t, fake.requests(), "Mom", testFromNumber, testFromNumber, testFromNumber)
	if count := strings.Count(output.String(), "disabled"); count != 1 {
		t.Errorf("logged the rejection %d times, want once:\n%s", count, output.String())
	}

	clock.advance(senderIDCooldown)
	if _, err := sender.Send(context.Background(), "Dad", "hello"); err != nil {
		t.Fatal(err)
	}
	requests := fake.requests()
	assertFromSequence(t, requests[4:], "Dad", testFromNumber)
}

func TestTwilioDeliveryStatus(t *testing.T) {
	fake := newFakeTwilio(t, nil)
	fake.repliesBySID = map[string]twilioReply{
		"SM1": {httpStatus: http.StatusOK, body: `{"sid":"SM1","status":"delivered","error_code":null,"error_message":null}`},
		"SM2": {httpStatus: http.StatusOK, body: `{"sid":"SM2","status":"undelivered","error_code":30003,"error_message":"Unreachable destination handset"}`},
		"SM3": errorReply(http.StatusNotFound, 20404, "The requested resource was not found"),
	}
	sender := newTestTwilioSender(fake.server.URL)

	delivered, err := sender.DeliveryStatus(context.Background(), "SM1")
	if err != nil || delivered != (Delivery{Status: "delivered"}) {
		t.Errorf("SM1 = %+v, %v", delivered, err)
	}
	undelivered, err := sender.DeliveryStatus(context.Background(), "SM2")
	expected := Delivery{Status: "undelivered", ErrorCode: 30003, ErrorMessage: "Unreachable destination handset"}
	if err != nil || undelivered != expected {
		t.Errorf("SM2 = %+v, %v", undelivered, err)
	}
	if !undelivered.isFailed() || undelivered.errorText() != "30003 Unreachable destination handset" {
		t.Errorf("SM2 isFailed %v, errorText %q", undelivered.isFailed(), undelivered.errorText())
	}
	if _, err := sender.DeliveryStatus(context.Background(), "SM3"); err == nil {
		t.Error("SM3: want error for 404")
	}
}

// The account rejects alphanumeric sender IDs: a forward still succeeds on its first attempt.
func TestRejectedSenderIDIsNotAFailedForwardAttempt(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{
		"Klaudia":      errorReply(http.StatusBadRequest, 21212, "Invalid From Number (caller ID): Klaudia"),
		testFromNumber: successReply("SM5"),
	})
	harness := newHarness(t)
	twilio := newTestTwilioSender(fake.server.URL)
	harness.scheduler = NewScheduler(harness.queue, harness.chats, twilio, twilio, harness.clock.Now)
	harness.scheduler.Resume()

	harness.add("M1", directChat, startTime)
	harness.advance(forwardDelay)

	message := harness.expectStatus("M1", StatusSent)
	if message.Attempts != 1 || message.TwilioSID != "SM5" || message.Reason != "" {
		t.Errorf("after fallback: attempts %d, sid %q, reason %q", message.Attempts, message.TwilioSID, message.Reason)
	}
	assertFromSequence(t, fake.requests(), "Klaudia", testFromNumber)
}

func TestTwilioSendServerErrorFromSenderIDIsTemporaryWithoutFallback(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{
		"Mom":          errorReply(http.StatusInternalServerError, 20500, "Internal Server Error"),
		testFromNumber: successReply("SM4"),
	})

	_, err := newTestTwilioSender(fake.server.URL).Send(context.Background(), "Mom", "hello")
	if !errors.Is(err, ErrTemporarySendFailure) {
		t.Fatalf("err = %v, want temporary", err)
	}
	assertNoAuthToken(t, err)
	assertFromSequence(t, fake.requests(), "Mom")
}

func TestTwilioSendTooManyRequestsIsTemporary(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{
		testFromNumber: errorReply(http.StatusTooManyRequests, 20429, "Too Many Requests"),
	})

	sender := newTestTwilioSender(fake.server.URL)
	sender.senderIDsDisabledUntil = time.Now().Add(time.Hour)
	_, err := sender.Send(context.Background(), "Mom", "hello")
	if !errors.Is(err, ErrTemporarySendFailure) {
		t.Fatalf("err = %v, want temporary", err)
	}
	assertFromSequence(t, fake.requests(), testFromNumber)
}

func TestTwilioSendRejectedFromNumberIsPermanent(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{
		"Mom":          errorReply(http.StatusBadRequest, 21212, "Invalid From Number"),
		testFromNumber: errorReply(http.StatusBadRequest, 21211, "Invalid 'To' Phone Number"),
	})

	_, err := newTestTwilioSender(fake.server.URL).Send(context.Background(), "Mom", "hello")
	if err == nil || errors.Is(err, ErrTemporarySendFailure) {
		t.Fatalf("err = %v, want permanent", err)
	}
	if !strings.Contains(err.Error(), "21211") || !strings.Contains(err.Error(), "Invalid 'To' Phone Number") {
		t.Errorf("err = %v, want Twilio code and message", err)
	}
	assertNoAuthToken(t, err)
	assertFromSequence(t, fake.requests(), "Mom", testFromNumber)
}

func TestTwilioSendNetworkErrorIsTemporary(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	apiBaseURL := server.URL
	server.Close()

	_, err := newTestTwilioSender(apiBaseURL).Send(context.Background(), "Mom", "hello")
	if !errors.Is(err, ErrTemporarySendFailure) {
		t.Fatalf("err = %v, want temporary", err)
	}
	assertNoAuthToken(t, err)
}
