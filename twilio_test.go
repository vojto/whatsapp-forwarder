package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
)

const testAccountSID = "AC123"
const testAuthToken = "secret-token-xyz"
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

func TestTwilioSendBothSenderIDsRejectedIsPermanent(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{
		"Mom":            errorReply(http.StatusBadRequest, 21612, "The 'From' is not valid"),
		fallbackSenderID: errorReply(http.StatusBadRequest, 21211, "Invalid 'To' Phone Number"),
	})

	_, err := newTestTwilioSender(fake.server.URL).Send(context.Background(), "Mom", "hello")
	if err == nil || errors.Is(err, ErrTemporarySendFailure) {
		t.Fatalf("err = %v, want permanent", err)
	}
	if !strings.Contains(err.Error(), "21211") || !strings.Contains(err.Error(), "Invalid 'To' Phone Number") {
		t.Errorf("err = %v, want Twilio code and message", err)
	}
	assertNoAuthToken(t, err)
	assertFromSequence(t, fake.requests(), "Mom", fallbackSenderID)
}

// 21212 means the account can't use sender IDs at all: "WA" would fail too, so it isn't tried, and
// the error is temporary so the scheduler retries instead of dropping the message.
func TestTwilioSendSenderIDsDisabledIsTemporary(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{
		"Mom": errorReply(http.StatusBadRequest, 21212, "Invalid From Number (caller ID): Mom"),
	})

	_, err := newTestTwilioSender(fake.server.URL).Send(context.Background(), "Mom", "hello")
	if !errors.Is(err, ErrTemporarySendFailure) {
		t.Fatalf("err = %v, want temporary", err)
	}
	if !strings.Contains(err.Error(), "21212") {
		t.Errorf("err = %v, want Twilio code", err)
	}
	assertNoAuthToken(t, err)
	assertFromSequence(t, fake.requests(), "Mom")
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

// A rejected name falls back to "WA" within the same attempt.
func TestRejectedSenderIDIsNotAFailedForwardAttempt(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{
		"Klaudia":        errorReply(http.StatusBadRequest, 21612, "The 'From' is not valid for this destination"),
		fallbackSenderID: successReply("SM5"),
	})
	harness := newHarness(t)
	twilio := newTestTwilioSender(fake.server.URL)
	harness.scheduler = NewScheduler(harness.queue, harness.chats, twilio, twilio, nil, harness.clock.Now)
	harness.scheduler.Resume()

	harness.add("M1", directChat, startTime)
	harness.advance(forwardDelay)

	message := harness.expectStatus("M1", StatusSent)
	if message.Attempts != 1 || message.TwilioSID != "SM5" || message.Reason != "" {
		t.Errorf("after fallback: attempts %d, sid %q, reason %q", message.Attempts, message.TwilioSID, message.Reason)
	}
	assertFromSequence(t, fake.requests(), "Klaudia", fallbackSenderID)
}

// Sender IDs disabled on the account: the forward is retried with backoff, and goes out once the
// account is fixed.
func TestSenderIDsDisabledIsRetriedUntilFixed(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{
		"Klaudia": errorReply(http.StatusBadRequest, 21212, "Invalid From Number (caller ID): Klaudia"),
	})
	harness := newHarness(t)
	twilio := newTestTwilioSender(fake.server.URL)
	harness.scheduler = NewScheduler(harness.queue, harness.chats, twilio, twilio, nil, harness.clock.Now)
	harness.scheduler.Resume()

	harness.add("M1", directChat, startTime)
	harness.advance(forwardDelay)
	message := harness.expectStatus("M1", StatusPending)
	if message.Attempts != 1 || !strings.Contains(message.Reason, "21212") {
		t.Errorf("after first attempt: attempts %d, reason %q", message.Attempts, message.Reason)
	}

	fake.mutex.Lock()
	fake.repliesByFrom["Klaudia"] = successReply("SM6")
	fake.mutex.Unlock()
	harness.advance(retryBackoffs[0])

	message = harness.expectStatus("M1", StatusSent)
	if message.Attempts != 2 || message.TwilioSID != "SM6" {
		t.Errorf("after retry: attempts %d, sid %q", message.Attempts, message.TwilioSID)
	}
	assertFromSequence(t, fake.requests(), "Klaudia", "Klaudia")
}

func TestTwilioSendServerErrorFromSenderIDIsTemporaryWithoutFallback(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{
		"Mom":            errorReply(http.StatusInternalServerError, 20500, "Internal Server Error"),
		fallbackSenderID: successReply("SM4"),
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
		fallbackSenderID: errorReply(http.StatusTooManyRequests, 20429, "Too Many Requests"),
	})

	_, err := newTestTwilioSender(fake.server.URL).Send(context.Background(), "", "hello")
	if !errors.Is(err, ErrTemporarySendFailure) {
		t.Fatalf("err = %v, want temporary", err)
	}
	assertFromSequence(t, fake.requests(), fallbackSenderID)
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
