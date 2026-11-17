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
	"strings"
	"sync"
	"testing"
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

// fakeTwilio answers each request with the reply registered for its From field.
type fakeTwilio struct {
	server           *httptest.Server
	mutex            sync.Mutex
	repliesByFrom    map[string]twilioReply
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
		fake.recordedRequests = append(fake.recordedRequests, record)
		reply, hasReply := fake.repliesByFrom[record.from]
		fake.mutex.Unlock()
		if !hasReply {
			t.Errorf("unexpected From %q", record.from)
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

func TestTwilioSendEmptySenderIDUsesNumber(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{testFromNumber: successReply("SM2")})

	sid, err := newTestTwilioSender(fake.server.URL).Send(context.Background(), "", "hello")
	if err != nil || sid != "SM2" {
		t.Fatalf("Send = %q, %v", sid, err)
	}
	assertFromSequence(t, fake.requests(), testFromNumber)
}

func TestTwilioSendRejectedSenderIDFallsBackToNumber(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{
		"Mom":          errorReply(http.StatusBadRequest, 21212, "Invalid From Number"),
		testFromNumber: successReply("SM3"),
	})

	sid, err := newTestTwilioSender(fake.server.URL).Send(context.Background(), "Mom", "hello")
	if err != nil || sid != "SM3" {
		t.Fatalf("Send = %q, %v", sid, err)
	}
	assertFromSequence(t, fake.requests(), "Mom", testFromNumber)
}

func TestTwilioSendAfterRejectionSkipsSenderIDsAndLogsOnce(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{
		"Mom":          errorReply(http.StatusBadRequest, 21212, "Invalid From Number (caller ID): Mom"),
		"Dad":          errorReply(http.StatusBadRequest, 21212, "Invalid From Number (caller ID): Dad"),
		testFromNumber: successReply("SM4"),
	})
	var output bytes.Buffer
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(io.Discard) })
	sender := newTestTwilioSender(fake.server.URL)

	for _, senderID := range []string{"Mom", "Dad", "Mom"} {
		if sid, err := sender.Send(context.Background(), senderID, "hello"); err != nil || sid != "SM4" {
			t.Fatalf("Send(%q) = %q, %v", senderID, sid, err)
		}
	}
	assertFromSequence(t, fake.requests(), "Mom", testFromNumber, testFromNumber, testFromNumber)
	if count := strings.Count(output.String(), "sender ID"); count != 1 {
		t.Errorf("logged the rejection %d times, want once:\n%s", count, output.String())
	}
}

// The account rejects alphanumeric sender IDs: a forward still succeeds on its first attempt.
func TestRejectedSenderIDIsNotAFailedForwardAttempt(t *testing.T) {
	fake := newFakeTwilio(t, map[string]twilioReply{
		"Klaudia":      errorReply(http.StatusBadRequest, 21212, "Invalid From Number (caller ID): Klaudia"),
		testFromNumber: successReply("SM5"),
	})
	harness := newHarness(t)
	harness.scheduler = NewScheduler(harness.queue, harness.chats, newTestTwilioSender(fake.server.URL), harness.clock.Now)
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

	_, err := newTestTwilioSender(fake.server.URL).Send(context.Background(), "", "hello")
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
