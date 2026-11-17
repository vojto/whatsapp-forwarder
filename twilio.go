package main

// Expected behavior:
//   - Send texts the body to the configured recipient (Vojto's Nokia) through Twilio's Messages
//     API and returns the message SID.
//   - With a sender ID, Twilio is asked to show it as the sender. If Twilio rejects the request
//     with any 4xx other than 429, Send immediately retries once from the Twilio number.
//   - Network errors, 5xx and 429 wrap ErrTemporarySendFailure so the scheduler retries later;
//     any other 4xx from the number is a permanent error.
//
// Gotchas:
//   - Twilio rejects alphanumeric sender IDs for some routes (codes like 21212, 21612, 21659),
//     but the codes aren't stable enough to rely on, so every non-temporary 4xx falls back.
//   - A temporary failure from the sender ID must not fall back: the scheduler's retry will try
//     the sender ID again, and the number fallback would otherwise hide outages.
//   - Errors must never include the auth token; it only travels in the basic auth header.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const twilioAPIBaseURL = "https://api.twilio.com"
const twilioRequestTimeout = 20 * time.Second

type TwilioSender struct {
	accountSID      string
	authToken       string
	fromNumber      string
	recipientNumber string
	apiBaseURL      string
	httpClient      *http.Client
}

// twilioRejection is a non-temporary 4xx response: the request itself was refused.
type twilioRejection struct {
	httpStatus int
	code       int
	message    string
}

func (rejection *twilioRejection) Error() string {
	return fmt.Sprintf("twilio rejected message (HTTP %d, code %d): %s", rejection.httpStatus, rejection.code, rejection.message)
}

// twilioMessageResponse covers both the success body (sid) and the error body (code, message).
// The error body's status field repeats the HTTP status, so it isn't read.
type twilioMessageResponse struct {
	SID     string `json:"sid"`
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Lifecycle
// ---------------------------------------------------------------------------

func NewTwilioSender(config Config) *TwilioSender {
	return &TwilioSender{
		accountSID:      config.TwilioAccountSID,
		authToken:       config.TwilioAuthToken,
		fromNumber:      config.TwilioFromNumber,
		recipientNumber: config.SMSRecipientNumber,
		apiBaseURL:      twilioAPIBaseURL,
		httpClient:      &http.Client{Timeout: twilioRequestTimeout},
	}
}

// Send texts the body to the recipient, showing senderID as the sender, or the Twilio number
// when senderID is empty or rejected.
func (sender *TwilioSender) Send(ctx context.Context, senderID string, body string) (string, error) {
	if senderID == "" {
		return sender.post(ctx, sender.fromNumber, body)
	}

	sid, err := sender.post(ctx, senderID, body)
	var rejection *twilioRejection
	if !errors.As(err, &rejection) {
		return sid, err
	}
	log.Printf("sender ID %q rejected, using number: %v", senderID, err)
	return sender.post(ctx, sender.fromNumber, body)
}

// Twilio API
// ---------------------------------------------------------------------------

func (sender *TwilioSender) post(ctx context.Context, from string, body string) (string, error) {
	endpoint := fmt.Sprintf("%s/2010-04-01/Accounts/%s/Messages.json", sender.apiBaseURL, url.PathEscape(sender.accountSID))
	form := url.Values{"From": {from}, "To": {sender.recipientNumber}, "Body": {body}}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	request.SetBasicAuth(sender.accountSID, sender.authToken)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	response, err := sender.httpClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("%w: twilio request: %v", ErrTemporarySendFailure, err)
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("%w: read twilio response: %v", ErrTemporarySendFailure, err)
	}
	return parseTwilioResponse(response.StatusCode, responseBody)
}

// Response parsing
// ---------------------------------------------------------------------------

func parseTwilioResponse(httpStatus int, responseBody []byte) (string, error) {
	var parsed twilioMessageResponse
	isParsed := json.Unmarshal(responseBody, &parsed) == nil

	if httpStatus >= 200 && httpStatus < 300 {
		if !isParsed || parsed.SID == "" {
			return "", fmt.Errorf("twilio HTTP %d: response has no sid: %s", httpStatus, truncatedBody(responseBody))
		}
		return parsed.SID, nil
	}

	message := parsed.Message
	if !isParsed || message == "" {
		message = truncatedBody(responseBody)
	}
	if httpStatus == http.StatusTooManyRequests || httpStatus >= 500 {
		return "", fmt.Errorf("%w: twilio HTTP %d, code %d: %s", ErrTemporarySendFailure, httpStatus, parsed.Code, message)
	}
	return "", &twilioRejection{httpStatus: httpStatus, code: parsed.Code, message: message}
}

func truncatedBody(responseBody []byte) string {
	const maxLength = 300
	text := strings.TrimSpace(string(responseBody))
	if len(text) > maxLength {
		return text[:maxLength] + "..."
	}
	return text
}
