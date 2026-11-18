package main

// Expected behavior:
//   - Send texts the body to the configured recipient (e.g. a dumb phone) through Twilio's Messages
//     API and returns the message SID. It tries the senders in order: the contact's name as an
//     alphanumeric sender ID, then the fixed sender ID "WA", then the Twilio number. All of that
//     happens within one Send call, so falling back never counts as a failed forward attempt.
//   - Twilio error 21212 on an alphanumeric sender means the account can't use sender IDs at all:
//     they're skipped for senderIDCooldown (logged once), and every send goes straight from the
//     number. After the cooldown one send probes again, so enabling sender IDs needs no restart.
//   - Network errors, 5xx and 429 wrap ErrTemporarySendFailure so the scheduler retries later;
//     any other 4xx from the number is a permanent error.
//   - DeliveryStatus reads a sent message's delivery status back (there is no public URL for a
//     StatusCallback, so the scheduler polls).
//
// Gotchas:
//   - Every rejected request costs money ($0.001), which is why a 21212 is cached instead of
//     being rediscovered before every SMS.
//   - Other 4xx on a sender ID (e.g. a name Twilio won't accept) only skip that one sender ID.
//   - A temporary failure on a sender ID must not fall back: the scheduler's retry tries the same
//     chain again, and falling back would otherwise hide outages.
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
	"sync"
	"time"
)

const twilioAPIBaseURL = "https://api.twilio.com"
const twilioRequestTimeout = 20 * time.Second
const fallbackSenderID = "WA"
const senderIDCooldown = time.Hour
const twilioInvalidFromNumberCode = 21212

type TwilioSender struct {
	accountSID      string
	authToken       string
	fromNumber      string
	recipientNumber string
	apiBaseURL      string
	httpClient      *http.Client
	now             Clock

	mutex                  sync.Mutex
	senderIDsDisabledUntil time.Time
}

// twilioRejection is a non-temporary 4xx response: the request itself was refused.
type twilioRejection struct {
	httpStatus int
	code       int
	message    string
}

func (rejection *twilioRejection) Error() string {
	return fmt.Sprintf("twilio rejected request (HTTP %d, code %d): %s", rejection.httpStatus, rejection.code, rejection.message)
}

// twilioMessageResponse covers the message resource (sid, status, error_code, error_message)
// and the error body (code, message). The error body's status field repeats the HTTP status and
// would not parse as a string, so error bodies are decoded into twilioErrorResponse instead.
type twilioMessageResponse struct {
	SID          string  `json:"sid"`
	Status       string  `json:"status"`
	ErrorCode    *int    `json:"error_code"`
	ErrorMessage *string `json:"error_message"`
}

type twilioErrorResponse struct {
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
		now:             time.Now,
	}
}

// Send texts the body to the recipient from the first sender Twilio accepts: senderID, "WA",
// then the Twilio number.
func (sender *TwilioSender) Send(ctx context.Context, senderID string, body string) (string, error) {
	for _, alphanumericSender := range sender.alphanumericSenders(senderID) {
		message, err := sender.postMessage(ctx, alphanumericSender, body)
		var rejection *twilioRejection
		if !errors.As(err, &rejection) {
			return message.SID, err
		}
		if rejection.code == twilioInvalidFromNumberCode {
			sender.disableSenderIDs(err)
			break
		}
		log.Printf("sender ID %q rejected, trying the next sender: %v", alphanumericSender, err)
	}
	message, err := sender.postMessage(ctx, sender.fromNumber, body)
	return message.SID, err
}

// DeliveryStatus returns Twilio's current delivery status for a sent message.
func (sender *TwilioSender) DeliveryStatus(ctx context.Context, twilioSID string) (Delivery, error) {
	endpoint := sender.messagesURL() + "/" + url.PathEscape(twilioSID) + ".json"
	message, err := sender.do(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Delivery{}, err
	}
	delivery := Delivery{Status: message.Status}
	if message.ErrorCode != nil {
		delivery.ErrorCode = *message.ErrorCode
	}
	if message.ErrorMessage != nil {
		delivery.ErrorMessage = *message.ErrorMessage
	}
	return delivery, nil
}

// Sender IDs
// ---------------------------------------------------------------------------

func (sender *TwilioSender) alphanumericSenders(senderID string) []string {
	sender.mutex.Lock()
	defer sender.mutex.Unlock()
	if sender.now().Before(sender.senderIDsDisabledUntil) {
		return nil
	}
	if senderID == "" || senderID == fallbackSenderID {
		return []string{fallbackSenderID}
	}
	return []string{senderID, fallbackSenderID}
}

func (sender *TwilioSender) disableSenderIDs(cause error) {
	sender.mutex.Lock()
	defer sender.mutex.Unlock()
	sender.senderIDsDisabledUntil = sender.now().Add(senderIDCooldown)
	log.Printf("alphanumeric sender IDs disabled on the Twilio account, sending from the number for %s: %v",
		senderIDCooldown, cause)
}

// Twilio API
// ---------------------------------------------------------------------------

func (sender *TwilioSender) postMessage(ctx context.Context, from string, body string) (twilioMessageResponse, error) {
	form := url.Values{"From": {from}, "To": {sender.recipientNumber}, "Body": {body}}
	message, err := sender.do(ctx, http.MethodPost, sender.messagesURL()+".json", form)
	if err == nil && message.SID == "" {
		return message, errors.New("twilio response has no sid")
	}
	return message, err
}

func (sender *TwilioSender) messagesURL() string {
	return fmt.Sprintf("%s/2010-04-01/Accounts/%s/Messages", sender.apiBaseURL, url.PathEscape(sender.accountSID))
}

func (sender *TwilioSender) do(ctx context.Context, method string, endpoint string, form url.Values) (twilioMessageResponse, error) {
	var requestBody io.Reader
	if form != nil {
		requestBody = strings.NewReader(form.Encode())
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, requestBody)
	if err != nil {
		return twilioMessageResponse{}, err
	}
	request.SetBasicAuth(sender.accountSID, sender.authToken)
	if form != nil {
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	response, err := sender.httpClient.Do(request)
	if err != nil {
		return twilioMessageResponse{}, fmt.Errorf("%w: twilio request: %v", ErrTemporarySendFailure, err)
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return twilioMessageResponse{}, fmt.Errorf("%w: read twilio response: %v", ErrTemporarySendFailure, err)
	}
	return parseTwilioResponse(response.StatusCode, responseBody)
}

// Response parsing
// ---------------------------------------------------------------------------

func parseTwilioResponse(httpStatus int, responseBody []byte) (twilioMessageResponse, error) {
	if httpStatus >= 200 && httpStatus < 300 {
		var message twilioMessageResponse
		if err := json.Unmarshal(responseBody, &message); err != nil {
			return message, fmt.Errorf("twilio HTTP %d: unreadable response: %s", httpStatus, truncatedBody(responseBody))
		}
		return message, nil
	}

	var parsed twilioErrorResponse
	errorMessage := truncatedBody(responseBody)
	if json.Unmarshal(responseBody, &parsed) == nil && parsed.Message != "" {
		errorMessage = parsed.Message
	}
	if httpStatus == http.StatusTooManyRequests || httpStatus >= 500 {
		return twilioMessageResponse{}, fmt.Errorf("%w: twilio HTTP %d, code %d: %s", ErrTemporarySendFailure, httpStatus, parsed.Code, errorMessage)
	}
	return twilioMessageResponse{}, &twilioRejection{httpStatus: httpStatus, code: parsed.Code, message: errorMessage}
}

func truncatedBody(responseBody []byte) string {
	const maxLength = 300
	text := strings.TrimSpace(string(responseBody))
	if len(text) > maxLength {
		return text[:maxLength] + "..."
	}
	return text
}
