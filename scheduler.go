package main

// Expected behavior:
//   - The scheduler forwards due queue rows as SMS: every pollInterval it claims due messages one
//     by one and finishes each as sent, skipped, stale or failed, or puts it back for a retry.
//   - Messages received more than staleAfter ago are never sent. Late messages younger than that
//     (e.g. due while the process was down) are still forwarded.
//   - Temporary failures (ErrTemporarySendFailure, chat lookup errors) are retried after
//     retryBackoffs[attempt-1]; once the backoffs run out the message fails. Other errors fail it
//     immediately.
//   - Once per pruneInterval, finished rows older than pruneAfter are deleted.
//   - Every deliveryCheckInterval, sent messages without a final delivery status are checked with
//     Twilio and their status recorded. Undelivered/failed ones are logged loudly; ones still not
//     final deliveryTimeout after sending are recorded as unknown.
//
// Gotchas:
//   - The scheduler starts paused and only forwards after Resume, which the WhatsApp adapter calls
//     after OfflineSyncCompleted. Before that the offline backlog (including read receipts for
//     messages that are already due) hasn't arrived, and forwarding would send read messages.
//   - Pause/Resume come from whatsmeow's event goroutines, so the paused flag is atomic.
//   - A retry is a normal pending row, so a read receipt or reply during the backoff cancels it.
//   - Queue errors stop the current tick; the row stays as it is and the next tick tries again
//     (a row stuck in sending only becomes failed at the next startup).
//   - Delivery checks run even while paused: they only talk to Twilio, not WhatsApp.
//   - Run never cancels a tick in progress: cancelling a Twilio request mid-flight can leave an SMS
//     sent but recorded as a temporary failure, and the retry would send it twice. On shutdown the
//     current tick finishes, then Run returns.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"
)

const pollInterval = 5 * time.Second
const staleAfter = 12 * time.Hour
const pruneAfter = 30 * 24 * time.Hour
const pruneInterval = time.Hour
const deliveryCheckInterval = 30 * time.Second
const deliveryTimeout = 10 * time.Minute

// retryBackoffs[n-1] is the wait after the n-th failed attempt; failing beyond the last is final.
var retryBackoffs = []time.Duration{30 * time.Second, 2 * time.Minute, 10 * time.Minute}

// Clock returns the current time; injected so tests can move time.
type Clock func() time.Time

// SMSSender sends one SMS to the configured recipient (e.g. a dumb phone) and returns the Twilio SID.
// senderID is an alphanumeric sender ID, or "" to send from the Twilio number.
// Errors wrapping ErrTemporarySendFailure (network, 5xx, 429) are retried with backoff;
// any other error is final.
type SMSSender interface {
	Send(ctx context.Context, senderID string, body string) (string, error)
}

var ErrTemporarySendFailure = errors.New("temporary send failure")

// DeliveryChecker reads back the delivery status of a sent SMS by its Twilio SID.
type DeliveryChecker interface {
	DeliveryStatus(ctx context.Context, twilioSID string) (Delivery, error)
}

// Delivery is the carrier's view of a sent SMS, as reported by Twilio: Status is one of queued,
// accepted, sending, sent, delivered, undelivered, failed (and others for features not used here).
type Delivery struct {
	Status       string
	ErrorCode    int
	ErrorMessage string
}

// deliveryStatusUnknown is recorded when Twilio still hasn't reported a final status after
// deliveryTimeout (international delivery receipts sometimes never arrive).
const deliveryStatusUnknown = "unknown"

func (delivery Delivery) isFinal() bool {
	return delivery.Status == "delivered" || delivery.isFailed()
}

func (delivery Delivery) isFailed() bool {
	return delivery.Status == "undelivered" || delivery.Status == "failed"
}

// errorText is the delivery error as stored in the queue: "30003 Unreachable destination handset".
func (delivery Delivery) errorText() string {
	if delivery.ErrorCode == 0 {
		return delivery.ErrorMessage
	}
	return strings.TrimSpace(fmt.Sprintf("%d %s", delivery.ErrorCode, delivery.ErrorMessage))
}

type Scheduler struct {
	queue       *Queue
	chats       ChatLookup
	sms         SMSSender
	deliveries  DeliveryChecker
	now         Clock
	isPaused    atomic.Bool
	lastPruneAt time.Time

	lastDeliveryCheckAt time.Time
}

// Lifecycle
// ---------------------------------------------------------------------------

func NewScheduler(queue *Queue, chats ChatLookup, sms SMSSender, deliveries DeliveryChecker, now Clock) *Scheduler {
	scheduler := &Scheduler{queue: queue, chats: chats, sms: sms, deliveries: deliveries, now: now}
	scheduler.isPaused.Store(true)
	return scheduler
}

// Run calls Tick every pollInterval until ctx is cancelled, letting the current tick finish.
func (scheduler *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scheduler.Tick(context.WithoutCancel(ctx))
		}
	}
}

// Tick checks deliveries when due, then handles every due message once (not while paused).
func (scheduler *Scheduler) Tick(ctx context.Context) {
	scheduler.checkDeliveriesIfDue(ctx)
	if scheduler.isPaused.Load() {
		return
	}
	scheduler.pruneIfDue()
	for ctx.Err() == nil {
		message, isClaimed, err := scheduler.queue.ClaimNextDue(scheduler.now())
		if err != nil {
			log.Printf("queue error while claiming: %v", err)
			return
		}
		if !isClaimed {
			break
		}
		if err := scheduler.handle(ctx, message); err != nil {
			log.Printf("queue error while finishing %s: %v", message.MessageID, err)
			return
		}
	}
}

// Pause stops forwarding (WhatsApp disconnected; backlog not synced yet). Schedulers start paused.
func (scheduler *Scheduler) Pause() {
	if scheduler.isPaused.CompareAndSwap(false, true) {
		log.Printf("scheduler paused")
	}
}

// Resume starts forwarding (offline sync completed).
func (scheduler *Scheduler) Resume() {
	if scheduler.isPaused.CompareAndSwap(true, false) {
		log.Printf("scheduler resumed")
	}
}

// Forwarding
// ---------------------------------------------------------------------------

// handle decides the outcome of one claimed message. Returned errors are queue errors.
func (scheduler *Scheduler) handle(ctx context.Context, message Message) error {
	now := scheduler.now()
	if now.Sub(message.ReceivedAt) > staleAfter {
		log.Printf("stale %s", message.MessageID)
		return scheduler.queue.Finish(message.MessageID, StatusStale, "older than 12h", "", now)
	}

	chat, err := scheduler.chats.ChatInfo(ctx, message.ChatJID)
	if err != nil {
		return scheduler.retryOrFail(message, fmt.Errorf("chat info: %w", err))
	}
	if reason := skipReason(chat); reason != "" {
		log.Printf("skipped %s (%s)", message.MessageID, reason)
		return scheduler.queue.Finish(message.MessageID, StatusSkipped, reason, "", now)
	}

	twilioSID, err := scheduler.sms.Send(ctx, senderID(message.SenderName), smsBody(message, chat))
	if errors.Is(err, ErrTemporarySendFailure) {
		return scheduler.retryOrFail(message, err)
	}
	if err != nil {
		log.Printf("failed to forward %s: %v", message.MessageID, err)
		return scheduler.queue.Finish(message.MessageID, StatusFailed, err.Error(), "", scheduler.now())
	}
	log.Printf("forwarded %s from %s", message.MessageID, message.SenderName)
	return scheduler.queue.Finish(message.MessageID, StatusSent, "", twilioSID, scheduler.now())
}

// retryOrFail puts the message back with the backoff for its attempt, or fails it once the
// backoffs are used up.
func (scheduler *Scheduler) retryOrFail(message Message, cause error) error {
	now := scheduler.now()
	if message.Attempts > len(retryBackoffs) {
		log.Printf("failed to forward %s: %v", message.MessageID, cause)
		return scheduler.queue.Finish(message.MessageID, StatusFailed, cause.Error(), "", now)
	}
	backoff := retryBackoffs[message.Attempts-1]
	log.Printf("retrying %s in %s (%v)", message.MessageID, backoff, cause)
	return scheduler.queue.Retry(message.MessageID, now.Add(backoff), cause.Error(), now)
}

// smsBody formats the forwarded text: "<phone>: <text>", "<phone> @ <group>: <text>", or just the
// text when there is nothing to put in front of it.
func smsBody(message Message, chat ChatInfo) string {
	header := message.SenderPhone
	if chat.IsGroup {
		header = strings.TrimSpace(header + " @ " + chat.GroupName)
	}
	body := message.Text
	if header != "" {
		body = header + ": " + message.Text
	}
	return singleSegmentSMS(body)
}

// Housekeeping
// ---------------------------------------------------------------------------

func (scheduler *Scheduler) pruneIfDue() {
	now := scheduler.now()
	if !scheduler.lastPruneAt.IsZero() && now.Sub(scheduler.lastPruneAt) < pruneInterval {
		return
	}
	scheduler.lastPruneAt = now
	count, err := scheduler.queue.Prune(now.Add(-pruneAfter))
	if err != nil {
		log.Printf("queue error while pruning: %v", err)
		return
	}
	if count > 0 {
		log.Printf("pruned %d old messages", count)
	}
}

// Delivery
// ---------------------------------------------------------------------------

func (scheduler *Scheduler) checkDeliveriesIfDue(ctx context.Context) {
	now := scheduler.now()
	if now.Sub(scheduler.lastDeliveryCheckAt) < deliveryCheckInterval {
		return
	}
	scheduler.lastDeliveryCheckAt = now
	messages, err := scheduler.queue.UnconfirmedDeliveries()
	if err != nil {
		log.Printf("queue error while listing deliveries: %v", err)
		return
	}
	for _, message := range messages {
		if ctx.Err() != nil {
			return
		}
		scheduler.checkDelivery(ctx, message)
	}
}

func (scheduler *Scheduler) checkDelivery(ctx context.Context, message Message) {
	delivery, err := scheduler.deliveries.DeliveryStatus(ctx, message.TwilioSID)
	if err != nil {
		log.Printf("failed to check delivery of %s (%s): %v", message.MessageID, message.TwilioSID, err)
		return
	}
	now := scheduler.now()
	switch {
	case delivery.isFailed():
		log.Printf("NOT DELIVERED %s from %s: %s (%s)", message.MessageID, message.SenderName, delivery.Status, delivery.errorText())
	case delivery.Status == "delivered":
		log.Printf("delivered %s", message.MessageID)
	case now.Sub(message.SentAt) >= deliveryTimeout:
		log.Printf("delivery of %s unknown: still %s after %s", message.MessageID, delivery.Status, deliveryTimeout)
		delivery = Delivery{Status: deliveryStatusUnknown, ErrorMessage: "still " + delivery.Status + " after " + deliveryTimeout.String()}
	}
	if delivery.Status == message.DeliveryStatus {
		return
	}
	if err := scheduler.queue.RecordDelivery(message.MessageID, delivery.Status, delivery.errorText(), now); err != nil {
		log.Printf("queue error while recording delivery of %s: %v", message.MessageID, err)
	}
}
