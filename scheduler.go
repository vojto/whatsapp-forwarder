package main

// Expected behavior:
//   - The scheduler forwards due queue rows as SMS: every pollInterval it claims the next due
//     message together with every other pending message in its chat (a burst), and finishes the
//     whole burst as sent, skipped or failed, or puts it back for a retry. A burst is one SMS.
//   - Before sending, the forward judge (judge.go) decides whether the burst is worth an SMS. When
//     it says no, the burst is skipped with the probability as the reason. Without a judge, or
//     when it fails, the burst is forwarded.
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
//     A retried burst asks the judge again.
//   - The burst shares the oldest message's attempt count for backoff, since it was due first.
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
// senderID is the preferred alphanumeric sender ID, or "" for the default one.
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
	judge       ForwardJudge
	now         Clock
	isPaused    atomic.Bool
	lastPruneAt time.Time

	lastDeliveryCheckAt time.Time
}

// Lifecycle
// ---------------------------------------------------------------------------

// NewScheduler builds a paused scheduler. judge may be nil, which forwards every burst.
func NewScheduler(queue *Queue, chats ChatLookup, sms SMSSender, deliveries DeliveryChecker, judge ForwardJudge, now Clock) *Scheduler {
	scheduler := &Scheduler{queue: queue, chats: chats, sms: sms, deliveries: deliveries, judge: judge, now: now}
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
		burst, err := scheduler.queue.ClaimChatPending(message.ChatJID, scheduler.now())
		if err != nil {
			log.Printf("queue error while claiming the rest of %s: %v", message.ChatJID, err)
			return
		}
		if err := scheduler.handle(ctx, append([]Message{message}, burst...)); err != nil {
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

// handle decides the outcome of one claimed burst (oldest first). Returned errors are queue errors.
func (scheduler *Scheduler) handle(ctx context.Context, burst []Message) error {
	now := scheduler.now()
	var fresh []Message
	for _, message := range burst {
		if now.Sub(message.ReceivedAt) <= staleAfter {
			fresh = append(fresh, message)
			continue
		}
		log.Printf("stale %s", message.MessageID)
		if err := scheduler.queue.Finish(message.MessageID, StatusStale, "older than 12h", "", now); err != nil {
			return err
		}
	}
	if len(fresh) == 0 {
		return nil
	}

	chat, err := scheduler.chats.ChatInfo(ctx, fresh[0].ChatJID)
	if err != nil {
		return scheduler.retryOrFail(fresh, fmt.Errorf("chat info: %w", err))
	}
	reason := skipReason(chat)
	if reason == "" {
		reason = scheduler.judgeSkipReason(ctx, chat, fresh)
	}
	if reason != "" {
		log.Printf("skipped %d from %s (%s)", len(fresh), fresh[0].ChatJID, reason)
		return scheduler.finishAll(fresh, StatusSkipped, reason, "")
	}

	twilioSID, err := scheduler.sms.Send(ctx, burstSenderID(fresh, chat), smsBody(fresh, chat))
	if errors.Is(err, ErrTemporarySendFailure) {
		return scheduler.retryOrFail(fresh, err)
	}
	if err != nil {
		log.Printf("failed to forward %d from %s: %v", len(fresh), fresh[0].ChatJID, err)
		return scheduler.finishAll(fresh, StatusFailed, err.Error(), "")
	}
	log.Printf("forwarded %d from %s", len(fresh), fresh[len(fresh)-1].SenderName)
	return scheduler.finishAll(fresh, StatusSent, "", twilioSID)
}

// judgeSkipReason asks the forward judge about the burst and returns why it must not be sent, or
// "" to send it. Judge and history errors forward the burst.
func (scheduler *Scheduler) judgeSkipReason(ctx context.Context, chat ChatInfo, burst []Message) string {
	if scheduler.judge == nil {
		return ""
	}
	now := scheduler.now()
	history, err := scheduler.queue.RecentChat(burst[0].ChatJID, now.Add(-judgeContextWindow), judgeContextLimit)
	if err != nil {
		log.Printf("queue error while reading history, forwarding anyway: %v", err)
		return ""
	}
	probability, err := scheduler.judge.ForwardProbability(ctx, judgeState(chat, history, burst, now))
	if err != nil {
		log.Printf("judge failed, forwarding anyway: %v", err)
		return ""
	}
	log.Printf("judge: %.2f for %d from %s", probability, len(burst), burst[0].ChatJID)
	if probability > forwardThreshold {
		return ""
	}
	return fmt.Sprintf("not important (%.2f)", probability)
}

func (scheduler *Scheduler) finishAll(burst []Message, status MessageStatus, reason string, twilioSID string) error {
	now := scheduler.now()
	for _, message := range burst {
		if err := scheduler.queue.Finish(message.MessageID, status, reason, twilioSID, now); err != nil {
			return err
		}
	}
	return nil
}

// retryOrFail puts the burst back with the backoff for its oldest message's attempt, or fails it
// once the backoffs are used up.
func (scheduler *Scheduler) retryOrFail(burst []Message, cause error) error {
	now := scheduler.now()
	attempts := burst[0].Attempts
	if attempts > len(retryBackoffs) {
		log.Printf("failed to forward %d from %s: %v", len(burst), burst[0].ChatJID, cause)
		return scheduler.finishAll(burst, StatusFailed, cause.Error(), "")
	}
	backoff := retryBackoffs[attempts-1]
	log.Printf("retrying %d from %s in %s (%v)", len(burst), burst[0].ChatJID, backoff, cause)
	for _, message := range burst {
		if err := scheduler.queue.Retry(message.MessageID, now.Add(backoff), cause.Error(), now); err != nil {
			return err
		}
	}
	return nil
}

// Formatting
// ---------------------------------------------------------------------------

// smsBody formats a burst as one SMS: "<phone>: <text> / <text>", "<phone> @ <group>: <text>", or
// "@ <group>: <name>: <text> / <name>: <text>" when several people wrote. When it doesn't fit one
// segment, the oldest messages are dropped first and "..." marks the gap.
func smsBody(burst []Message, chat ChatInfo) string {
	isSingleSender := isSingleSender(burst)
	header := ""
	if isSingleSender {
		header = burst[0].SenderPhone
	}
	if chat.IsGroup {
		header = strings.TrimSpace(header + " @ " + chat.GroupName)
	}
	var parts []string
	for _, message := range burst {
		if toGSMText(message.Text) == "" {
			continue
		}
		part := message.Text
		if !isSingleSender {
			part = firstName(message.SenderName) + ": " + part
		}
		parts = append(parts, part)
	}
	for start := range parts {
		text := strings.Join(parts[start:], " / ")
		if start > 0 {
			text = "... " + text
		}
		if body := withHeader(header, text); fitsOneSegment(body) {
			return singleSegmentSMS(body)
		}
	}
	return singleSegmentSMS(withHeader(header, strings.Join(parts, " / ")))
}

// burstSenderID shows who wrote: the person, or the group name when several people did.
func burstSenderID(burst []Message, chat ChatInfo) string {
	if !isSingleSender(burst) {
		return senderID(chat.GroupName)
	}
	return senderID(burst[len(burst)-1].SenderName)
}

func withHeader(header string, text string) string {
	if header == "" {
		return text
	}
	return header + ": " + text
}

func isSingleSender(burst []Message) bool {
	for _, message := range burst {
		if message.SenderName != burst[0].SenderName || message.SenderPhone != burst[0].SenderPhone {
			return false
		}
	}
	return true
}

func firstName(name string) string {
	if words := strings.Fields(name); len(words) > 0 {
		return words[0]
	}
	return "?"
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
