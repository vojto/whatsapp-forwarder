package main

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var baseTime = time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)

// Adding messages
// ---------------------------------------------------------------------------

func TestAddStoresPendingMessageDueAfterForwardDelay(t *testing.T) {
	queue := openTestQueue(t)
	message := testMessage("message-1", "chat-1", baseTime)

	wasAdded := mustAdd(t, queue, message, baseTime.Add(time.Second))
	if !wasAdded {
		t.Fatal("Add returned false for a new message")
	}

	stored := mustGet(t, queue, "message-1")
	if stored.Status != StatusPending {
		t.Errorf("status = %q, want pending", stored.Status)
	}
	if !stored.DueAt.Equal(baseTime.Add(3 * time.Minute)) {
		t.Errorf("due_at = %v, want %v", stored.DueAt, baseTime.Add(3*time.Minute))
	}
	if stored.ChatJID != "chat-1" || stored.SenderName != "Sender" || stored.SenderPhone != "+421900000000" ||
		stored.Text != "text of message-1" || stored.Attempts != 0 || stored.Reason != "" || stored.TwilioSID != "" {
		t.Errorf("stored message fields differ: %+v", stored)
	}
	if !stored.UpdatedAt.Equal(baseTime.Add(time.Second)) {
		t.Errorf("updated_at = %v, want %v", stored.UpdatedAt, baseTime.Add(time.Second))
	}
}

func TestAddDuplicateIsIgnored(t *testing.T) {
	queue := openTestQueue(t)
	mustAdd(t, queue, testMessage("message-1", "chat-1", baseTime), baseTime)

	redelivered := testMessage("message-1", "chat-other", baseTime.Add(time.Hour))
	redelivered.Text = "redelivered text"
	if mustAdd(t, queue, redelivered, baseTime.Add(time.Hour)) {
		t.Fatal("duplicate Add returned true")
	}

	stored := mustGet(t, queue, "message-1")
	if stored.Text != "text of message-1" || stored.ChatJID != "chat-1" || !stored.ReceivedAt.Equal(baseTime) {
		t.Errorf("duplicate Add overwrote the first copy: %+v", stored)
	}
}

func TestTimesRoundTripAtMillisecondPrecision(t *testing.T) {
	queue := openTestQueue(t)
	receivedAt := baseTime.Add(123*time.Millisecond + 456*time.Microsecond)
	mustAdd(t, queue, testMessage("message-1", "chat-1", receivedAt), receivedAt)

	stored := mustGet(t, queue, "message-1")
	expectedReceivedAt := receivedAt.Truncate(time.Millisecond)
	if !stored.ReceivedAt.Equal(expectedReceivedAt) {
		t.Errorf("received_at = %v, want %v", stored.ReceivedAt, expectedReceivedAt)
	}
	if !stored.DueAt.Equal(expectedReceivedAt.Add(forwardDelay)) {
		t.Errorf("due_at = %v, want %v", stored.DueAt, expectedReceivedAt.Add(forwardDelay))
	}
	if !stored.UpdatedAt.Equal(expectedReceivedAt) {
		t.Errorf("updated_at = %v, want %v", stored.UpdatedAt, expectedReceivedAt)
	}
}

func TestGetMissingMessageReturnsFalse(t *testing.T) {
	queue := openTestQueue(t)
	_, wasFound, err := queue.Get("missing")
	if err != nil {
		t.Fatal(err)
	}
	if wasFound {
		t.Error("Get returned true for a missing message")
	}
}

// Read receipts and replies
// ---------------------------------------------------------------------------

func TestMarkReadCancelsOnlyPendingMessages(t *testing.T) {
	queue := openTestQueue(t)
	mustAdd(t, queue, testMessage("pending", "chat-1", baseTime), baseTime)
	mustAdd(t, queue, testMessage("sending", "chat-1", baseTime.Add(-time.Hour)), baseTime)
	mustAdd(t, queue, testMessage("sent", "chat-1", baseTime.Add(-2*time.Hour)), baseTime)

	claimNow := baseTime.Add(-time.Hour + forwardDelay)
	sentMessage := mustClaim(t, queue, claimNow)
	if sentMessage.MessageID != "sent" {
		t.Fatalf("claimed %q first, want sent", sentMessage.MessageID)
	}
	mustFinish(t, queue, "sent", StatusSent, "", "SM123", claimNow)
	if sendingMessage := mustClaim(t, queue, claimNow); sendingMessage.MessageID != "sending" {
		t.Fatalf("claimed %q, want sending", sendingMessage.MessageID)
	}

	cancelled, err := queue.MarkRead([]string{"pending", "sending", "sent"}, baseTime.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if cancelled != 1 {
		t.Errorf("cancelled = %d, want 1", cancelled)
	}
	expectStatus(t, queue, "pending", StatusRead)
	expectStatus(t, queue, "sending", StatusSending)
	expectStatus(t, queue, "sent", StatusSent)
	if sent := mustGet(t, queue, "sent"); sent.TwilioSID != "SM123" {
		t.Errorf("twilio_sid = %q, want SM123", sent.TwilioSID)
	}
}

func TestMarkReadBeforeMessageLeavesReadPlaceholder(t *testing.T) {
	queue := openTestQueue(t)
	readAt := baseTime.Add(-time.Minute)

	cancelled, err := queue.MarkRead([]string{"early"}, readAt)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled != 0 {
		t.Errorf("cancelled = %d, want 0 for an unknown message", cancelled)
	}
	expectStatus(t, queue, "early", StatusRead)

	if mustAdd(t, queue, testMessage("early", "chat-1", baseTime), baseTime) {
		t.Error("Add after read placeholder returned true")
	}
	expectStatus(t, queue, "early", StatusRead)
	if _, wasClaimed := claim(t, queue, baseTime.Add(time.Hour)); wasClaimed {
		t.Error("read placeholder was claimed for sending")
	}
}

func TestMarkReadTwiceCountsOnlyFirstCancellation(t *testing.T) {
	queue := openTestQueue(t)
	mustAdd(t, queue, testMessage("message-1", "chat-1", baseTime), baseTime)

	first, err := queue.MarkRead([]string{"message-1"}, baseTime)
	if err != nil {
		t.Fatal(err)
	}
	second, err := queue.MarkRead([]string{"message-1"}, baseTime)
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 || second != 0 {
		t.Errorf("cancelled = %d then %d, want 1 then 0", first, second)
	}
}

func TestMarkRepliedAffectsOnlyPendingMessagesInChatUpToReplyTime(t *testing.T) {
	queue := openTestQueue(t)
	repliedAt := baseTime.Add(time.Minute)
	mustAdd(t, queue, testMessage("before", "chat-1", baseTime), baseTime)
	mustAdd(t, queue, testMessage("exactly-at", "chat-1", repliedAt), repliedAt)
	mustAdd(t, queue, testMessage("after", "chat-1", repliedAt.Add(time.Millisecond)), repliedAt)
	mustAdd(t, queue, testMessage("other-chat", "chat-2", baseTime), baseTime)
	mustAdd(t, queue, testMessage("already-read", "chat-1", baseTime), baseTime)
	if _, err := queue.MarkRead([]string{"already-read"}, baseTime); err != nil {
		t.Fatal(err)
	}

	replied, err := queue.MarkReplied("chat-1", repliedAt, repliedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if replied != 2 {
		t.Errorf("replied = %d, want 2", replied)
	}
	expectStatus(t, queue, "before", StatusReplied)
	expectStatus(t, queue, "exactly-at", StatusReplied)
	expectStatus(t, queue, "after", StatusPending)
	expectStatus(t, queue, "other-chat", StatusPending)
	expectStatus(t, queue, "already-read", StatusRead)
}

// Scheduling
// ---------------------------------------------------------------------------

func TestClaimNextDueReturnsEarliestDueFirst(t *testing.T) {
	queue := openTestQueue(t)
	mustAdd(t, queue, testMessage("second", "chat-1", baseTime.Add(time.Second)), baseTime)
	mustAdd(t, queue, testMessage("first", "chat-1", baseTime), baseTime)
	mustAdd(t, queue, testMessage("future", "chat-1", baseTime.Add(time.Hour)), baseTime)

	now := baseTime.Add(forwardDelay + time.Second)
	first := mustClaim(t, queue, now)
	if first.MessageID != "first" || first.Status != StatusSending || first.Attempts != 1 {
		t.Errorf("first claim = %+v, want first/sending/1 attempt", first)
	}
	expectStatus(t, queue, "first", StatusSending)

	second := mustClaim(t, queue, now)
	if second.MessageID != "second" {
		t.Errorf("second claim = %q, want second", second.MessageID)
	}

	if _, wasClaimed := claim(t, queue, now); wasClaimed {
		t.Error("claimed a message that is not due yet")
	}
}

func TestClaimNextDueOnEmptyQueueReturnsFalse(t *testing.T) {
	queue := openTestQueue(t)
	if _, wasClaimed := claim(t, queue, baseTime); wasClaimed {
		t.Error("claimed from an empty queue")
	}
}

func TestClaimNextDueIncludesMessageDueExactlyNow(t *testing.T) {
	queue := openTestQueue(t)
	mustAdd(t, queue, testMessage("message-1", "chat-1", baseTime), baseTime)

	if _, wasClaimed := claim(t, queue, baseTime.Add(forwardDelay-time.Millisecond)); wasClaimed {
		t.Error("claimed a message one millisecond before it was due")
	}
	if _, wasClaimed := claim(t, queue, baseTime.Add(forwardDelay)); !wasClaimed {
		t.Error("did not claim a message due exactly now")
	}
}

func TestFinishOnlyActsOnSendingMessages(t *testing.T) {
	queue := openTestQueue(t)
	mustAdd(t, queue, testMessage("pending", "chat-1", baseTime), baseTime)
	mustFinish(t, queue, "pending", StatusSent, "", "SM1", baseTime)
	expectStatus(t, queue, "pending", StatusPending)

	now := baseTime.Add(forwardDelay)
	mustClaim(t, queue, now)
	mustFinish(t, queue, "pending", StatusFailed, "twilio error", "", now)
	failed := mustGet(t, queue, "pending")
	if failed.Status != StatusFailed || failed.Reason != "twilio error" {
		t.Errorf("after Finish: %+v", failed)
	}

	mustFinish(t, queue, "pending", StatusSent, "", "SM2", now)
	expectStatus(t, queue, "pending", StatusFailed)

	if _, err := queue.MarkRead([]string{"read"}, now); err != nil {
		t.Fatal(err)
	}
	mustFinish(t, queue, "read", StatusSent, "", "SM3", now)
	expectStatus(t, queue, "read", StatusRead)
}

func TestRetryPutsSendingMessageBackToPending(t *testing.T) {
	queue := openTestQueue(t)
	mustAdd(t, queue, testMessage("message-1", "chat-1", baseTime), baseTime)

	if err := queue.Retry("message-1", baseTime.Add(time.Hour), "not sending", baseTime); err != nil {
		t.Fatal(err)
	}
	unchanged := mustGet(t, queue, "message-1")
	if unchanged.Reason != "" || !unchanged.DueAt.Equal(baseTime.Add(forwardDelay)) {
		t.Errorf("Retry changed a pending message: %+v", unchanged)
	}

	now := baseTime.Add(forwardDelay)
	mustClaim(t, queue, now)
	retryAt := now.Add(time.Minute)
	if err := queue.Retry("message-1", retryAt, "twilio 503", now); err != nil {
		t.Fatal(err)
	}
	retried := mustGet(t, queue, "message-1")
	if retried.Status != StatusPending || !retried.DueAt.Equal(retryAt) || retried.Reason != "twilio 503" || retried.Attempts != 1 {
		t.Errorf("after Retry: %+v", retried)
	}

	if _, wasClaimed := claim(t, queue, retryAt.Add(-time.Millisecond)); wasClaimed {
		t.Error("claimed a retried message before its new due time")
	}
	reclaimed := mustClaim(t, queue, retryAt)
	if reclaimed.Attempts != 2 {
		t.Errorf("attempts after second claim = %d, want 2", reclaimed.Attempts)
	}
}

func TestReadReceiptCancelsRetriedMessage(t *testing.T) {
	queue := openTestQueue(t)
	mustAdd(t, queue, testMessage("message-1", "chat-1", baseTime), baseTime)
	now := baseTime.Add(forwardDelay)
	mustClaim(t, queue, now)
	if err := queue.Retry("message-1", now.Add(time.Minute), "twilio 503", now); err != nil {
		t.Fatal(err)
	}

	cancelled, err := queue.MarkRead([]string{"message-1"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled != 1 {
		t.Errorf("cancelled = %d, want 1", cancelled)
	}
	expectStatus(t, queue, "message-1", StatusRead)
	if _, wasClaimed := claim(t, queue, now.Add(time.Hour)); wasClaimed {
		t.Error("claimed a message that was read after its retry")
	}
}

func TestReadDuringSendCancelsItsRetry(t *testing.T) {
	queue := openTestQueue(t)
	mustAdd(t, queue, testMessage("message-1", "chat-1", baseTime), baseTime)
	now := baseTime.Add(forwardDelay)
	mustClaim(t, queue, now)

	if _, err := queue.MarkRead([]string{"message-1"}, now); err != nil {
		t.Fatal(err)
	}
	expectStatus(t, queue, "message-1", StatusSending)
	if err := queue.Retry("message-1", now.Add(time.Minute), "twilio 503", now); err != nil {
		t.Fatal(err)
	}
	expectStatus(t, queue, "message-1", StatusRead)
}

func TestReplyDuringSendCancelsItsRetry(t *testing.T) {
	queue := openTestQueue(t)
	mustAdd(t, queue, testMessage("message-1", "chat-1", baseTime), baseTime)
	now := baseTime.Add(forwardDelay)
	mustClaim(t, queue, now)

	if _, err := queue.MarkReplied("chat-1", now, now); err != nil {
		t.Fatal(err)
	}
	if err := queue.Retry("message-1", now.Add(time.Minute), "twilio 503", now); err != nil {
		t.Fatal(err)
	}
	expectStatus(t, queue, "message-1", StatusReplied)
}

func TestReadDuringSendDoesNotChangeSuccessfulSend(t *testing.T) {
	queue := openTestQueue(t)
	mustAdd(t, queue, testMessage("message-1", "chat-1", baseTime), baseTime)
	now := baseTime.Add(forwardDelay)
	mustClaim(t, queue, now)

	if _, err := queue.MarkRead([]string{"message-1"}, now); err != nil {
		t.Fatal(err)
	}
	if err := queue.Finish("message-1", StatusSent, "", "SM123", now); err != nil {
		t.Fatal(err)
	}
	expectStatus(t, queue, "message-1", StatusSent)
}

// Startup and housekeeping
// ---------------------------------------------------------------------------

func TestFailInterruptedSendsSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "forwarder.db")
	queue, err := OpenQueue(path)
	if err != nil {
		t.Fatal(err)
	}
	mustAdd(t, queue, testMessage("interrupted", "chat-1", baseTime), baseTime)
	mustAdd(t, queue, testMessage("waiting", "chat-1", baseTime.Add(time.Hour)), baseTime)
	mustClaim(t, queue, baseTime.Add(forwardDelay))
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenQueue(path)
	if err != nil {
		t.Fatal(err)
	}
	expectStatus(t, reopened, "interrupted", StatusSending)
	failedCount, err := reopened.FailInterruptedSends(baseTime.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if failedCount != 1 {
		t.Errorf("failed count = %d, want 1", failedCount)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	final, err := OpenQueue(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { final.Close() })
	interrupted := mustGet(t, final, "interrupted")
	if interrupted.Status != StatusFailed || interrupted.Reason != "unknown (crashed mid-send)" || interrupted.Attempts != 1 {
		t.Errorf("interrupted after reopen: %+v", interrupted)
	}
	expectStatus(t, final, "waiting", StatusPending)
	if _, wasClaimed := claim(t, final, baseTime.Add(24*time.Hour)); !wasClaimed {
		t.Error("pending message was not claimable after reopen")
	}
	if claimedAgain, _ := claim(t, final, baseTime.Add(24*time.Hour)); claimedAgain.MessageID == "interrupted" {
		t.Error("interrupted message became claimable again")
	}
}

func TestPruneDeletesOnlyOldFinishedMessages(t *testing.T) {
	queue := openTestQueue(t)
	old := baseTime.Add(-48 * time.Hour)
	cutoff := baseTime.Add(-24 * time.Hour)

	mustAdd(t, queue, testMessage("old-pending", "chat-1", old), old)
	mustAdd(t, queue, testMessage("old-sending", "chat-1", old.Add(-time.Minute)), old)
	mustClaim(t, queue, old.Add(forwardDelay))
	mustAdd(t, queue, testMessage("old-sent", "chat-1", old.Add(-2*time.Minute)), old)
	mustClaim(t, queue, old.Add(forwardDelay))
	mustFinish(t, queue, "old-sent", StatusSent, "", "SM1", old)
	mustAdd(t, queue, testMessage("old-replied", "chat-2", old), old)
	if _, err := queue.MarkReplied("chat-2", old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.MarkRead([]string{"old-placeholder"}, old); err != nil {
		t.Fatal(err)
	}
	mustAdd(t, queue, testMessage("recent-read", "chat-1", baseTime), baseTime)
	if _, err := queue.MarkRead([]string{"recent-read", "recent-placeholder"}, baseTime); err != nil {
		t.Fatal(err)
	}
	expectStatus(t, queue, "old-sending", StatusSending)

	pruned, err := queue.Prune(cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 3 {
		t.Errorf("pruned = %d, want 3", pruned)
	}
	for _, messageID := range []string{"old-sent", "old-replied", "old-placeholder"} {
		if _, wasFound, _ := queue.Get(messageID); wasFound {
			t.Errorf("%s was not pruned", messageID)
		}
	}
	expectStatus(t, queue, "old-pending", StatusPending)
	expectStatus(t, queue, "old-sending", StatusSending)
	expectStatus(t, queue, "recent-read", StatusRead)
	expectStatus(t, queue, "recent-placeholder", StatusRead)
}

// Concurrency
// ---------------------------------------------------------------------------

func TestConcurrentAddMarkReadAndClaimNeverClaimTwice(t *testing.T) {
	queue := openTestQueue(t)
	const writerCount = 4
	const messagesPerWriter = 50
	now := baseTime.Add(time.Hour)

	var waitGroup sync.WaitGroup
	var claimedMutex sync.Mutex
	claimedCounts := map[string]int{}
	errorChannel := make(chan error, 100)
	isAddingDone := make(chan struct{})

	var addersWaitGroup sync.WaitGroup
	for writerIndex := range writerCount {
		addersWaitGroup.Add(1)
		go func() {
			defer addersWaitGroup.Done()
			for messageIndex := range messagesPerWriter {
				messageID := fmt.Sprintf("writer-%d-message-%d", writerIndex, messageIndex)
				if _, err := queue.Add(testMessage(messageID, "chat-1", baseTime), now); err != nil {
					errorChannel <- err
					return
				}
				if messageIndex%3 == 0 {
					if _, err := queue.MarkRead([]string{messageID}, now); err != nil {
						errorChannel <- err
						return
					}
				}
			}
		}()
	}
	go func() {
		addersWaitGroup.Wait()
		close(isAddingDone)
	}()

	for range writerCount {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for {
				wasAddingDoneBeforeClaim := isClosed(isAddingDone)
				message, wasClaimed, err := queue.ClaimNextDue(now)
				if err != nil {
					errorChannel <- err
					return
				}
				if !wasClaimed {
					if wasAddingDoneBeforeClaim {
						return
					}
					continue
				}
				claimedMutex.Lock()
				claimedCounts[message.MessageID]++
				claimedMutex.Unlock()
				if err := queue.Finish(message.MessageID, StatusSent, "", "SM", now); err != nil {
					errorChannel <- err
					return
				}
			}
		}()
	}

	waitGroup.Wait()
	close(errorChannel)
	for err := range errorChannel {
		t.Error(err)
	}

	if len(claimedCounts) == 0 {
		t.Error("no message was claimed")
	}
	for messageID, count := range claimedCounts {
		if count != 1 {
			t.Errorf("%s claimed %d times", messageID, count)
		}
		expectStatus(t, queue, messageID, StatusSent)
	}
	for writerIndex := range writerCount {
		for messageIndex := range messagesPerWriter {
			messageID := fmt.Sprintf("writer-%d-message-%d", writerIndex, messageIndex)
			stored := mustGet(t, queue, messageID)
			if stored.Status != StatusSent && stored.Status != StatusRead {
				t.Errorf("%s ended as %q", messageID, stored.Status)
			}
			if stored.Status == StatusSent && claimedCounts[messageID] != 1 {
				t.Errorf("%s is sent but was claimed %d times", messageID, claimedCounts[messageID])
			}
		}
	}
}

// Helpers
// ---------------------------------------------------------------------------

func openTestQueue(t *testing.T) *Queue {
	t.Helper()
	queue, err := OpenQueue(filepath.Join(t.TempDir(), "forwarder.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { queue.Close() })
	return queue
}

func isClosed(channel chan struct{}) bool {
	select {
	case <-channel:
		return true
	default:
		return false
	}
}

func testMessage(messageID string, chatJID string, receivedAt time.Time) Message {
	return Message{
		MessageID:   messageID,
		ChatJID:     chatJID,
		SenderName:  "Sender",
		SenderPhone: "+421900000000",
		Text:        "text of " + messageID,
		ReceivedAt:  receivedAt,
	}
}

func mustAdd(t *testing.T, queue *Queue, message Message, now time.Time) bool {
	t.Helper()
	wasAdded, err := queue.Add(message, now)
	if err != nil {
		t.Fatal(err)
	}
	return wasAdded
}

func mustGet(t *testing.T, queue *Queue, messageID string) Message {
	t.Helper()
	message, wasFound, err := queue.Get(messageID)
	if err != nil {
		t.Fatal(err)
	}
	if !wasFound {
		t.Fatalf("message %q not found", messageID)
	}
	return message
}

func claim(t *testing.T, queue *Queue, now time.Time) (Message, bool) {
	t.Helper()
	message, wasClaimed, err := queue.ClaimNextDue(now)
	if err != nil {
		t.Fatal(err)
	}
	return message, wasClaimed
}

func mustClaim(t *testing.T, queue *Queue, now time.Time) Message {
	t.Helper()
	message, wasClaimed := claim(t, queue, now)
	if !wasClaimed {
		t.Fatal("expected a due message to claim")
	}
	return message
}

func mustFinish(t *testing.T, queue *Queue, messageID string, status MessageStatus, reason string, twilioSID string, now time.Time) {
	t.Helper()
	if err := queue.Finish(messageID, status, reason, twilioSID, now); err != nil {
		t.Fatal(err)
	}
}

func expectStatus(t *testing.T, queue *Queue, messageID string, expected MessageStatus) {
	t.Helper()
	if actual := mustGet(t, queue, messageID).Status; actual != expected {
		t.Errorf("%s status = %q, want %q", messageID, actual, expected)
	}
}
