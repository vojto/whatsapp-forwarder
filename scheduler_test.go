package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const directChat = "421900111222@s.whatsapp.net"
const otherDirectChat = "421900333444@s.whatsapp.net"
const smallGroupChat = "120363000000000001@g.us"
const largeGroupChat = "120363000000000002@g.us"
const archivedChat = "421900555666@s.whatsapp.net"
const brokenChat = "421900777888@s.whatsapp.net"

var startTime = time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)

// Scenarios
// ---------------------------------------------------------------------------

func TestForwardsUnreadMessageOnceAfterDelay(t *testing.T) {
	harness := newResumedHarness(t)
	harness.add("m1", directChat, startTime)

	harness.advance(forwardDelay - time.Second)
	harness.expectSendCount(0)

	harness.advance(time.Second)
	harness.advance(time.Hour)
	harness.expectSendCount(1)
	harness.expectSent(0, "Klaudia", "+421900111222: Ahoj, kedy prides?")
	message := harness.expectStatus("m1", StatusSent)
	if message.TwilioSID != "SM1" {
		t.Fatalf("twilio SID = %q, want SM1", message.TwilioSID)
	}
}

func TestReadReceiptBeforeDelayCancelsForward(t *testing.T) {
	harness := newResumedHarness(t)
	harness.add("m1", directChat, startTime)

	harness.advance(2 * time.Minute)
	harness.markRead("m1")
	harness.advance(time.Hour)

	harness.expectSendCount(0)
	harness.expectStatus("m1", StatusRead)
}

func TestReadReceiptAfterRestartCancelsForward(t *testing.T) {
	harness := newResumedHarness(t)
	harness.add("m1", directChat, startTime)

	harness.advance(time.Minute)
	harness.restart()
	harness.advance(time.Minute)
	harness.markRead("m1")
	harness.scheduler.Resume()
	harness.advance(time.Hour)

	harness.expectSendCount(0)
	harness.expectStatus("m1", StatusRead)
}

func TestMessageDueWhileProcessWasDown(t *testing.T) {
	tests := []struct {
		name           string
		hasBacklogRead bool
		wantStatus     MessageStatus
	}{
		{name: "backlog read receipt cancels", hasBacklogRead: true, wantStatus: StatusRead},
		{name: "no receipt forwards late", hasBacklogRead: false, wantStatus: StatusSent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newResumedHarness(t)
			harness.add("m1", directChat, startTime)
			harness.advance(time.Minute)
			harness.restart()

			harness.advance(time.Hour)
			harness.expectSendCount(0)
			if test.hasBacklogRead {
				harness.markRead("m1")
			}
			harness.scheduler.Resume()
			harness.advance(0)

			harness.expectStatus("m1", test.wantStatus)
		})
	}
}

func TestOfflineBacklogMessage(t *testing.T) {
	tests := []struct {
		name           string
		hasBacklogRead bool
		wantStatus     MessageStatus
	}{
		{name: "backlog read receipt cancels", hasBacklogRead: true, wantStatus: StatusRead},
		{name: "no receipt forwards late", hasBacklogRead: false, wantStatus: StatusSent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newHarness(t)
			harness.add("m1", directChat, startTime.Add(-time.Hour))
			harness.advance(0)
			if test.hasBacklogRead {
				harness.markRead("m1")
			}
			harness.scheduler.Resume()
			harness.advance(0)

			harness.expectStatus("m1", test.wantStatus)
		})
	}
}

func TestReadReceiptBeforeMessageCancelsForward(t *testing.T) {
	harness := newResumedHarness(t)
	harness.markRead("m1")
	harness.add("m1", directChat, startTime)
	harness.advance(time.Hour)

	harness.expectSendCount(0)
}

func TestStaleMessageIsNotForwarded(t *testing.T) {
	harness := newResumedHarness(t)
	harness.add("m1", directChat, startTime.Add(-13*time.Hour))
	harness.advance(0)

	harness.expectSendCount(0)
	message := harness.expectStatus("m1", StatusStale)
	if message.Reason != "older than 12h" {
		t.Fatalf("reason = %q", message.Reason)
	}
}

func TestReplyCancelsEarlierMessagesInThatChatOnly(t *testing.T) {
	harness := newResumedHarness(t)
	harness.add("before", directChat, startTime)
	harness.add("other chat", otherDirectChat, startTime)
	harness.advance(time.Minute)
	harness.markReplied(directChat, harness.clock.now)
	harness.add("after", directChat, startTime.Add(time.Minute+time.Second))
	harness.advance(time.Hour)

	harness.expectStatus("before", StatusReplied)
	harness.expectStatus("other chat", StatusSent)
	harness.expectStatus("after", StatusSent)
	harness.expectSendCount(2)
}

func TestCrashMidSendFailsWithoutResending(t *testing.T) {
	harness := newResumedHarness(t)
	harness.add("m1", directChat, startTime)
	harness.clock.advance(forwardDelay)
	if _, isClaimed, err := harness.queue.ClaimNextDue(harness.clock.now); err != nil || !isClaimed {
		t.Fatalf("claim: claimed=%v err=%v", isClaimed, err)
	}

	harness.restart()
	harness.scheduler.Resume()
	harness.advance(time.Hour)

	harness.expectSendCount(0)
	message := harness.expectStatus("m1", StatusFailed)
	if message.Reason != crashedMidSendReason {
		t.Fatalf("reason = %q", message.Reason)
	}
}

func TestTemporaryFailuresBackOffThenFail(t *testing.T) {
	harness := newResumedHarness(t)
	harness.sender.scriptErrors(temporaryError(), temporaryError(), temporaryError(), temporaryError())
	harness.add("m1", directChat, startTime)

	harness.advance(forwardDelay)
	harness.expectSendCount(1)
	for index, backoff := range retryBackoffs {
		harness.advance(backoff - time.Second)
		harness.expectSendCount(index + 1)
		harness.expectStatus("m1", StatusPending)
		harness.advance(time.Second)
		harness.expectSendCount(index + 2)
	}

	message := harness.expectStatus("m1", StatusFailed)
	if message.Attempts != 4 || !strings.Contains(message.Reason, "503") {
		t.Fatalf("attempts = %d, reason = %q", message.Attempts, message.Reason)
	}
	harness.advance(time.Hour)
	harness.expectSendCount(4)
}

func TestTemporaryFailureThenSuccess(t *testing.T) {
	harness := newResumedHarness(t)
	harness.sender.scriptErrors(temporaryError())
	harness.add("m1", directChat, startTime)

	harness.advance(forwardDelay)
	harness.advance(retryBackoffs[0])

	harness.expectSendCount(2)
	harness.expectStatus("m1", StatusSent)
}

func TestReadReceiptDuringBackoffCancelsRetry(t *testing.T) {
	harness := newResumedHarness(t)
	harness.sender.scriptErrors(temporaryError())
	harness.add("m1", directChat, startTime)

	harness.advance(forwardDelay)
	harness.markRead("m1")
	harness.advance(time.Hour)

	harness.expectSendCount(1)
	harness.expectStatus("m1", StatusRead)
}

func TestPermanentFailureFailsImmediately(t *testing.T) {
	harness := newResumedHarness(t)
	harness.sender.scriptErrors(errors.New("twilio 400: invalid sender ID"))
	harness.add("m1", directChat, startTime)

	harness.advance(forwardDelay)
	harness.advance(time.Hour)

	harness.expectSendCount(1)
	message := harness.expectStatus("m1", StatusFailed)
	if message.Attempts != 1 || message.Reason != "twilio 400: invalid sender ID" {
		t.Fatalf("attempts = %d, reason = %q", message.Attempts, message.Reason)
	}
}

func TestChatRules(t *testing.T) {
	harness := newResumedHarness(t)
	harness.add("large group", largeGroupChat, startTime)
	harness.add("small group", smallGroupChat, startTime)
	harness.add("archived", archivedChat, startTime)
	harness.advance(forwardDelay)

	if message := harness.expectStatus("large group", StatusSkipped); message.Reason != "group of 132" {
		t.Fatalf("reason = %q", message.Reason)
	}
	if message := harness.expectStatus("archived", StatusSkipped); message.Reason != "archived chat" {
		t.Fatalf("reason = %q", message.Reason)
	}
	harness.expectStatus("small group", StatusSent)
	harness.expectSendCount(1)
	harness.expectSent(0, "Klaudia", "+421900111222 @ Rodina: Ahoj, kedy prides?")
}

func TestChatLookupErrorIsRetried(t *testing.T) {
	harness := newResumedHarness(t)
	harness.add("m1", brokenChat, startTime)

	harness.advance(forwardDelay)
	message := harness.expectStatus("m1", StatusPending)
	if message.Attempts != 1 || !strings.Contains(message.Reason, "chat info") {
		t.Fatalf("attempts = %d, reason = %q", message.Attempts, message.Reason)
	}

	delete(harness.chats.errors, brokenChat)
	harness.advance(retryBackoffs[0])
	harness.expectStatus("m1", StatusSent)
}

func TestPausedSchedulerNeverSends(t *testing.T) {
	harness := newResumedHarness(t)
	harness.scheduler.Pause()
	harness.add("m1", directChat, startTime)
	harness.advance(time.Hour)

	harness.expectSendCount(0)
	harness.expectStatus("m1", StatusPending)
}

func TestPruneDeletesOnlyOldFinishedRows(t *testing.T) {
	harness := newResumedHarness(t)
	harness.add("old sent", directChat, startTime.Add(-31*24*time.Hour))
	harness.add("old pending", otherDirectChat, startTime.Add(-31*24*time.Hour))
	harness.add("recent", directChat, startTime.Add(-29*24*time.Hour))
	harness.markRead("recent")
	harness.forceStatus("old sent", StatusSent)
	harness.advance(0)

	harness.expectMissing("old sent")
	harness.expectStatus("recent", StatusRead)
	// Pruning runs before claiming, so this row was still pending then; the tick then marked it
	// stale, and it goes at the next hourly prune.
	harness.expectStatus("old pending", StatusStale)
	harness.advance(pruneInterval - time.Second)
	harness.expectStatus("old pending", StatusStale)
	harness.advance(time.Second)
	harness.expectMissing("old pending")
}

// Bursts and the forward judge
// ---------------------------------------------------------------------------

func TestBurstIsForwardedAsOneSMS(t *testing.T) {
	harness := newResumedHarness(t)
	harness.addText("m1", directChat, startTime, "Ahoj")
	harness.addText("m2", directChat, startTime.Add(time.Minute), "Kedy prídeš?")
	harness.advance(forwardDelay)

	harness.expectSendCount(1)
	harness.expectSent(0, "Klaudia", "+421900111222: Ahoj / Kedy prides?")
	harness.expectStatus("m1", StatusSent)
	harness.expectStatus("m2", StatusSent)
	if len(harness.judge.states) != 1 {
		t.Fatalf("judge asked %d times, want once per burst", len(harness.judge.states))
	}
}

func TestMessageAfterBurstStartsNewBurst(t *testing.T) {
	harness := newResumedHarness(t)
	harness.add("m1", directChat, startTime)
	harness.advance(forwardDelay)
	harness.add("m2", directChat, harness.clock.now.Add(time.Second))
	harness.advance(time.Second)
	harness.expectSendCount(1)
	harness.expectStatus("m2", StatusPending)

	harness.advance(forwardDelay)
	harness.expectSendCount(2)
	harness.expectStatus("m2", StatusSent)
}

func TestJudgeSaysNoSkipsBurst(t *testing.T) {
	harness := newResumedHarness(t)
	harness.judge.probability = 0.2
	harness.add("m1", directChat, startTime)
	harness.add("m2", directChat, startTime.Add(time.Second))
	harness.advance(forwardDelay)

	harness.expectSendCount(0)
	for _, messageID := range []string{"m1", "m2"} {
		if message := harness.expectStatus(messageID, StatusSkipped); message.Reason != "not important (0.20)" {
			t.Fatalf("reason = %q", message.Reason)
		}
	}
}

func TestJudgeErrorForwardsAnyway(t *testing.T) {
	harness := newResumedHarness(t)
	harness.judge.err = errors.New("jev 500")
	harness.add("m1", directChat, startTime)
	harness.advance(forwardDelay)

	harness.expectSendCount(1)
	harness.expectStatus("m1", StatusSent)
}

func TestNoJudgeForwardsEverything(t *testing.T) {
	harness := newResumedHarness(t)
	harness.scheduler.judge = nil
	harness.add("m1", directChat, startTime)
	harness.advance(forwardDelay)

	harness.expectSendCount(1)
}

func TestChatRulesSkipBeforeAskingJudge(t *testing.T) {
	harness := newResumedHarness(t)
	harness.add("m1", largeGroupChat, startTime)
	harness.advance(forwardDelay)

	harness.expectStatus("m1", StatusSkipped)
	if len(harness.judge.states) != 0 {
		t.Fatalf("judge was asked about a skipped chat")
	}
}

func TestJudgeSeesChatHistory(t *testing.T) {
	harness := newResumedHarness(t)
	harness.addText("earlier", directChat, startTime, "Ideme na obed?")
	harness.advance(forwardDelay)
	mine := Message{MessageID: "mine", ChatJID: directChat, SenderName: "me", Text: "Neviem", ReceivedAt: startTime.Add(10 * time.Minute)}
	if _, err := harness.queue.AddMine(mine, harness.clock.now); err != nil {
		t.Fatal(err)
	}
	harness.addText("other chat", otherDirectChat, startTime.Add(11*time.Minute), "Nesuvisi")
	harness.addText("new", directChat, startTime.Add(12*time.Minute), "O 13:00 pri fontane")
	harness.clock.now = startTime.Add(12*time.Minute + forwardDelay)
	harness.advance(0)

	state := harness.judge.states[len(harness.judge.states)-1]
	if len(state.Messages) != 3 {
		t.Fatalf("state messages = %+v, want the 3 from this chat", state.Messages)
	}
	earlier, own, latest := state.Messages[0], state.Messages[1], state.Messages[2]
	if !earlier.WasSentToDumbPhone || earlier.IsNew || own.From != "me" || !latest.IsNew || latest.Text != "O 13:00 pri fontane" {
		t.Fatalf("state messages = %+v", state.Messages)
	}
	if state.MinutesSinceUserWroteHere == nil || *state.MinutesSinceUserWroteHere != 5 || state.Chat != "direct chat" {
		t.Fatalf("state = %+v", state)
	}
}

// Delivery
// ---------------------------------------------------------------------------

func TestDeliveryIsPolledUntilDelivered(t *testing.T) {
	harness := newResumedHarness(t)
	harness.sender.deliveriesBySID = map[string][]Delivery{"SM1": {{Status: "queued"}, {Status: "sent"}, {Status: "delivered"}}}
	harness.add("M1", directChat, startTime)
	harness.advance(forwardDelay)
	sent := harness.expectStatus("M1", StatusSent)
	if !sent.SentAt.Equal(startTime.Add(forwardDelay)) || sent.DeliveryStatus != "" {
		t.Fatalf("after send: sent_at %v, delivery %q", sent.SentAt, sent.DeliveryStatus)
	}

	expectedStatuses := []string{"queued", "sent", "delivered"}
	for _, expectedStatus := range expectedStatuses {
		harness.advance(deliveryCheckInterval)
		if message := harness.expectStatus("M1", StatusSent); message.DeliveryStatus != expectedStatus {
			t.Fatalf("delivery status = %q, want %q", message.DeliveryStatus, expectedStatus)
		}
	}
	harness.advance(deliveryCheckInterval)
	harness.advance(deliveryCheckInterval)
	if harness.sender.deliveryChecks != len(expectedStatuses) {
		t.Errorf("checked %d times, want %d (no checks after delivered)", harness.sender.deliveryChecks, len(expectedStatuses))
	}
}

func TestUndeliveredIsRecordedWithError(t *testing.T) {
	harness := newResumedHarness(t)
	harness.sender.deliveriesBySID = map[string][]Delivery{
		"SM1": {{Status: "undelivered", ErrorCode: 30003, ErrorMessage: "Unreachable destination handset"}},
	}
	harness.add("M1", directChat, startTime)
	harness.advance(forwardDelay)
	harness.advance(deliveryCheckInterval)
	harness.advance(deliveryCheckInterval)

	message := harness.expectStatus("M1", StatusSent)
	if message.DeliveryStatus != "undelivered" || message.DeliveryError != "30003 Unreachable destination handset" {
		t.Errorf("delivery = %q / %q", message.DeliveryStatus, message.DeliveryError)
	}
	if harness.sender.deliveryChecks != 1 {
		t.Errorf("checked %d times, want 1", harness.sender.deliveryChecks)
	}
}

func TestDeliveryStillSentAfterTimeoutIsUnknown(t *testing.T) {
	harness := newResumedHarness(t)
	harness.sender.deliveriesBySID = map[string][]Delivery{"SM1": {{Status: "sent"}}}
	harness.add("M1", directChat, startTime)
	harness.advance(forwardDelay)

	for elapsed := time.Duration(0); elapsed < deliveryTimeout-deliveryCheckInterval; elapsed += deliveryCheckInterval {
		harness.advance(deliveryCheckInterval)
	}
	if message := harness.expectStatus("M1", StatusSent); message.DeliveryStatus != "sent" {
		t.Fatalf("before timeout: delivery status = %q, want sent", message.DeliveryStatus)
	}
	harness.advance(deliveryCheckInterval)
	message := harness.expectStatus("M1", StatusSent)
	if message.DeliveryStatus != deliveryStatusUnknown || message.DeliveryError != "still sent after 10m0s" {
		t.Errorf("after timeout: delivery = %q / %q", message.DeliveryStatus, message.DeliveryError)
	}
}

func TestDeliveryChecksRunWhilePaused(t *testing.T) {
	harness := newResumedHarness(t)
	harness.add("M1", directChat, startTime)
	harness.advance(forwardDelay)
	harness.scheduler.Pause()
	harness.advance(deliveryCheckInterval)

	if message := harness.expectStatus("M1", StatusSent); message.DeliveryStatus != "delivered" {
		t.Errorf("delivery status = %q, want delivered", message.DeliveryStatus)
	}
}

func TestDeliveryCheckErrorIsRetriedLater(t *testing.T) {
	harness := newResumedHarness(t)
	harness.add("M1", directChat, startTime)
	harness.advance(forwardDelay)
	harness.sender.deliveryCheckErr = errors.New("twilio down")
	harness.advance(deliveryCheckInterval)
	if message := harness.expectStatus("M1", StatusSent); message.DeliveryStatus != "" {
		t.Fatalf("delivery status after error = %q, want empty", message.DeliveryStatus)
	}

	harness.sender.deliveryCheckErr = nil
	harness.advance(deliveryCheckInterval)
	if message := harness.expectStatus("M1", StatusSent); message.DeliveryStatus != "delivered" {
		t.Errorf("delivery status = %q, want delivered", message.DeliveryStatus)
	}
}

// Formatting
// ---------------------------------------------------------------------------

func TestSMSBody(t *testing.T) {
	klaudia := func(text string) Message {
		return Message{SenderName: "Klaudia K", SenderPhone: "+421900111222", Text: text}
	}
	peter := Message{SenderName: "Peter", SenderPhone: "+421900999999", Text: "Ja tiez"}
	family := ChatInfo{IsGroup: true, GroupName: "Rodina"}
	tests := []struct {
		name  string
		burst []Message
		chat  ChatInfo
		want  string
	}{
		{"direct", []Message{klaudia("Ahoj")}, ChatInfo{}, "+421900111222: Ahoj"},
		{"direct without phone", []Message{{Text: "Ahoj"}}, ChatInfo{}, "Ahoj"},
		{"group", []Message{klaudia("Ahoj")}, family, "+421900111222 @ Rodina: Ahoj"},
		{"group without phone", []Message{{Text: "Ahoj"}}, family, "@ Rodina: Ahoj"},
		{"converted to GSM", []Message{{SenderPhone: "+421", Text: "Čau 👋"}}, ChatInfo{}, "+421: Cau"},
		{"burst", []Message{klaudia("Ahoj"), klaudia("👋"), klaudia("Kedy prides?")}, ChatInfo{},
			"+421900111222: Ahoj / Kedy prides?"},
		{"several people in a group", []Message{klaudia("Ideme?"), peter}, family,
			"@ Rodina: Klaudia: Ideme? / Peter: Ja tiez"},
		{"too long drops oldest first", []Message{klaudia(strings.Repeat("a", 100)), klaudia(strings.Repeat("b", 100)), klaudia("Kedy?")},
			ChatInfo{}, "+421900111222: ... " + strings.Repeat("b", 100) + " / Kedy?"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := smsBody(test.burst, test.chat); got != test.want {
				t.Fatalf("smsBody = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSkipReason(t *testing.T) {
	tests := []struct {
		chat ChatInfo
		want string
	}{
		{ChatInfo{}, ""},
		{ChatInfo{IsGroup: true, GroupSize: 10}, ""},
		{ChatInfo{IsGroup: true, GroupSize: 11}, "group of 11"},
		{ChatInfo{IsArchived: true}, "archived chat"},
	}
	for _, test := range tests {
		if got := skipReason(test.chat); got != test.want {
			t.Errorf("skipReason(%+v) = %q, want %q", test.chat, got, test.want)
		}
	}
}

// Harness
// ---------------------------------------------------------------------------

type harness struct {
	t         *testing.T
	queuePath string
	queue     *Queue
	clock     *fakeClock
	sender    *fakeSender
	chats     *fakeChats
	judge     *fakeJudge
	scheduler *Scheduler
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	harness := &harness{
		t:         t,
		queuePath: filepath.Join(t.TempDir(), "forwarder.db"),
		clock:     &fakeClock{now: startTime},
		sender:    &fakeSender{},
		chats:     newFakeChats(),
		judge:     &fakeJudge{probability: 1},
	}
	harness.open()
	t.Cleanup(func() { harness.queue.Close() })
	return harness
}

func newResumedHarness(t *testing.T) *harness {
	harness := newHarness(t)
	harness.scheduler.Resume()
	return harness
}

// open opens the queue file and builds a fresh (paused) scheduler, like a process start.
func (harness *harness) open() {
	harness.t.Helper()
	queue, err := OpenQueue(harness.queuePath)
	if err != nil {
		harness.t.Fatalf("open queue: %v", err)
	}
	if _, err := queue.FailInterruptedSends(harness.clock.now); err != nil {
		harness.t.Fatalf("fail interrupted sends: %v", err)
	}
	harness.queue = queue
	harness.scheduler = NewScheduler(queue, harness.chats, harness.sender, harness.sender, harness.judge, harness.clock.Now)
}

// restart simulates the process dying (without finishing anything) and starting again.
func (harness *harness) restart() {
	harness.t.Helper()
	if err := harness.queue.Close(); err != nil {
		harness.t.Fatalf("close queue: %v", err)
	}
	harness.open()
}

// advance moves the clock and runs one scheduler tick.
func (harness *harness) advance(duration time.Duration) {
	harness.clock.advance(duration)
	harness.scheduler.Tick(context.Background())
}

func (harness *harness) add(messageID string, chatJID string, receivedAt time.Time) {
	harness.t.Helper()
	harness.addText(messageID, chatJID, receivedAt, "Ahoj, kedy prídeš?")
}

func (harness *harness) addText(messageID string, chatJID string, receivedAt time.Time, text string) {
	harness.t.Helper()
	message := Message{
		MessageID:   messageID,
		ChatJID:     chatJID,
		SenderName:  "Klaudia Komárová",
		SenderPhone: "+421900111222",
		Text:        text,
		ReceivedAt:  receivedAt,
	}
	if _, err := harness.queue.Add(message, harness.clock.now); err != nil {
		harness.t.Fatalf("add %s: %v", messageID, err)
	}
}

func (harness *harness) markRead(messageID string) {
	harness.t.Helper()
	if _, err := harness.queue.MarkRead([]string{messageID}, harness.clock.now); err != nil {
		harness.t.Fatalf("mark read %s: %v", messageID, err)
	}
}

func (harness *harness) markReplied(chatJID string, repliedAt time.Time) {
	harness.t.Helper()
	if _, err := harness.queue.MarkReplied(chatJID, repliedAt, harness.clock.now); err != nil {
		harness.t.Fatalf("mark replied %s: %v", chatJID, err)
	}
}

// forceStatus finishes a pending row directly, bypassing the scheduler.
func (harness *harness) forceStatus(messageID string, status MessageStatus) {
	harness.t.Helper()
	_, err := harness.queue.database.Exec(`UPDATE messages SET status = ? WHERE message_id = ?`, status, messageID)
	if err != nil {
		harness.t.Fatalf("force status %s: %v", messageID, err)
	}
}

// Assertions
// ---------------------------------------------------------------------------

func (harness *harness) expectStatus(messageID string, status MessageStatus) Message {
	harness.t.Helper()
	message, isFound, err := harness.queue.Get(messageID)
	if err != nil || !isFound {
		harness.t.Fatalf("get %s: found=%v err=%v", messageID, isFound, err)
	}
	if message.Status != status {
		harness.t.Fatalf("%s status = %s (reason %q), want %s", messageID, message.Status, message.Reason, status)
	}
	return message
}

func (harness *harness) expectMissing(messageID string) {
	harness.t.Helper()
	if _, isFound, err := harness.queue.Get(messageID); err != nil || isFound {
		harness.t.Fatalf("get %s: found=%v err=%v, want missing", messageID, isFound, err)
	}
}

func (harness *harness) expectSendCount(count int) {
	harness.t.Helper()
	if len(harness.sender.calls) != count {
		harness.t.Fatalf("sent %d SMS, want %d: %+v", len(harness.sender.calls), count, harness.sender.calls)
	}
}

func (harness *harness) expectSent(index int, senderID string, body string) {
	harness.t.Helper()
	call := harness.sender.calls[index]
	if call.senderID != senderID || call.body != body {
		harness.t.Fatalf("SMS %d = %+v, want sender %q body %q", index, call, senderID, body)
	}
}

// Fakes
// ---------------------------------------------------------------------------

type fakeClock struct {
	now time.Time
}

func (clock *fakeClock) Now() time.Time {
	return clock.now
}

func (clock *fakeClock) advance(duration time.Duration) {
	clock.now = clock.now.Add(duration)
}

type sentSMS struct {
	senderID string
	body     string
}

// fakeSender records every Send call; scripted errors are returned by the next calls in order.
// fakeSender is both the SMSSender and the DeliveryChecker. Deliveries are scripted per SID as a
// sequence of statuses (the last one repeats); unscripted SIDs report "delivered".
type fakeSender struct {
	calls            []sentSMS
	scriptedErrors   []error
	deliveriesBySID  map[string][]Delivery
	deliveryChecks   int
	deliveryCheckErr error
}

func (sender *fakeSender) DeliveryStatus(ctx context.Context, twilioSID string) (Delivery, error) {
	sender.deliveryChecks++
	if sender.deliveryCheckErr != nil {
		return Delivery{}, sender.deliveryCheckErr
	}
	deliveries := sender.deliveriesBySID[twilioSID]
	if len(deliveries) == 0 {
		return Delivery{Status: "delivered"}, nil
	}
	if len(deliveries) > 1 {
		sender.deliveriesBySID[twilioSID] = deliveries[1:]
	}
	return deliveries[0], nil
}

func (sender *fakeSender) scriptErrors(scriptedErrors ...error) {
	sender.scriptedErrors = append(sender.scriptedErrors, scriptedErrors...)
}

func (sender *fakeSender) Send(ctx context.Context, senderID string, body string) (string, error) {
	sender.calls = append(sender.calls, sentSMS{senderID: senderID, body: body})
	if len(sender.scriptedErrors) > 0 {
		err := sender.scriptedErrors[0]
		sender.scriptedErrors = sender.scriptedErrors[1:]
		return "", err
	}
	return fmt.Sprintf("SM%d", len(sender.calls)), nil
}

func temporaryError() error {
	return fmt.Errorf("twilio 503: %w", ErrTemporarySendFailure)
}

// fakeJudge answers every question with probability (or err) and records the states it saw.
type fakeJudge struct {
	probability float64
	err         error
	states      []JudgeState
}

func (judge *fakeJudge) ForwardProbability(ctx context.Context, state JudgeState) (float64, error) {
	judge.states = append(judge.states, state)
	return judge.probability, judge.err
}

type fakeChats struct {
	infos  map[string]ChatInfo
	errors map[string]error
}

func newFakeChats() *fakeChats {
	return &fakeChats{
		infos: map[string]ChatInfo{
			smallGroupChat: {IsGroup: true, GroupName: "Rodina", GroupSize: 4},
			largeGroupChat: {IsGroup: true, GroupName: "Skolka", GroupSize: 132},
			archivedChat:   {IsArchived: true},
		},
		errors: map[string]error{brokenChat: errors.New("websocket not connected")},
	}
}

func (chats *fakeChats) ChatInfo(ctx context.Context, chatJID string) (ChatInfo, error) {
	if err := chats.errors[chatJID]; err != nil {
		return ChatInfo{}, err
	}
	return chats.infos[chatJID], nil
}
