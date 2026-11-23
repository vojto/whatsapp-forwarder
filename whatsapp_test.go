package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

var (
	ownPhone    = types.NewJID("421900000000", types.DefaultUserServer)
	lenkaLID    = types.NewJID("123456789", types.HiddenUserServer)
	lenkaPhone  = types.NewJID("421911111111", types.DefaultUserServer)
	strangerLID = types.NewJID("999999999", types.HiddenUserServer)
	familyGroup = types.NewJID("120363000000000000", types.GroupServer)
)

// Fakes
// ---------------------------------------------------------------------------

type fakeIdentities struct {
	phonesByLID   map[types.JID]types.JID
	contactsByJID map[types.JID]types.ContactInfo
}

func (identities fakeIdentities) phoneForLID(ctx context.Context, lid types.JID) (types.JID, error) {
	return identities.phonesByLID[lid], nil
}

func (identities fakeIdentities) contact(ctx context.Context, phone types.JID) (types.ContactInfo, error) {
	return identities.contactsByJID[phone], nil
}

type fakeSocket struct {
	isConnected bool
}

func (socket *fakeSocket) IsConnected() bool { return socket.isConnected }

// fakeScheduler records state changes only, like the real one: it starts paused, and pausing a
// paused scheduler records nothing.
type fakeScheduler struct {
	isRunning bool
	calls     []string
}

func (scheduler *fakeScheduler) Pause() {
	if scheduler.isRunning {
		scheduler.isRunning = false
		scheduler.calls = append(scheduler.calls, "pause")
	}
}

func (scheduler *fakeScheduler) Resume() {
	if !scheduler.isRunning {
		scheduler.isRunning = true
		scheduler.calls = append(scheduler.calls, "resume")
	}
}

type fakeSMS struct {
	bodies []string
}

func (sms *fakeSMS) Send(ctx context.Context, senderID string, body string) (string, error) {
	sms.bodies = append(sms.bodies, body)
	return "SM1", nil
}

type testHarness struct {
	whatsApp    *WhatsApp
	socket      *fakeSocket
	queue       *Queue
	scheduler   *fakeScheduler
	sms         *fakeSMS
	clock       *fakeClock
	logoutCalls int
}

func newTestHarness(t *testing.T) *testHarness {
	queue, err := OpenQueue(filepath.Join(t.TempDir(), "forwarder.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { queue.Close() })
	identities := fakeIdentities{
		phonesByLID:   map[types.JID]types.JID{lenkaLID: lenkaPhone},
		contactsByJID: map[types.JID]types.ContactInfo{lenkaPhone: {Found: true, FullName: "Lenka Horváthová"}},
	}
	harness := &testHarness{socket: &fakeSocket{}, queue: queue, scheduler: &fakeScheduler{}, sms: &fakeSMS{}, clock: &fakeClock{now: startTime}}
	harness.whatsApp = newWhatsApp(identities, harness.socket, queue, harness.scheduler, harness.sms, harness.clock.Now,
		func() { harness.logoutCalls++ })
	return harness
}

func (harness *testHarness) handle(t *testing.T, rawEvent any) {
	t.Helper()
	if !harness.whatsApp.HandleEvent(rawEvent) {
		t.Fatalf("HandleEvent(%T) returned false", rawEvent)
	}
}

func (harness *testHarness) status(t *testing.T, messageID string) MessageStatus {
	t.Helper()
	message, isFound, err := harness.queue.Get(messageID)
	if err != nil {
		t.Fatal(err)
	}
	if !isFound {
		return ""
	}
	return message.Status
}

// tickConnection moves the clock and runs one WatchConnection check.
func (harness *testHarness) tickConnection(duration time.Duration) {
	harness.clock.advance(duration)
	harness.whatsApp.checkConnection(context.Background())
}

func textMessage(messageID string, chat types.JID, sender types.JID, text string) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: sender, IsGroup: chat.Server == types.GroupServer},
			ID:            messageID,
			PushName:      "Klau",
			Timestamp:     startTime,
		},
		Message: &waE2E.Message{Conversation: proto.String(text)},
	}
}

func messageFromMe(messageID string, chat types.JID, timestamp time.Time) *events.Message {
	message := textMessage(messageID, chat, ownPhone, "ok")
	message.Info.IsFromMe = true
	message.Info.Timestamp = timestamp
	return message
}

// Incoming messages
// ---------------------------------------------------------------------------

func TestIncomingPersonMessageIsQueuedWithNormalizedChatAndIdentity(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, textMessage("M1", lenkaLID, lenkaLID, "ahoj"))

	message, isFound, err := harness.queue.Get("M1")
	if err != nil || !isFound {
		t.Fatalf("message not queued: %v", err)
	}
	expected := Message{
		MessageID:   "M1",
		ChatJID:     lenkaPhone.String(),
		SenderName:  "Lenka Horváthová",
		SenderPhone: "+421911111111",
		Text:        "ahoj",
		Status:      StatusPending,
	}
	actual := Message{
		MessageID:   message.MessageID,
		ChatJID:     message.ChatJID,
		SenderName:  message.SenderName,
		SenderPhone: message.SenderPhone,
		Text:        message.Text,
		Status:      message.Status,
	}
	if actual != expected {
		t.Fatalf("queued %+v, want %+v", actual, expected)
	}
	if !message.ReceivedAt.Equal(startTime) {
		t.Fatalf("ReceivedAt = %v, want %v", message.ReceivedAt, startTime)
	}
}

func TestUnknownLIDSenderKeepsLIDChatAndPushName(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, textMessage("M1", strangerLID, strangerLID, "hi"))

	message, _, _ := harness.queue.Get("M1")
	if message.ChatJID != strangerLID.String() || message.SenderName != "Klau" || message.SenderPhone != "" {
		t.Fatalf("queued %+v", message)
	}
}

func TestSenderAltProvidesPhone(t *testing.T) {
	harness := newTestHarness(t)
	event := textMessage("M1", strangerLID, strangerLID, "hi")
	event.Info.SenderAlt = types.NewJID("421922222222", types.DefaultUserServer)
	harness.handle(t, event)

	message, _, _ := harness.queue.Get("M1")
	if message.SenderPhone != "+421922222222" {
		t.Fatalf("SenderPhone = %q", message.SenderPhone)
	}
}

func TestRedeliveredMessageIsIgnored(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, textMessage("M1", lenkaLID, lenkaLID, "first"))
	harness.handle(t, textMessage("M1", lenkaLID, lenkaLID, "second"))

	message, _, _ := harness.queue.Get("M1")
	if message.Text != "first" {
		t.Fatalf("Text = %q, want first copy", message.Text)
	}
}

func TestGroupMessageIsQueuedUnderGroupJID(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, textMessage("M1", familyGroup, lenkaLID, "dinner?"))

	message, _, _ := harness.queue.Get("M1")
	if message.ChatJID != familyGroup.String() || message.SenderName != "Lenka Horváthová" {
		t.Fatalf("queued %+v", message)
	}
}

func TestMediaMessageUsesLabelAndCaption(t *testing.T) {
	harness := newTestHarness(t)
	event := textMessage("M1", lenkaLID, lenkaLID, "")
	event.Message = &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Caption: proto.String("look")}}
	harness.handle(t, event)

	message, _, _ := harness.queue.Get("M1")
	if message.Text != "[photo] look" {
		t.Fatalf("Text = %q", message.Text)
	}
}

func TestMessagesWithoutContentAreIgnored(t *testing.T) {
	harness := newTestHarness(t)
	reaction := textMessage("M1", lenkaLID, lenkaLID, "")
	reaction.Message = &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{Text: proto.String("👍")}}
	protocol := textMessage("M2", lenkaLID, lenkaLID, "")
	protocol.Message = &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{}}
	harness.handle(t, reaction)
	harness.handle(t, protocol)

	for _, messageID := range []string{"M1", "M2"} {
		if status := harness.status(t, messageID); status != "" {
			t.Fatalf("%s queued with status %s", messageID, status)
		}
	}
}

func TestNewsletterAndBroadcastChatsAreIgnored(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, textMessage("M1", types.NewJID("1203630000", types.NewsletterServer), lenkaLID, "news"))
	harness.handle(t, textMessage("M2", types.StatusBroadcastJID, lenkaLID, "story"))

	for _, messageID := range []string{"M1", "M2"} {
		if status := harness.status(t, messageID); status != "" {
			t.Fatalf("%s queued with status %s", messageID, status)
		}
	}
}

func TestQueueFailureLeavesEventUnacked(t *testing.T) {
	harness := newTestHarness(t)
	harness.queue.Close()

	if harness.whatsApp.HandleEvent(textMessage("M1", lenkaLID, lenkaLID, "ahoj")) {
		t.Fatal("HandleEvent returned true for a failed queue write")
	}
	if harness.whatsApp.HandleEvent(&events.Receipt{Type: types.ReceiptTypeReadSelf, MessageIDs: []string{"M1"}}) {
		t.Fatal("HandleEvent returned true for a failed receipt write")
	}
}

// Replies and receipts
// ---------------------------------------------------------------------------

func TestMessageFromMeInLIDChatMarksPhoneChatReplied(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, textMessage("M1", lenkaPhone, lenkaLID, "ahoj"))
	harness.handle(t, messageFromMe("R1", lenkaLID, startTime.Add(time.Minute)))

	if status := harness.status(t, "M1"); status != StatusReplied {
		t.Fatalf("status = %s, want replied", status)
	}
	if status := harness.status(t, "R1"); status != StatusMine {
		t.Fatalf("own message status = %q, want mine", status)
	}
	if mine, _, _ := harness.queue.Get("R1"); mine.ChatJID != lenkaPhone.String() || mine.Text != "ok" {
		t.Fatalf("own message stored as %+v", mine)
	}
}

func TestReadSelfReceiptMarksRead(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, textMessage("M1", lenkaLID, lenkaLID, "ahoj"))
	harness.handle(t, &events.Receipt{Type: types.ReceiptTypeRead, MessageIDs: []string{"M1"}})
	if status := harness.status(t, "M1"); status != StatusPending {
		t.Fatalf("status after other receipt = %s, want pending", status)
	}

	harness.handle(t, &events.Receipt{Type: types.ReceiptTypeReadSelf, MessageIDs: []string{"M1"}})
	if status := harness.status(t, "M1"); status != StatusRead {
		t.Fatalf("status = %s, want read", status)
	}
}

func TestReceiptBeforeMessageLeavesItRead(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, &events.Receipt{Type: types.ReceiptTypeReadSelf, MessageIDs: []string{"M1"}})
	harness.handle(t, textMessage("M1", lenkaLID, lenkaLID, "ahoj"))

	if status := harness.status(t, "M1"); status != StatusRead {
		t.Fatalf("status = %s, want read", status)
	}
}

// Connection
// ---------------------------------------------------------------------------

func TestOfflineSyncCompletedResumesAndDisconnectedPauses(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, &events.Connected{})
	harness.handle(t, &events.OfflineSyncCompleted{Count: 3})
	harness.handle(t, &events.Disconnected{})
	harness.handle(t, &events.Connected{})
	harness.handle(t, &events.OfflineSyncCompleted{Count: 0})
	harness.handle(t, &events.StreamReplaced{})

	expected := fmt.Sprint([]string{"resume", "pause", "resume", "pause"})
	if actual := fmt.Sprint(harness.scheduler.calls); actual != expected {
		t.Fatalf("scheduler calls = %s, want %s", actual, expected)
	}
}

func TestConnectedPausesUntilItsOwnOfflineSync(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, &events.Connected{})
	harness.handle(t, &events.OfflineSyncCompleted{})
	harness.handle(t, &events.Connected{})
	if harness.scheduler.isRunning {
		t.Fatal("scheduler running before the new connection's offline sync")
	}
}

func TestStaleDisconnectAfterReconnectIsIgnored(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, &events.Connected{})
	harness.handle(t, &events.OfflineSyncCompleted{})
	harness.socket.isConnected = true
	harness.handle(t, &events.Disconnected{})
	harness.tickConnection(20 * time.Minute)

	if !harness.scheduler.isRunning {
		t.Fatal("stale disconnect paused the scheduler")
	}
	if len(harness.sms.bodies) != 0 {
		t.Fatalf("SMS = %v, want none", harness.sms.bodies)
	}
}

func TestSchedulerResumesWithoutOfflineSyncAfterTimeout(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, &events.Connected{})
	harness.tickConnection(time.Minute)
	if len(harness.scheduler.calls) != 0 {
		t.Fatalf("resumed too early: %v", harness.scheduler.calls)
	}
	harness.tickConnection(time.Minute)
	harness.tickConnection(time.Minute)
	if fmt.Sprint(harness.scheduler.calls) != "[resume]" {
		t.Fatalf("scheduler calls = %v, want one resume", harness.scheduler.calls)
	}
}

func TestOfflineSyncFallbackIsCancelledByRealEventOrDisconnect(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, &events.Connected{})
	harness.handle(t, &events.OfflineSyncCompleted{})
	harness.tickConnection(3 * time.Minute)
	harness.handle(t, &events.Connected{})
	harness.handle(t, &events.Disconnected{})
	harness.tickConnection(3 * time.Minute)

	if fmt.Sprint(harness.scheduler.calls) != "[resume pause]" {
		t.Fatalf("scheduler calls = %v", harness.scheduler.calls)
	}
}

func TestDisconnectAlerts(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, &events.Connected{})
	harness.handle(t, &events.Disconnected{})

	harness.tickConnection(14 * time.Minute)
	if len(harness.sms.bodies) != 0 {
		t.Fatalf("alert before 15 min: %v", harness.sms.bodies)
	}
	harness.tickConnection(time.Minute)
	harness.tickConnection(10 * time.Minute)
	harness.handle(t, &events.Disconnected{})
	harness.tickConnection(10 * time.Minute)
	if fmt.Sprint(harness.sms.bodies) != "["+disconnectedAlertText+"]" {
		t.Fatalf("SMS = %v, want one disconnect alert", harness.sms.bodies)
	}

	harness.handle(t, &events.Connected{})
	harness.tickConnection(30 * time.Second)
	harness.tickConnection(30 * time.Second)
	expected := []string{disconnectedAlertText, "WA forwarder reconnected after 35 min"}
	if fmt.Sprint(harness.sms.bodies) != fmt.Sprint(expected) {
		t.Fatalf("SMS = %v, want %v", harness.sms.bodies, expected)
	}
}

func TestShortDisconnectSendsNothing(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, &events.Connected{})
	harness.handle(t, &events.Disconnected{})
	harness.tickConnection(10 * time.Minute)
	harness.handle(t, &events.Connected{})
	harness.tickConnection(30 * time.Minute)

	if len(harness.sms.bodies) != 0 {
		t.Fatalf("SMS = %v, want none", harness.sms.bodies)
	}
}

func TestNeverConnectingSendsDisconnectAlert(t *testing.T) {
	harness := newTestHarness(t)
	harness.tickConnection(15 * time.Minute)

	if fmt.Sprint(harness.sms.bodies) != "["+disconnectedAlertText+"]" {
		t.Fatalf("SMS = %v", harness.sms.bodies)
	}
}

func TestLoggedOutSendsOneAlertAndCallsOnLoggedOut(t *testing.T) {
	harness := newTestHarness(t)
	harness.handle(t, &events.Connected{})
	harness.handle(t, &events.LoggedOut{OnConnect: true, Reason: events.ConnectFailureLoggedOut})

	if fmt.Sprint(harness.sms.bodies) != "["+loggedOutAlertText+"]" {
		t.Fatalf("SMS = %v", harness.sms.bodies)
	}
	if harness.logoutCalls != 1 {
		t.Fatalf("onLoggedOut called %d times", harness.logoutCalls)
	}
	if harness.scheduler.isRunning {
		t.Fatal("scheduler still running after logout")
	}
}

// whatsmeow log
// ---------------------------------------------------------------------------

func TestWhatsmeowLoggerDropsLTHashLinesAndDebugIncludingSubLoggers(t *testing.T) {
	var output bytes.Buffer
	logger := whatsmeowLogger{module: "Client", output: log.New(&output, "", 0)}
	appStateLogger := logger.Sub("AppState")

	appStateLogger.Errorf("Failed to sync app state: %v", fmt.Errorf("failed to decode regular_low: %w", errors.New("mismatching LTHash")))
	logger.Warnf("Ignoring mismatching LTHash in %s v%d", "regular_high", 3)
	appStateLogger.Infof("Synced %s", "critical_block")
	appStateLogger.Warnf("Something else broke: %v", "boom")

	if actual := output.String(); actual != "[Client/AppState WARN] Something else broke: boom\n" {
		t.Fatalf("logged %q", actual)
	}
}
