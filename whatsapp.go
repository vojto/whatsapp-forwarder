package main

// Expected behavior:
//   - Incoming person/group messages with something to show go into the queue; the scheduler
//     forwards them later. Vojto's own messages mark the chat's pending messages replied, and
//     read-self receipts (read on another device) mark messages read.
//   - The scheduler only runs between OfflineSyncCompleted and the next disconnect, so the offline
//     backlog's receipts and replies are applied before anything due gets forwarded. If the server
//     never sends OfflineSyncCompleted, the scheduler resumes offlineSyncTimeout after Connected.
//   - WatchConnection sends one SMS after disconnectAlertDelay without a connection, and one more
//     once connected again. Being logged out sends one SMS and calls onLoggedOut.
//
// Gotchas:
//   - main sets client.SynchronousAck, so HandleEvent returning false leaves the message or receipt
//     unacked and WhatsApp redelivers it. Return false only when the queue write failed.
//   - The handler runs on whatsmeow's event loop: it must stay fast. Only the logout alert talks to
//     the network; the disconnect/reconnect alerts are sent from WatchConnection.
//   - Direct chats arrive under either @lid or phone-number JIDs. Chats are stored under the phone
//     number whenever the LID map knows it, so "replied" matches across both forms.
//   - Senders usually arrive as anonymous @lid JIDs. The phone number comes from SenderAlt or the
//     LID map in store.db, and is left empty when neither knows it.
//   - Names prefer Vojto's address book (synced into store.db) over the sender's own push name.
//   - Disconnected is dispatched from a goroutine, so it isn't ordered with message events or with
//     the next Connected (whatsmeow's first reconnect attempt has no delay). A Disconnected that
//     arrives while the socket is already connected again is stale and ignored. Connected itself
//     pauses the scheduler, so it only ever runs after the OfflineSyncCompleted that follows it.
//   - Disconnected is only emitted for unexpected disconnects (whatsmeow then auto-reconnects);
//     permanent ones (StreamReplaced, TemporaryBan, ...) come as their own events and don't reconnect.
//   - The offline-sync fallback and the alerts are checked on WatchConnection's ticks, so they
//     fire up to connectionCheckInterval late.

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const connectionCheckInterval = 30 * time.Second
const offlineSyncTimeout = 2 * time.Minute
const disconnectAlertDelay = 15 * time.Minute
const logoutAlertTimeout = 30 * time.Second

const loggedOutAlertText = "WA forwarder logged out, re-pair"
const disconnectedAlertText = "WA forwarder disconnected for 15 min"

// identityLookup answers who is behind a JID from whatsmeow's store.
type identityLookup interface {
	// phoneForLID returns the phone-number JID for a @lid JID, or an empty JID if unknown.
	phoneForLID(ctx context.Context, lid types.JID) (types.JID, error)
	contact(ctx context.Context, phone types.JID) (types.ContactInfo, error)
}

// socketState reports whether the WhatsApp websocket is connected right now.
type socketState interface {
	IsConnected() bool
}

type schedulerControl interface {
	Pause()
	Resume()
}

type connectionAlert int

const (
	noConnectionAlert connectionAlert = iota
	disconnectedConnectionAlert
	reconnectedConnectionAlert
)

type WhatsApp struct {
	identities  identityLookup
	socket      socketState
	queue       *Queue
	scheduler   schedulerControl
	sms         SMSSender
	now         Clock
	onLoggedOut func()

	mutex                   sync.Mutex
	isConnected             bool
	isWaitingForOfflineSync bool
	wasDisconnectAlertSent  bool
	connectedAt             time.Time
	disconnectedAt          time.Time
}

// Lifecycle
// ---------------------------------------------------------------------------

// NewWhatsApp returns the event adapter. Register HandleEvent with
// client.AddEventHandlerWithSuccessStatus and run WatchConnection in a goroutine.
func NewWhatsApp(client *whatsmeow.Client, queue *Queue, scheduler *Scheduler, sms SMSSender, now Clock, onLoggedOut func()) *WhatsApp {
	return newWhatsApp(storeIdentityLookup{client: client}, client, queue, scheduler, sms, now, onLoggedOut)
}

func newWhatsApp(identities identityLookup, socket socketState, queue *Queue, scheduler schedulerControl, sms SMSSender, now Clock, onLoggedOut func()) *WhatsApp {
	return &WhatsApp{
		identities:     identities,
		socket:         socket,
		queue:          queue,
		scheduler:      scheduler,
		sms:            sms,
		now:            now,
		onLoggedOut:    onLoggedOut,
		disconnectedAt: now(),
	}
}

// HandleEvent handles one whatsmeow event. Returns false when the event must not be acked
// (the queue write failed), so WhatsApp redelivers it.
func (whatsApp *WhatsApp) HandleEvent(rawEvent any) bool {
	switch event := rawEvent.(type) {
	case *events.Message:
		if event.Info.IsFromMe {
			return whatsApp.markReplied(event)
		}
		return whatsApp.addMessage(event)
	case *events.Receipt:
		if event.Type == types.ReceiptTypeReadSelf {
			return whatsApp.markRead(event)
		}
	case *events.OfflineSyncPreview:
		log.Printf("offline sync preview: %d events (%d messages, %d receipts, %d notifications, %d app data changes)",
			event.Total, event.Messages, event.Receipts, event.Notifications, event.AppDataChanges)
	case *events.OfflineSyncCompleted:
		whatsApp.handleOfflineSyncCompleted(event.Count)
	case *events.Connected:
		whatsApp.handleConnected()
	case *events.Disconnected:
		if whatsApp.socket.IsConnected() {
			log.Printf("ignoring stale disconnect event, already reconnected")
			return true
		}
		whatsApp.handleDisconnected("disconnected")
	case *events.LoggedOut:
		whatsApp.handleLoggedOut(event)
	case events.PermanentDisconnect:
		whatsApp.handleDisconnected("permanently disconnected: " + event.PermanentDisconnectDescription())
	}
	return true
}

// WatchConnection sends the disconnect/reconnect alerts and the offline-sync fallback until ctx
// is cancelled.
func (whatsApp *WhatsApp) WatchConnection(ctx context.Context) {
	ticker := time.NewTicker(connectionCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			whatsApp.checkConnection(ctx)
		}
	}
}

// Messages
// ---------------------------------------------------------------------------

func (whatsApp *WhatsApp) addMessage(event *events.Message) bool {
	if !isPersonOrGroup(event.Info.Chat) {
		return true
	}
	text, hasContent := messageText(event.Message)
	if !hasContent {
		return true
	}

	ctx := context.Background()
	phone := whatsApp.phoneJID(ctx, event.Info.MessageSource)
	message := Message{
		MessageID:  event.Info.ID,
		ChatJID:    whatsApp.normalizedChat(ctx, event.Info.Chat),
		SenderName: whatsApp.contactName(ctx, phone, event.Info.PushName),
		Text:       text,
		ReceivedAt: event.Info.Timestamp,
	}
	if !phone.IsEmpty() {
		message.SenderPhone = "+" + phone.User
	}

	isAdded, err := whatsApp.queue.Add(message, whatsApp.now())
	if err != nil {
		log.Printf("failed to queue %s: %v", message.MessageID, err)
		return false
	}
	if isAdded {
		log.Printf("waiting on %s from %s in %s", message.MessageID, message.SenderName, message.ChatJID)
	}
	return true
}

func (whatsApp *WhatsApp) markReplied(event *events.Message) bool {
	chat := whatsApp.normalizedChat(context.Background(), event.Info.Chat)
	count, err := whatsApp.queue.MarkReplied(chat, event.Info.Timestamp, whatsApp.now())
	if err != nil {
		log.Printf("failed to mark %s replied: %v", chat, err)
		return false
	}
	if count > 0 {
		log.Printf("replied in %s, cancelled %d", chat, count)
	}
	return true
}

func (whatsApp *WhatsApp) markRead(event *events.Receipt) bool {
	count, err := whatsApp.queue.MarkRead(event.MessageIDs, whatsApp.now())
	if err != nil {
		log.Printf("failed to mark %v read: %v", event.MessageIDs, err)
		return false
	}
	if count > 0 {
		log.Printf("read %v, cancelled %d", event.MessageIDs, count)
	}
	return true
}

// Sender identity
// ---------------------------------------------------------------------------

// normalizedChat returns the chat JID to store: the phone-number JID for @lid chats the LID map
// knows, otherwise the chat JID itself.
func (whatsApp *WhatsApp) normalizedChat(ctx context.Context, chat types.JID) string {
	chat = chat.ToNonAD()
	if chat.Server != types.HiddenUserServer {
		return chat.String()
	}
	phone, err := whatsApp.identities.phoneForLID(ctx, chat)
	if err != nil || phone.IsEmpty() {
		return chat.String()
	}
	return phone.ToNonAD().String()
}

// phoneJID returns the sender's phone-number JID, or an empty JID if WhatsApp hid the number
// behind a LID that isn't in the LID map.
func (whatsApp *WhatsApp) phoneJID(ctx context.Context, source types.MessageSource) types.JID {
	phone := source.Sender
	if phone.Server == types.HiddenUserServer {
		phone = source.SenderAlt
	}
	if phone.Server == types.DefaultUserServer {
		return phone.ToNonAD()
	}
	if source.Sender.Server != types.HiddenUserServer {
		return types.JID{}
	}
	mapped, err := whatsApp.identities.phoneForLID(ctx, source.Sender.ToNonAD())
	if err != nil {
		return types.JID{}
	}
	return mapped.ToNonAD()
}

func (whatsApp *WhatsApp) contactName(ctx context.Context, phone types.JID, pushName string) string {
	if !phone.IsEmpty() {
		contact, err := whatsApp.identities.contact(ctx, phone)
		if err == nil && contact.FullName != "" {
			return contact.FullName
		}
		if err == nil && contact.FirstName != "" {
			return contact.FirstName
		}
	}
	return pushName
}

// Connection
// ---------------------------------------------------------------------------

func (whatsApp *WhatsApp) handleConnected() {
	whatsApp.mutex.Lock()
	defer whatsApp.mutex.Unlock()
	whatsApp.scheduler.Pause()
	whatsApp.isConnected = true
	whatsApp.isWaitingForOfflineSync = true
	whatsApp.connectedAt = whatsApp.now()
	log.Printf("connected, waiting for offline sync")
}

func (whatsApp *WhatsApp) handleOfflineSyncCompleted(count int) {
	whatsApp.mutex.Lock()
	defer whatsApp.mutex.Unlock()
	whatsApp.isWaitingForOfflineSync = false
	whatsApp.scheduler.Resume()
	log.Printf("offline sync completed (%d events), scheduler started", count)
}

func (whatsApp *WhatsApp) handleDisconnected(description string) {
	whatsApp.mutex.Lock()
	defer whatsApp.mutex.Unlock()
	whatsApp.scheduler.Pause()
	whatsApp.isWaitingForOfflineSync = false
	if whatsApp.isConnected {
		whatsApp.isConnected = false
		whatsApp.disconnectedAt = whatsApp.now()
	}
	log.Printf("%s, scheduler paused", description)
}

func (whatsApp *WhatsApp) handleLoggedOut(event *events.LoggedOut) {
	whatsApp.handleDisconnected("logged out: " + event.PermanentDisconnectDescription())
	ctx, cancel := context.WithTimeout(context.Background(), logoutAlertTimeout)
	defer cancel()
	if _, err := whatsApp.sms.Send(ctx, "", loggedOutAlertText); err != nil {
		log.Printf("failed to send logout alert: %v", err)
	}
	whatsApp.onLoggedOut()
}

// checkConnection resumes the scheduler if offline sync never completed, and sends a pending
// disconnect/reconnect alert.
func (whatsApp *WhatsApp) checkConnection(ctx context.Context) {
	whatsApp.resumeIfOfflineSyncOverdue()
	alert, alertText := whatsApp.nextConnectionAlert()
	if alert == noConnectionAlert {
		return
	}
	if _, err := whatsApp.sms.Send(ctx, "", alertText); err != nil {
		log.Printf("failed to send connection alert: %v", err)
		return
	}
	log.Printf("sent alert: %s", alertText)

	whatsApp.mutex.Lock()
	defer whatsApp.mutex.Unlock()
	whatsApp.wasDisconnectAlertSent = alert == disconnectedConnectionAlert
}

func (whatsApp *WhatsApp) resumeIfOfflineSyncOverdue() {
	whatsApp.mutex.Lock()
	defer whatsApp.mutex.Unlock()
	if whatsApp.isConnected && whatsApp.isWaitingForOfflineSync && whatsApp.now().Sub(whatsApp.connectedAt) >= offlineSyncTimeout {
		whatsApp.isWaitingForOfflineSync = false
		whatsApp.scheduler.Resume()
		log.Printf("WARNING: no offline sync completed %v after connecting, scheduler started anyway", offlineSyncTimeout)
	}
}

func (whatsApp *WhatsApp) nextConnectionAlert() (connectionAlert, string) {
	whatsApp.mutex.Lock()
	defer whatsApp.mutex.Unlock()
	now := whatsApp.now()
	switch {
	case whatsApp.isConnected && whatsApp.wasDisconnectAlertSent:
		downtime := whatsApp.connectedAt.Sub(whatsApp.disconnectedAt)
		return reconnectedConnectionAlert, "WA forwarder reconnected after " + durationText(downtime)
	case !whatsApp.isConnected && !whatsApp.wasDisconnectAlertSent && now.Sub(whatsApp.disconnectedAt) >= disconnectAlertDelay:
		return disconnectedConnectionAlert, disconnectedAlertText
	}
	return noConnectionAlert, ""
}

func durationText(duration time.Duration) string {
	minutes := int(duration.Round(time.Minute).Minutes())
	if minutes < 60 {
		return fmt.Sprintf("%d min", minutes)
	}
	return fmt.Sprintf("%d h %d min", minutes/60, minutes%60)
}

// Message content
// ---------------------------------------------------------------------------

func isPersonOrGroup(chat types.JID) bool {
	switch chat.Server {
	case types.DefaultUserServer, types.HiddenUserServer, types.GroupServer:
		return true
	}
	return false
}

// messageText returns what to put in the SMS, or false for messages with nothing to show
// (reactions, edits, deletions and other protocol messages).
func messageText(message *waE2E.Message) (string, bool) {
	switch {
	case message.GetConversation() != "":
		return message.GetConversation(), true
	case message.GetExtendedTextMessage() != nil:
		return message.GetExtendedTextMessage().GetText(), true
	case message.GetImageMessage() != nil:
		return withCaption("[photo]", message.GetImageMessage().GetCaption()), true
	case message.GetVideoMessage() != nil:
		return withCaption("[video]", message.GetVideoMessage().GetCaption()), true
	case message.GetDocumentMessage() != nil:
		return withCaption("[file]", message.GetDocumentMessage().GetCaption()), true
	case message.GetAudioMessage() != nil:
		return "[voice message]", true
	case message.GetStickerMessage() != nil:
		return "[sticker]", true
	case message.GetLocationMessage() != nil, message.GetLiveLocationMessage() != nil:
		return "[location]", true
	case message.GetContactMessage() != nil:
		return "[contact]", true
	}
	return "", false
}

func withCaption(label string, caption string) string {
	if caption == "" {
		return label
	}
	return label + " " + caption
}

// whatsmeow store lookups
// ---------------------------------------------------------------------------

// storeIdentityLookup reads client.Store on every call: pairing replaces the device's contents.
type storeIdentityLookup struct {
	client *whatsmeow.Client
}

func (lookup storeIdentityLookup) phoneForLID(ctx context.Context, lid types.JID) (types.JID, error) {
	return lookup.client.Store.LIDs.GetPNForLID(ctx, lid)
}

func (lookup storeIdentityLookup) contact(ctx context.Context, phone types.JID) (types.ContactInfo, error) {
	return lookup.client.Store.Contacts.GetContact(ctx, phone)
}
