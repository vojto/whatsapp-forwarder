package main

// Expected behavior:
//   - forwarder.db holds every incoming message the forwarder cares about, one row per WhatsApp
//     message ID, so pending forwards survive restarts. whatsmeow's store.db is never touched here.
//   - Rows start pending with due_at = received_at + forwardDelay. Read receipts and your own
//     messages move pending rows to read/replied; the scheduler claims due rows (sending) and
//     finishes them as sent/skipped/stale/failed, or puts them back to pending for a retry.
//   - Every status change only applies to rows still in the expected state, so a read receipt can
//     never undo a send and a late scheduler can never resurrect a read message.
//
// Gotchas:
//   - Add is INSERT OR IGNORE: WhatsApp redelivers messages after reconnects and the first copy wins.
//   - A read receipt can arrive before its message (offline backlog order isn't guaranteed), so
//     MarkRead leaves a placeholder row marked read; the message's later Add is then ignored.
//   - Rows left in sending at startup were mid-send when the process died. They become failed,
//     never pending again: a possibly missing SMS beats a duplicate one.
//   - One connection only (SetMaxOpenConns(1)): SQLite serializes writers anyway, and this keeps
//     ClaimNextDue's read-then-update atomic without explicit transactions.
//   - A read receipt or reply can arrive while its row is sending. It can't change the status
//     then, so it's recorded in cancelled_by; if that send is then retried, Retry finishes the row
//     as read/replied instead of putting it back to pending.
//   - Sent rows also track delivery: sent_at, and Twilio's delivery_status/delivery_error, filled
//     in by the scheduler's delivery checks until the status is final.
//   - Columns added after the first release live in addedColumns, not in schema: OpenQueue adds
//     whichever an existing forwarder.db is missing, so old files keep working without a reset.
//   - Times are stored as Unix milliseconds.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const forwardDelay = 3 * time.Minute
const crashedMidSendReason = "unknown (crashed mid-send)"

type MessageStatus string

const (
	StatusPending MessageStatus = "pending"
	StatusRead    MessageStatus = "read"
	StatusReplied MessageStatus = "replied"
	StatusSkipped MessageStatus = "skipped"
	StatusStale   MessageStatus = "stale"
	StatusSending MessageStatus = "sending"
	StatusSent    MessageStatus = "sent"
	StatusFailed  MessageStatus = "failed"
)

type Message struct {
	MessageID   string
	ChatJID     string
	SenderName  string
	SenderPhone string
	Text        string
	ReceivedAt  time.Time
	DueAt       time.Time
	Status      MessageStatus
	Reason      string
	Attempts    int
	TwilioSID   string
	UpdatedAt   time.Time

	SentAt         time.Time
	DeliveryStatus string
	DeliveryError  string
}

type Queue struct {
	database *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS messages (
	message_id   TEXT PRIMARY KEY,
	chat_jid     TEXT NOT NULL DEFAULT '',
	sender_name  TEXT NOT NULL DEFAULT '',
	sender_phone TEXT NOT NULL DEFAULT '',
	text         TEXT NOT NULL DEFAULT '',
	received_at  INTEGER NOT NULL,
	due_at       INTEGER NOT NULL,
	status       TEXT NOT NULL,
	reason       TEXT NOT NULL DEFAULT '',
	attempts     INTEGER NOT NULL DEFAULT 0,
	twilio_sid   TEXT NOT NULL DEFAULT '',
	cancelled_by TEXT NOT NULL DEFAULT '',
	updated_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_status_due_at ON messages (status, due_at);
CREATE INDEX IF NOT EXISTS messages_chat_jid ON messages (chat_jid, status);
`

// addedColumns were added to messages after forwarder.db first shipped.
var addedColumns = []struct {
	name       string
	definition string
}{
	{"sent_at", "INTEGER NOT NULL DEFAULT 0"},
	{"delivery_status", "TEXT NOT NULL DEFAULT ''"},
	{"delivery_error", "TEXT NOT NULL DEFAULT ''"},
}

// finalDeliveryStatuses are the delivery statuses that are never checked again.
var finalDeliveryStatuses = []string{"delivered", "undelivered", "failed", deliveryStatusUnknown}

const messageColumns = `message_id, chat_jid, sender_name, sender_phone, text, received_at, due_at,
	status, reason, attempts, twilio_sid, updated_at, sent_at, delivery_status, delivery_error`

// Lifecycle
// ---------------------------------------------------------------------------

// OpenQueue opens (or creates) the queue database. path is a file path or ":memory:".
func OpenQueue(path string) (*Queue, error) {
	database, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, err
	}
	database.SetMaxOpenConns(1)
	queue := &Queue{database: database}
	if err := queue.migrate(); err != nil {
		database.Close()
		return nil, err
	}
	return queue, nil
}

func (queue *Queue) Close() error {
	return queue.database.Close()
}

// FailInterruptedSends marks rows left in sending by a previous process as failed.
// Call it once at startup, before the scheduler runs.
func (queue *Queue) FailInterruptedSends(now time.Time) (int, error) {
	return queue.exec(`UPDATE messages SET status = ?, reason = ?, updated_at = ? WHERE status = ?`,
		StatusFailed, crashedMidSendReason, millis(now), StatusSending)
}

// Incoming events
// ---------------------------------------------------------------------------

// Add stores a new pending message due forwardDelay after it was received.
// Returns false if a row with that message ID already exists.
func (queue *Queue) Add(message Message, now time.Time) (bool, error) {
	count, err := queue.exec(`INSERT OR IGNORE INTO messages
		(message_id, chat_jid, sender_name, sender_phone, text, received_at, due_at, status, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		message.MessageID, message.ChatJID, message.SenderName, message.SenderPhone, message.Text,
		millis(message.ReceivedAt), millis(message.ReceivedAt.Add(forwardDelay)), StatusPending, millis(now))
	return count > 0, err
}

// MarkRead marks pending messages as read, and records unknown IDs as read placeholders so the
// message itself is ignored if it arrives later. Returns how many pending messages it cancelled.
func (queue *Queue) MarkRead(messageIDs []string, now time.Time) (int, error) {
	cancelled := 0
	for _, messageID := range messageIDs {
		count, err := queue.exec(`UPDATE messages SET status = ?, reason = '', updated_at = ?
			WHERE message_id = ? AND status = ?`, StatusRead, millis(now), messageID, StatusPending)
		if err != nil {
			return cancelled, err
		}
		cancelled += count
		_, err = queue.exec(`UPDATE messages SET cancelled_by = ? WHERE message_id = ? AND status = ?`,
			StatusRead, messageID, StatusSending)
		if err != nil {
			return cancelled, err
		}
		_, err = queue.exec(`INSERT OR IGNORE INTO messages (message_id, received_at, due_at, status, updated_at)
			VALUES (?, ?, ?, ?, ?)`, messageID, millis(now), millis(now), StatusRead, millis(now))
		if err != nil {
			return cancelled, err
		}
	}
	return cancelled, nil
}

// MarkReplied marks pending messages in the chat received at or before your own message as replied.
func (queue *Queue) MarkReplied(chatJID string, repliedAt time.Time, now time.Time) (int, error) {
	count, err := queue.exec(`UPDATE messages SET status = ?, reason = '', updated_at = ?
		WHERE chat_jid = ? AND status = ? AND received_at <= ?`,
		StatusReplied, millis(now), chatJID, StatusPending, millis(repliedAt))
	if err != nil {
		return count, err
	}
	_, err = queue.exec(`UPDATE messages SET cancelled_by = ?
		WHERE chat_jid = ? AND status = ? AND received_at <= ?`,
		StatusReplied, chatJID, StatusSending, millis(repliedAt))
	return count, err
}

// Scheduling
// ---------------------------------------------------------------------------

// ClaimNextDue moves the earliest due pending message to sending, increments its attempts and
// returns it. Returns false when nothing is due.
func (queue *Queue) ClaimNextDue(now time.Time) (Message, bool, error) {
	row := queue.database.QueryRow(`UPDATE messages SET status = ?, attempts = attempts + 1, updated_at = ?
		WHERE message_id = (
			SELECT message_id FROM messages WHERE status = ? AND due_at <= ? ORDER BY due_at LIMIT 1
		)
		RETURNING `+messageColumns,
		StatusSending, millis(now), StatusPending, millis(now))
	message, err := scanMessage(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Message{}, false, nil
	}
	return message, err == nil, err
}

// Finish records the final outcome of a claimed message (sent, skipped, stale or failed).
// Sent messages get sent_at = now.
func (queue *Queue) Finish(messageID string, status MessageStatus, reason string, twilioSID string, now time.Time) error {
	_, err := queue.exec(`UPDATE messages SET status = ?, reason = ?, twilio_sid = ?, updated_at = ?,
		sent_at = CASE WHEN ? = ? THEN ? ELSE sent_at END
		WHERE message_id = ? AND status = ?`,
		status, reason, twilioSID, millis(now), status, StatusSent, millis(now), messageID, StatusSending)
	return err
}

// Retry puts a claimed message back to pending, due again at dueAt. If it was read or replied to
// while sending, it's finished as read/replied instead.
func (queue *Queue) Retry(messageID string, dueAt time.Time, reason string, now time.Time) error {
	_, err := queue.exec(`UPDATE messages
		SET status = CASE cancelled_by WHEN '' THEN ? ELSE cancelled_by END, due_at = ?, reason = ?, updated_at = ?
		WHERE message_id = ? AND status = ?`,
		StatusPending, millis(dueAt), reason, millis(now), messageID, StatusSending)
	return err
}

// Delivery
// ---------------------------------------------------------------------------

// UnconfirmedDeliveries returns sent messages whose delivery status isn't final yet, oldest first.
func (queue *Queue) UnconfirmedDeliveries() ([]Message, error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(finalDeliveryStatuses)), ", ")
	arguments := []any{StatusSent}
	for _, status := range finalDeliveryStatuses {
		arguments = append(arguments, status)
	}
	rows, err := queue.database.Query(`SELECT `+messageColumns+` FROM messages
		WHERE status = ? AND twilio_sid != '' AND delivery_status NOT IN (`+placeholders+`)
		ORDER BY sent_at`, arguments...)
	if err != nil {
		return nil, fmt.Errorf("SELECT: %w", err)
	}
	defer rows.Close()
	var messages []Message
	for rows.Next() {
		message, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, rows.Err()
}

// RecordDelivery stores Twilio's delivery status (and error, if any) for a sent message.
func (queue *Queue) RecordDelivery(messageID string, deliveryStatus string, deliveryError string, now time.Time) error {
	_, err := queue.exec(`UPDATE messages SET delivery_status = ?, delivery_error = ?, updated_at = ?
		WHERE message_id = ? AND status = ?`,
		deliveryStatus, deliveryError, millis(now), messageID, StatusSent)
	return err
}

// Housekeeping
// ---------------------------------------------------------------------------

// Prune deletes finished rows received before the cutoff. Pending and sending rows are kept.
func (queue *Queue) Prune(receivedBefore time.Time) (int, error) {
	return queue.exec(`DELETE FROM messages WHERE received_at < ? AND status NOT IN (?, ?)`,
		millis(receivedBefore), StatusPending, StatusSending)
}

// Get returns one message, or false if there is no row with that ID.
func (queue *Queue) Get(messageID string) (Message, bool, error) {
	row := queue.database.QueryRow(`SELECT `+messageColumns+` FROM messages WHERE message_id = ?`, messageID)
	message, err := scanMessage(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Message{}, false, nil
	}
	return message, err == nil, err
}

// Schema
// ---------------------------------------------------------------------------

func (queue *Queue) migrate() error {
	if _, err := queue.database.Exec(schema); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	existingColumns, err := queue.columnNames()
	if err != nil {
		return err
	}
	for _, column := range addedColumns {
		if existingColumns[column.name] {
			continue
		}
		if _, err := queue.database.Exec(`ALTER TABLE messages ADD COLUMN ` + column.name + ` ` + column.definition); err != nil {
			return fmt.Errorf("add column %s: %w", column.name, err)
		}
	}
	return nil
}

func (queue *Queue) columnNames() (map[string]bool, error) {
	rows, err := queue.database.Query(`SELECT name FROM pragma_table_info('messages')`)
	if err != nil {
		return nil, fmt.Errorf("read columns: %w", err)
	}
	defer rows.Close()
	names := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names[name] = true
	}
	return names, rows.Err()
}

// SQL helpers
// ---------------------------------------------------------------------------

func (queue *Queue) exec(query string, arguments ...any) (int, error) {
	result, err := queue.database.ExecContext(context.Background(), query, arguments...)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", strings.Fields(query)[0], err)
	}
	count, err := result.RowsAffected()
	return int(count), err
}

// scanMessage reads one messageColumns row from a *sql.Row or *sql.Rows.
func scanMessage(row interface{ Scan(...any) error }) (Message, error) {
	var message Message
	var receivedAt, dueAt, updatedAt, sentAt int64
	err := row.Scan(&message.MessageID, &message.ChatJID, &message.SenderName, &message.SenderPhone,
		&message.Text, &receivedAt, &dueAt, &message.Status, &message.Reason, &message.Attempts,
		&message.TwilioSID, &updatedAt, &sentAt, &message.DeliveryStatus, &message.DeliveryError)
	message.ReceivedAt = time.UnixMilli(receivedAt)
	message.DueAt = time.UnixMilli(dueAt)
	message.UpdatedAt = time.UnixMilli(updatedAt)
	if sentAt != 0 {
		message.SentAt = time.UnixMilli(sentAt)
	}
	return message, err
}

func millis(moment time.Time) int64 {
	return moment.UnixMilli()
}
