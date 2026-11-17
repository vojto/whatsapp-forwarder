# whatsapp-forwarder

Forwards WhatsApp messages by SMS to a dumb phone when they haven't been seen on the iPhone within
three minutes.

It runs on the Mac Studio as a linked WhatsApp device ([whatsmeow](https://github.com/tulir/whatsmeow))
under launchd, and sends SMS through Twilio.

## How it works

```
WhatsApp events ──▶ whatsapp.go ──▶ forwarder.db (queue.go) ◀── scheduler.go ──▶ twilio.go ──▶ SMS
                     received   → pending, due in 3 min           every 5 s: claim due rows,
                     read-self  → read                             check rules.go, send,
                     own reply  → replied (earlier msgs in chat)   record sent / skipped / failed
```

- Every message is a row in `forwarder.db`, so pending forwards survive restarts.
- **Read on the iPhone** (`read-self` receipt) or **replied in the chat** → not forwarded.
- **Skipped**: groups with more than 5 members, archived chats.
- **SMS format**: the sender is the contact's name as an alphanumeric sender ID (falls back to the
  Twilio number when rejected). The body is `+421…: text`, or `+421… @ Group: text` for groups.
  It is always one GSM-7 segment: diacritics stripped, emoji dropped, cut at 160 characters.
- **After downtime** the scheduler waits for WhatsApp's offline sync, so messages read in the
  meantime are dropped. Unread ones are still forwarded late. Messages older than 12 h are marked stale.
- **At most once**: a crash mid-send marks that message failed rather than risking a duplicate.
  Network/5xx errors are retried after 30 s, 2 min and 10 min.
- **Alerts by SMS**: logged out of WhatsApp; disconnected for more than 15 min (and back again).

## Setup

```bash
cp .env.example .env   # fill in Twilio credentials and the recipient number
./install.sh           # runs tests, builds, (re)starts the launchd agent
```

On first start the log shows a pairing code. On the phone, open WhatsApp › Linked devices ›
Link a device › *Link with phone number instead*, and enter it. The session is kept in `store.db`.

## Files

| Path | What |
| --- | --- |
| `.env` | Twilio credentials, numbers (never committed) |
| `store.db` | whatsmeow's paired session (never committed, don't delete unless re-pairing) |
| `forwarder.db` | the message queue (never committed, rows pruned after 30 days) |
| `~/Library/Logs/whatsapp-forwarder.log` | app log, rotated at 5 MB to `.log.1` |
| `~/Library/Logs/whatsapp-forwarder.crash.log` | launchd stdout/stderr, only panics |

## Operating

```bash
tail -f ~/Library/Logs/whatsapp-forwarder.log
sqlite3 forwarder.db "select datetime(received_at/1000,'unixepoch','localtime'), status, reason, sender_name, text from messages order by received_at desc limit 20"
launchctl kickstart -k gui/$(id -u)/com.vojto.whatsapp-forwarder   # restart
launchctl bootout gui/$(id -u)/com.vojto.whatsapp-forwarder        # stop
```

## Development

```bash
go test ./...
```

Scenario tests (`scheduler_test.go`, `whatsapp_test.go`) run on a real SQLite queue with a fake
clock, fake SMS sender and fake WhatsApp lookups. They cover restarts, offline backlog, crashes mid-send,
retries and connection alerts.
