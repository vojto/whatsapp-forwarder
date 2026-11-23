// whatsapp-forwarder: forwards WhatsApp messages by SMS when you haven't seen them on your main phone
// within three minutes.
//
// Expected behavior:
//   - Runs as a linked WhatsApp device (whatsmeow) under launchd. whatsapp.go turns WhatsApp events
//     into rows in forwarder.db (queue.go); scheduler.go forwards rows that are still pending when
//     they become due, through Twilio (twilio.go), within the rules in rules.go and only when the
//     forward judge (judge.go) thinks they're worth it, then polls Twilio
//     until each SMS is delivered or known to have failed.
//   - First run prints a pairing code to the log; enter it in WhatsApp > Linked devices >
//     Link a device > Link with phone number instead. The session is then kept in store.db.
//   - The scheduler starts paused and only runs after WhatsApp's offline sync, so messages read
//     while the forwarder was down are not forwarded.
//
// Gotchas:
//   - store.db is whatsmeow's paired session. forwarder.db is ours. Never delete store.db unless
//     re-pairing on purpose; deleting forwarder.db only loses pending forwards.
//   - SynchronousAck makes whatsmeow ack messages and receipts only after HandleEvent returns, so
//     a failed queue write leads to redelivery instead of a lost message.
//   - PairPhone only works after Connect and once the QR channel has produced its first code.
//   - Configuration comes from .env in the working directory (see .env.example).
//   - The app writes its own size-capped log; launchd's stdout/stderr file only catches panics.

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	_ "modernc.org/sqlite"
)

const whatsmeowStorePath = "file:store.db?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
const queuePath = "forwarder.db"

// Lifecycle
// ---------------------------------------------------------------------------

func main() {
	logFile := openLog()
	defer logFile.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	config := loadConfig()
	queue := openQueue()
	defer queue.Close()
	client := newClient(ctx)

	sms := NewTwilioSender(config)
	scheduler := NewScheduler(queue, newWhatsmeowChats(client), sms, sms, newForwardJudge(config), time.Now)
	whatsApp := NewWhatsApp(client, queue, scheduler, sms, time.Now, func() {
		log.Fatal("logged out of WhatsApp; restarting into pairing")
	})
	client.AddEventHandlerWithSuccessStatus(whatsApp.HandleEvent)

	schedulerDone := make(chan struct{})
	go func() {
		scheduler.Run(ctx)
		close(schedulerDone)
	}()
	go whatsApp.WatchConnection(ctx)

	if client.Store.ID == nil {
		pair(ctx, client, config.WhatsAppPhoneNumber)
	} else if err := client.Connect(); err != nil {
		log.Fatalf("connect: %v", err)
	}
	log.Print("running")

	<-ctx.Done()
	log.Print("shutting down")
	<-schedulerDone
	client.Disconnect()
}

// Setup
// ---------------------------------------------------------------------------

func openLog() *rotatingLog {
	path, err := defaultLogPath()
	if err != nil {
		log.Fatalf("log path: %v", err)
	}
	logFile, err := openRotatingLog(path, maxLogBytes)
	if err != nil {
		log.Fatalf("open log: %v", err)
	}
	log.SetOutput(logFile)
	return logFile
}

// newForwardJudge returns Jev when TYPESAFE_API_KEY is set, otherwise nil (forward everything).
func newForwardJudge(config Config) ForwardJudge {
	if config.TypeSafeAPIKey == "" {
		log.Print("no TYPESAFE_API_KEY, forwarding every message")
		return nil
	}
	return newJevJudge(config.TypeSafeAPIKey)
}

func openQueue() *Queue {
	queue, err := OpenQueue(queuePath)
	if err != nil {
		log.Fatalf("open queue: %v", err)
	}
	count, err := queue.FailInterruptedSends(time.Now())
	if err != nil {
		log.Fatalf("fail interrupted sends: %v", err)
	}
	if count > 0 {
		log.Printf("marked %d messages failed: %s", count, crashedMidSendReason)
	}
	return queue
}

func newClient(ctx context.Context) *whatsmeow.Client {
	container, err := sqlstore.New(ctx, "sqlite", whatsmeowStorePath, newWhatsmeowLogger("store"))
	if err != nil {
		log.Fatalf("open whatsmeow store: %v", err)
	}
	device, err := container.GetFirstDevice(ctx)
	if err != nil {
		log.Fatalf("load device: %v", err)
	}
	client := whatsmeow.NewClient(device, newWhatsmeowLogger("whatsmeow"))
	client.SynchronousAck = true
	return client
}

// Pairing
// ---------------------------------------------------------------------------

func pair(ctx context.Context, client *whatsmeow.Client, phoneNumber string) {
	qrChannel, err := client.GetQRChannel(ctx)
	if err != nil {
		log.Fatalf("qr channel: %v", err)
	}
	if err := client.Connect(); err != nil {
		log.Fatalf("connect: %v", err)
	}

	hasRequestedCode := false
	for item := range qrChannel {
		switch {
		case item.Event == "code" && !hasRequestedCode:
			hasRequestedCode = true
			code, err := client.PairPhone(ctx, phoneNumber, true, whatsmeow.PairClientChrome, "Chrome (macOS)")
			if err != nil {
				log.Fatalf("pair phone: %v", err)
			}
			log.Printf("pairing code: %s", code)
		case item.Event == "success":
			log.Print("paired")
		case item.Event != "code":
			log.Fatalf("pairing failed: %s", item.Event)
		}
	}
}
