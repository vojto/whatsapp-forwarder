package main

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	WhatsAppPhoneNumber string
	TwilioAccountSID    string
	TwilioAuthToken     string
	SMSRecipientNumber  string
	// TypeSafeAPIKey enables the forward judge (Jev). Without it every message is forwarded.
	TypeSafeAPIKey string
}

func loadConfig() Config {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("load .env: %v", err)
	}
	return Config{
		WhatsAppPhoneNumber: requireEnv("WHATSAPP_PHONE_NUMBER"),
		TwilioAccountSID:    requireEnv("TWILIO_ACCOUNT_SID"),
		TwilioAuthToken:     requireEnv("TWILIO_AUTH_TOKEN"),
		SMSRecipientNumber:  requireEnv("SMS_RECIPIENT_NUMBER"),
		TypeSafeAPIKey:      os.Getenv("TYPESAFE_API_KEY"),
	}
}

func requireEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("missing %s in .env", name)
	}
	return value
}
