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
	TwilioFromNumber    string
	SMSRecipientNumber  string
}

func loadConfig() Config {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("load .env: %v", err)
	}
	return Config{
		WhatsAppPhoneNumber: requireEnv("WHATSAPP_PHONE_NUMBER"),
		TwilioAccountSID:    requireEnv("TWILIO_ACCOUNT_SID"),
		TwilioAuthToken:     requireEnv("TWILIO_AUTH_TOKEN"),
		TwilioFromNumber:    requireEnv("TWILIO_FROM_NUMBER"),
		SMSRecipientNumber:  requireEnv("SMS_RECIPIENT_NUMBER"),
	}
}

func requireEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("missing %s in .env", name)
	}
	return value
}
