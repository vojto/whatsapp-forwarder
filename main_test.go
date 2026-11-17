package main

import (
	"io"
	"log"
	"os"
	"testing"
)

// TestMain silences the standard logger: every component logs its decisions, which would bury
// test failures. Run with -v and comment this out to see them.
func TestMain(m *testing.M) {
	log.SetOutput(io.Discard)
	os.Exit(m.Run())
}
