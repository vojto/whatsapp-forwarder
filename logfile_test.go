package main

import (
	"os"
	"path/filepath"
	"testing"
)

func readLogFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(content)
}

func writeLogLines(t *testing.T, logFile *rotatingLog, lines ...string) {
	t.Helper()
	for _, line := range lines {
		if _, err := logFile.Write([]byte(line)); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
}

func openTestLog(t *testing.T, path string, maxBytes int64) *rotatingLog {
	t.Helper()
	logFile, err := openRotatingLog(path, maxBytes)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { logFile.Close() })
	return logFile
}

func TestRotatingLogAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "app.log")
	logFile := openTestLog(t, path, 100)

	writeLogLines(t, logFile, "one\n", "two\n")

	if content := readLogFile(t, path); content != "one\ntwo\n" {
		t.Errorf("log = %q", content)
	}
	if _, err := os.Stat(path + ".1"); !os.IsNotExist(err) {
		t.Errorf("backup exists before rotation: %v", err)
	}
}

func TestRotatingLogRotatesOverLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	logFile := openTestLog(t, path, 10)

	writeLogLines(t, logFile, "12345\n", "abcde\n")

	if content := readLogFile(t, path+".1"); content != "12345\n" {
		t.Errorf("backup = %q", content)
	}
	if content := readLogFile(t, path); content != "abcde\n" {
		t.Errorf("log = %q", content)
	}
}

func TestRotatingLogKeepsOnlyOneBackup(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "app.log")
	logFile := openTestLog(t, path, 10)

	writeLogLines(t, logFile, "first1\n", "second\n", "third3\n")

	if content := readLogFile(t, path+".1"); content != "second\n" {
		t.Errorf("backup = %q", content)
	}
	if content := readLogFile(t, path); content != "third3\n" {
		t.Errorf("log = %q", content)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("got %d files, want log and one backup", len(entries))
	}
}

func TestRotatingLogReopenContinuesAppending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	firstLog, err := openRotatingLog(path, 10)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	writeLogLines(t, firstLog, "abc\n")
	firstLog.Close()

	secondLog := openTestLog(t, path, 10)
	writeLogLines(t, secondLog, "def\n")
	if content := readLogFile(t, path); content != "abc\ndef\n" {
		t.Errorf("log = %q", content)
	}

	// The reopened file's existing 8 bytes count toward the limit.
	writeLogLines(t, secondLog, "ghi\n")
	if content := readLogFile(t, path+".1"); content != "abc\ndef\n" {
		t.Errorf("backup = %q", content)
	}
	if content := readLogFile(t, path); content != "ghi\n" {
		t.Errorf("log = %q", content)
	}
}
