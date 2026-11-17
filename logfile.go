package main

// Expected behavior:
//   - The process writes its own log to ~/Library/Logs/whatsapp-forwarder.log through rotatingLog,
//     an io.Writer shared by the standard log package and whatsmeow's logger.
//   - Writes append. When a write would push the file past maxBytes, the file is renamed to
//     <path>.1 (replacing any older .1) and a fresh file is started, so disk use stays under
//     about 2 × maxBytes.
//   - Reopening an existing log continues appending to it; its current size counts toward maxBytes.
//
// Gotchas:
//   - launchd's StandardOutPath/StandardErrorPath point at a separate crash log. That file only
//     catches what bypasses this writer (Go panics and runtime fatal errors), so it stays small.
//   - Writes come from several goroutines (log package, whatsmeow, scheduler), so the mutex
//     guards both the write and the rotation.
//   - A single write larger than maxBytes still goes into the fresh file whole; lines aren't split.
//   - If rotation fails, the write still goes to the current file: losing log lines is worse
//     than a log that's temporarily over the limit.

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const logFileName = "whatsapp-forwarder.log"
const maxLogBytes = 5 * 1024 * 1024

type rotatingLog struct {
	path        string
	maxBytes    int64
	mutex       sync.Mutex
	file        *os.File
	currentSize int64
}

// Lifecycle
// ---------------------------------------------------------------------------

// defaultLogPath returns ~/Library/Logs/whatsapp-forwarder.log.
func defaultLogPath() (string, error) {
	homeDirectory, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(homeDirectory, "Library", "Logs", logFileName), nil
}

func openRotatingLog(path string, maxBytes int64) (*rotatingLog, error) {
	logFile := &rotatingLog{path: path, maxBytes: maxBytes}
	if err := logFile.openFile(); err != nil {
		return nil, err
	}
	return logFile, nil
}

func (logFile *rotatingLog) Write(data []byte) (int, error) {
	logFile.mutex.Lock()
	defer logFile.mutex.Unlock()

	if logFile.currentSize > 0 && logFile.currentSize+int64(len(data)) > logFile.maxBytes {
		if err := logFile.rotate(); err != nil {
			fmt.Fprintf(os.Stderr, "rotate log %s: %v\n", logFile.path, err)
		}
	}
	written, err := logFile.file.Write(data)
	logFile.currentSize += int64(written)
	return written, err
}

func (logFile *rotatingLog) Close() error {
	logFile.mutex.Lock()
	defer logFile.mutex.Unlock()
	return logFile.file.Close()
}

// File handling
// ---------------------------------------------------------------------------

func (logFile *rotatingLog) openFile() error {
	if err := os.MkdirAll(filepath.Dir(logFile.path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(logFile.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}
	logFile.file = file
	logFile.currentSize = info.Size()
	return nil
}

// rotate moves the current file to <path>.1 and opens a fresh one. On failure the current file
// stays open so writes keep working.
func (logFile *rotatingLog) rotate() error {
	if err := os.Rename(logFile.path, logFile.path+".1"); err != nil {
		return err
	}
	previousFile := logFile.file
	if err := logFile.openFile(); err != nil {
		return err
	}
	return previousFile.Close()
}
