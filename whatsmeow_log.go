package main

// Expected behavior:
//   - whatsmeow logs WARN and ERROR lines through the standard log package, so they land in the
//     same rotating log file as the forwarder's own lines, tagged "[module LEVEL]".
//   - Lines known to be harmless noise are dropped.
//
// Gotchas:
//   - waLog.Stdout writes to stdout directly, which under launchd would bypass the rotating log
//     and fill the crash log instead. That's why this is a logger of its own, not a wrapper.
//   - App state sync for regular_low/regular_high permanently fails with "mismatching LTHash" on
//     this account and logs it on every reconnect. It only means archive state is unknown.
//   - whatsmeow logs mostly through Sub("...") loggers, so Sub keeps the filter and output.

import (
	"fmt"
	"log"
	"strings"

	waLog "go.mau.fi/whatsmeow/util/log"
)

var ignoredWhatsmeowLogFragments = []string{"mismatching LTHash"}

type whatsmeowLogger struct {
	module string
	output *log.Logger
}

// Lifecycle
// ---------------------------------------------------------------------------

func newWhatsmeowLogger(module string) waLog.Logger {
	return whatsmeowLogger{module: module, output: log.Default()}
}

func (logger whatsmeowLogger) Sub(module string) waLog.Logger {
	return whatsmeowLogger{module: logger.module + "/" + module, output: logger.output}
}

// Logging
// ---------------------------------------------------------------------------

func (logger whatsmeowLogger) Errorf(format string, arguments ...any) {
	logger.print("ERROR", format, arguments)
}

func (logger whatsmeowLogger) Warnf(format string, arguments ...any) {
	logger.print("WARN", format, arguments)
}

func (logger whatsmeowLogger) Infof(format string, arguments ...any) {}

func (logger whatsmeowLogger) Debugf(format string, arguments ...any) {}

func (logger whatsmeowLogger) print(level string, format string, arguments []any) {
	line := fmt.Sprintf(format, arguments...)
	if isIgnoredLogLine(line) {
		return
	}
	logger.output.Printf("[%s %s] %s", logger.module, level, line)
}

// Filtering
// ---------------------------------------------------------------------------

func isIgnoredLogLine(line string) bool {
	for _, fragment := range ignoredWhatsmeowLogFragments {
		if strings.Contains(line, fragment) {
			return true
		}
	}
	return false
}
