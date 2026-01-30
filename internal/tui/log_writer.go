package tui

import (
	"bytes"
	"strings"
)

// LogWriter forwards log lines into a channel for TUI display.
type LogWriter struct {
	ch chan<- string
}

// NewLogWriter creates a LogWriter that publishes to the provided channel.
func NewLogWriter(ch chan<- string) *LogWriter {
	return &LogWriter{ch: ch}
}

func (w *LogWriter) Write(p []byte) (int, error) {
	lines := bytes.Split(p, []byte("\n"))
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		w.ch <- strings.TrimRight(string(line), "\r")
	}
	return len(p), nil
}
