package tui

import (
	"strings"

	"github.com/sirupsen/logrus"
)

// LogHook streams formatted log lines to a provided sink.
type LogHook struct {
	formatter logrus.Formatter
	send      func(string)
}

// NewLogHook creates a LogHook with the given formatter and send callback.
func NewLogHook(formatter logrus.Formatter, send func(string)) *LogHook {
	return &LogHook{
		formatter: formatter,
		send:      send,
	}
}

// Levels returns all log levels handled by the hook.
func (h *LogHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire formats the log entry and forwards it to the send callback.
func (h *LogHook) Fire(entry *logrus.Entry) error {
	line, err := h.formatter.Format(entry)
	if err != nil {
		return err
	}
	text := strings.TrimRight(string(line), "\n")
	h.send(text)
	return nil
}
