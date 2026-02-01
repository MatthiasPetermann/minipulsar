package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestNewLoggerRejectsInvalidFormat(t *testing.T) {
	// Pulsar broker logging must be deterministic, so invalid format strings should be rejected.
	_, err := New(Options{Format: "xml", Level: slog.LevelInfo})
	if err == nil {
		t.Fatalf("expected error for invalid log format")
	}
}

func TestLoggerWithoutTimestampOmitsTime(t *testing.T) {
	// Pulsar operators can disable timestamps for structured log ingestion,
	// so we ensure the time attribute is removed when WithTimestamp is false.
	var buf bytes.Buffer
	logger, err := New(Options{
		Format:        "text",
		WithTimestamp: false,
		Level:         slog.LevelInfo,
		Writer:        &buf,
	})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}
	logger.Info("hello")
	output := buf.String()
	if strings.Contains(output, "time=") {
		t.Fatalf("expected no time attribute, got %q", output)
	}
}
