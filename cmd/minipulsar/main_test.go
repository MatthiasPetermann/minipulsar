package main

import (
	"log/slog"
	"testing"
)

func TestParseLogLevel(t *testing.T) {
	// Pulsar CLI must map log-level flags to known slog levels,
	// so we verify both known levels and an error for unknown input.
	level, err := parseLogLevel("debug")
	if err != nil {
		t.Fatalf("parse log level: %v", err)
	}
	if level != slog.LevelDebug {
		t.Fatalf("unexpected level: %v", level)
	}
	if _, err := parseLogLevel("unknown"); err == nil {
		t.Fatalf("expected error for unknown log level")
	}
}

func TestBuildTLSConfigRequiresPair(t *testing.T) {
	// Pulsar TLS listeners require both certificate and key,
	// so buildTLSConfig should error when only one path is provided.
	if _, err := buildTLSConfig("", "key.pem"); err == nil {
		t.Fatalf("expected error when only key provided")
	}
	if _, err := buildTLSConfig("cert.pem", ""); err == nil {
		t.Fatalf("expected error when only cert provided")
	}
	cfg, err := buildTLSConfig("", "")
	if err != nil {
		t.Fatalf("unexpected error for empty paths: %v", err)
	}
	if cfg != nil {
		t.Fatalf("expected nil config for empty paths")
	}
}
