package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// Logger is a lightweight wrapper that standardizes logging across service and TUI modes.
// It delegates to slog while allowing the output to be swapped based on runtime mode.
type Logger struct {
	base     *slog.Logger
	levelVar *slog.LevelVar
}

// Options configures the logger output and formatting.
type Options struct {
	Format        string
	WithTimestamp bool
	Level         slog.Level
	LevelVar      *slog.LevelVar
	TimeFormat    string
	Writer        io.Writer
}

// New builds a Logger using slog handlers and the provided options.
func New(opts Options) (*Logger, error) {
	writer := opts.Writer
	if writer == nil {
		writer = os.Stdout
	}
	handlerOpts := &slog.HandlerOptions{
		Level: opts.Level,
	}
	if opts.LevelVar != nil {
		handlerOpts.Level = opts.LevelVar
	}
	if !opts.WithTimestamp || opts.TimeFormat != "" {
		handlerOpts.ReplaceAttr = func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				if !opts.WithTimestamp {
					return slog.Attr{}
				}
				if opts.TimeFormat != "" {
					return slog.String(slog.TimeKey, attr.Value.Time().Format(opts.TimeFormat))
				}
			}
			return attr
		}
	}

	var handler slog.Handler
	switch strings.ToLower(opts.Format) {
	case "text":
		handler = slog.NewTextHandler(writer, handlerOpts)
	case "json":
		handler = slog.NewJSONHandler(writer, handlerOpts)
	default:
		return nil, fmt.Errorf("invalid log-format %q (expected text or json)", opts.Format)
	}

	return &Logger{base: slog.New(handler), levelVar: opts.LevelVar}, nil
}

// With returns a logger with additional key/value context.
func (l *Logger) With(args ...any) *Logger {
	if l == nil || l.base == nil {
		return l
	}
	return &Logger{base: l.base.With(args...)}
}

// Debug logs a debug message.
func (l *Logger) Debug(msg string, args ...any) {
	if l == nil || l.base == nil {
		return
	}
	l.base.Debug(msg, args...)
}

// Info logs an informational message.
func (l *Logger) Info(msg string, args ...any) {
	if l == nil || l.base == nil {
		return
	}
	l.base.Info(msg, args...)
}

// Warn logs a warning message.
func (l *Logger) Warn(msg string, args ...any) {
	if l == nil || l.base == nil {
		return
	}
	l.base.Warn(msg, args...)
}

// Error logs an error message.
func (l *Logger) Error(msg string, args ...any) {
	if l == nil || l.base == nil {
		return
	}
	l.base.Error(msg, args...)
}

// SetLevel updates the logger's minimum log level when configured with a LevelVar.
func (l *Logger) SetLevel(level slog.Level) {
	if l == nil || l.levelVar == nil {
		return
	}
	l.levelVar.Set(level)
}
