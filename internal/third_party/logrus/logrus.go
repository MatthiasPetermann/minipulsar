package logrus

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Level uint32

const (
	TraceLevel Level = iota
	DebugLevel
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

func ParseLevel(level string) (Level, error) {
	switch strings.ToLower(level) {
	case "trace":
		return TraceLevel, nil
	case "debug":
		return DebugLevel, nil
	case "info":
		return InfoLevel, nil
	case "warn", "warning":
		return WarnLevel, nil
	case "error":
		return ErrorLevel, nil
	case "fatal":
		return FatalLevel, nil
	default:
		return InfoLevel, fmt.Errorf("unknown level: %s", level)
	}
}

type Fields map[string]interface{}

type Formatter interface {
	Format(*Entry) ([]byte, error)
}

type TextFormatter struct {
	FullTimestamp bool
}

type JSONFormatter struct {
	DisableTimestamp bool
}

type Logger struct {
	mu           sync.Mutex
	level        Level
	out          io.Writer
	formatter    Formatter
	reportCaller bool
}

func New() *Logger {
	return &Logger{
		level:     InfoLevel,
		out:       os.Stdout,
		formatter: &TextFormatter{FullTimestamp: true},
	}
}

func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

func (l *Logger) SetOutput(out io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.out = out
}

func (l *Logger) SetFormatter(formatter Formatter) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.formatter = formatter
}

func (l *Logger) SetReportCaller(report bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reportCaller = report
}

func (l *Logger) WithField(key string, value interface{}) *Entry {
	return &Entry{Logger: l, Data: Fields{key: value}}
}

func (l *Logger) WithFields(fields Fields) *Entry {
	data := make(Fields, len(fields))
	for k, v := range fields {
		data[k] = v
	}
	return &Entry{Logger: l, Data: data}
}

func (l *Logger) WithError(err error) *Entry {
	return &Entry{Logger: l, Data: Fields{"error": err}}
}

type Entry struct {
	Logger *Logger
	Data   Fields
}

func (e *Entry) WithField(key string, value interface{}) *Entry {
	return e.WithFields(Fields{key: value})
}

func (e *Entry) WithFields(fields Fields) *Entry {
	data := make(Fields, len(e.Data)+len(fields))
	for k, v := range e.Data {
		data[k] = v
	}
	for k, v := range fields {
		data[k] = v
	}
	return &Entry{Logger: e.Logger, Data: data}
}

func (e *Entry) WithError(err error) *Entry {
	return e.WithField("error", err)
}

func (e *Entry) log(level Level, msg string) {
	logger := e.Logger
	logger.mu.Lock()
	defer logger.mu.Unlock()

	if level < logger.level {
		return
	}

	entry := &Entry{Logger: logger, Data: make(Fields, len(e.Data))}
	for k, v := range e.Data {
		entry.Data[k] = v
	}
	entry.Data["level"] = level.String()
	entry.Data["msg"] = msg
	entry.Data["time"] = time.Now()

	formatter := logger.formatter
	if formatter == nil {
		formatter = &TextFormatter{FullTimestamp: true}
	}
	line, err := formatter.Format(entry)
	if err != nil {
		line = []byte(fmt.Sprintf("%s level=%s msg=%s format_error=%v\n", time.Now().Format(time.RFC3339), level.String(), msg, err))
	}
	_, _ = logger.out.Write(line)

	if level == FatalLevel {
		os.Exit(1)
	}
}

func (e *Entry) Trace(msg string) { e.log(TraceLevel, msg) }
func (e *Entry) Debug(msg string) { e.log(DebugLevel, msg) }
func (e *Entry) Info(msg string)  { e.log(InfoLevel, msg) }
func (e *Entry) Warn(msg string)  { e.log(WarnLevel, msg) }
func (e *Entry) Error(msg string) { e.log(ErrorLevel, msg) }
func (e *Entry) Fatal(msg string) { e.log(FatalLevel, msg) }

func (l *Logger) Trace(msg string) { l.WithFields(nil).Trace(msg) }
func (l *Logger) Debug(msg string) { l.WithFields(nil).Debug(msg) }
func (l *Logger) Info(msg string)  { l.WithFields(nil).Info(msg) }
func (l *Logger) Warn(msg string)  { l.WithFields(nil).Warn(msg) }
func (l *Logger) Error(msg string) { l.WithFields(nil).Error(msg) }
func (l *Logger) Fatal(msg string) { l.WithFields(nil).Fatal(msg) }

func (level Level) String() string {
	switch level {
	case TraceLevel:
		return "trace"
	case DebugLevel:
		return "debug"
	case InfoLevel:
		return "info"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	case FatalLevel:
		return "fatal"
	default:
		return "info"
	}
}

func (f *TextFormatter) Format(entry *Entry) ([]byte, error) {
	timestamp := time.Now().Format(time.RFC3339)
	if !f.FullTimestamp {
		timestamp = time.Now().Format("15:04:05")
	}

	fields := flattenFields(entry.Data)
	return []byte(fmt.Sprintf("%s level=%s %s\n", timestamp, entry.Data["level"], fields)), nil
}

func (f *JSONFormatter) Format(entry *Entry) ([]byte, error) {
	payload := make(map[string]interface{}, len(entry.Data)+1)
	for k, v := range entry.Data {
		payload[k] = v
	}
	if f.DisableTimestamp {
		delete(payload, "time")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func flattenFields(fields Fields) string {
	pairs := make([]string, 0, len(fields))
	for k, v := range fields {
		if k == "level" || k == "msg" || k == "time" {
			continue
		}
		pairs = append(pairs, fmt.Sprintf("%s=%v", k, v))
	}
	sort.Strings(pairs)
	msg := fmt.Sprintf("msg=%v", fields["msg"])
	return strings.Join(append([]string{msg}, pairs...), " ")
}
