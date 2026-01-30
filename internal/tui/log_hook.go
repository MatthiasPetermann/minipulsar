package tui

import (
	"strings"
	"sync"
)

// LogWriter streams formatted log output into a provided sink.
// It implements io.Writer so it can be used with the logrus SetOutput API.
type LogWriter struct {
	mu   sync.Mutex
	buf  strings.Builder
	send func(string)
}

// NewLogWriter creates a LogWriter with the given send callback.
func NewLogWriter(send func(string)) *LogWriter {
	return &LogWriter{send: send}
}

// Write buffers incoming bytes and forwards complete lines to the send callback.
func (w *LogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, _ = w.buf.Write(p)
	data := w.buf.String()

	for {
		idx := strings.IndexByte(data, '\n')
		if idx == -1 {
			break
		}
		line := strings.TrimRight(data[:idx], "\r")
		w.send(line)
		data = data[idx+1:]
	}

	w.buf.Reset()
	if len(data) > 0 {
		w.buf.WriteString(data)
	}

	return len(p), nil
}
