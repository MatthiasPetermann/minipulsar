package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"

	_ "github.com/mattn/go-sqlite3"

	"minipulsar/internal/broker"
	"minipulsar/internal/storage"
	"minipulsar/internal/tui"
)

// CLI wiring lives here so application concerns stay outside core broker logic.
func main() {
	addr := flag.String("addr", ":6650", "listen address for Pulsar binary protocol")
	dbPath := flag.String("db", "./minipulsar.db", "path to sqlite database")
	brokerURL := flag.String("broker-url", "pulsar://localhost:6650", "broker service URL advertised in LOOKUP responses")
	serverVersion := flag.String("server-version", "minipulsar-0.1", "server version reported to Pulsar clients")
	maxFrame := flag.Uint("max-frame", 10*1024*1024, "maximum inbound frame size in bytes")
	maxMessage := flag.Int("max-message", 5*1024*1024, "maximum message size advertised to clients")
	logLevel := flag.String("log-level", "info", "log level (trace, debug, info, warn, error)")
	logFormat := flag.String("log-format", "text", "log format (text or json)")
	logTimestamp := flag.Bool("log-timestamp", true, "include timestamps in log output")
	enableTUI := flag.Bool("tui", false, "enable synthwave TUI dashboard (press Q to quit)")
	flag.Parse()

	logger := logrus.New()
	level, err := logrus.ParseLevel(strings.ToLower(*logLevel))
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid log-level %q: %v\n", *logLevel, err)
		os.Exit(2)
	}
	logger.SetLevel(level)
	logger.SetOutput(os.Stdout)
	logger.SetReportCaller(false)

	switch strings.ToLower(*logFormat) {
	case "text":
		logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: *logTimestamp})
	case "json":
		logger.SetFormatter(&logrus.JSONFormatter{DisableTimestamp: !*logTimestamp})
	default:
		fmt.Fprintf(os.Stderr, "invalid log-format %q (expected text or json)\n", *logFormat)
		os.Exit(2)
	}

	store, err := storage.Open(*dbPath)
	if err != nil {
		logger.WithError(err).Fatal("open db")
	}
	if err := store.InitSchema(); err != nil {
		logger.WithError(err).Fatal("init db schema")
	}

	b := broker.New(store, broker.Config{
		Logger:           logger.WithField("component", "broker"),
		MaxFrameSize:     uint32(*maxFrame),
		MaxMessageSize:   int32(*maxMessage),
		BrokerServiceURL: *brokerURL,
		ServerVersion:    *serverVersion,
	})

	logger.WithFields(map[string]interface{}{
		"addr":        *addr,
		"db":          *dbPath,
		"broker_url":  *brokerURL,
		"max_frame":   *maxFrame,
		"max_message": *maxMessage,
		"version":     *serverVersion,
		"tui":         *enableTUI,
	}).Info("starting minipulsar")

	if *enableTUI {
		logCh := make(chan string, 500)
		logger.SetOutput(tui.NewLogWriter(logCh))
		go func() {
			if err := b.Serve(*addr); err != nil {
				logger.WithError(err).Error("listen")
			}
		}()
		if err := tui.Run(b, logCh); err != nil {
			logger.WithError(err).Fatal("tui error")
		}
		return
	}

	if err := b.Serve(*addr); err != nil {
		logger.WithError(err).Fatal("listen")
	}
}
