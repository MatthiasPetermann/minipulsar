package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"

	_ "github.com/mattn/go-sqlite3"

	"minipulsar/internal/broker"
	"minipulsar/internal/messaging"
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
	enableTUI := flag.Bool("tui", false, "enable synthwave TUI dashboard")
	messagingConfig := flag.String("messaging-config", "", "path to messaging control-plane HCL config")
	functionWorkers := flag.Int("function-workers", 4, "number of Lua function workers")
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

	var formatter logrus.Formatter
	switch strings.ToLower(*logFormat) {
	case "text":
		formatter = &logrus.TextFormatter{FullTimestamp: *logTimestamp}
	case "json":
		formatter = &logrus.JSONFormatter{DisableTimestamp: !*logTimestamp}
	default:
		fmt.Fprintf(os.Stderr, "invalid log-format %q (expected text or json)\n", *logFormat)
		os.Exit(2)
	}
	logger.SetFormatter(formatter)

	store, err := storage.Open(*dbPath)
	if err != nil {
		logger.WithError(err).Fatal("open db")
	}
	if err := store.InitSchema(); err != nil {
		logger.WithError(err).Fatal("init db schema")
	}

	var messagingRuntime *messaging.Runtime
	if *messagingConfig != "" {
		cfg, err := messaging.LoadConfig(*messagingConfig)
		if err != nil {
			logger.WithError(err).Fatal("load messaging config")
		}
		runtime, err := messaging.BuildRuntime(cfg, messaging.Options{
			Logger:        logger.WithField("component", "messaging"),
			WorkerCount:   *functionWorkers,
			ValidateFuncs: true,
		})
		if err != nil {
			logger.WithError(err).Fatal("init messaging runtime")
		}
		messagingRuntime = runtime
	}

	b := broker.New(store, broker.Config{
		Logger:           logger.WithField("component", "broker"),
		MaxFrameSize:     uint32(*maxFrame),
		MaxMessageSize:   int32(*maxMessage),
		BrokerServiceURL: *brokerURL,
		ServerVersion:    *serverVersion,
		Messaging:        messagingRuntime,
	})

	logger.WithFields(map[string]interface{}{
		"addr":        *addr,
		"db":          *dbPath,
		"broker_url":  *brokerURL,
		"max_frame":   *maxFrame,
		"max_message": *maxMessage,
		"version":     *serverVersion,
		"messaging":   *messagingConfig,
	}).Info("starting minipulsar")

	if *enableTUI {
		logCh := make(chan string, 500)
		logger.SetOutput(tui.NewLogWriter(func(line string) {
			select {
			case logCh <- line:
			default:
			}
		}))

		program := tui.NewProgram(b, logCh)
		go func() {
			if err := b.Serve(*addr); err != nil {
				logger.WithError(err).Fatal("listen")
			}
		}()
		if err := program.Start(); err != nil {
			logger.WithError(err).Fatal("tui")
		}
		return
	}

	if err := b.Serve(*addr); err != nil {
		logger.WithError(err).Fatal("listen")
	}
}
