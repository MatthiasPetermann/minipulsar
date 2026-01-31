package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"minipulsar/internal/broker"
	"minipulsar/internal/messaging"
	"minipulsar/internal/metrics"
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
	metricsAddr := flag.String("metrics-addr", "127.0.0.1:8080", "listen address for Prometheus metrics endpoint (empty to disable)")
	metricsPath := flag.String("metrics-path", "/metrics", "HTTP path for Prometheus metrics endpoint")
	metricsInterval := flag.Duration("metrics-interval", 5*time.Second, "interval between metrics collection")
	metricsTopTopics := flag.Int("metrics-top-topics", 10, "number of top topics to export metrics for")
	jwtSecret := flag.String("jwt-secret", os.Getenv("MINIPULSAR_JWT_SECRET"), "shared secret for HS256 JWT verification (or set MINIPULSAR_JWT_SECRET)")
	tlsCert := flag.String("tls-cert", "", "path to TLS certificate PEM (enables TLS)")
	tlsKey := flag.String("tls-key", "", "path to TLS private key PEM (enables TLS)")
	tlsClientCA := flag.String("tls-client-ca", "", "optional path to client CA bundle PEM for mTLS")
	flag.Parse()

	level, err := parseLogLevel(*logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid log-level %q: %v\n", *logLevel, err)
		os.Exit(2)
	}
	logger, err := newLogger(*logFormat, *logTimestamp, level, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	store, err := storage.Open(*dbPath)
	if err != nil {
		logger.Error("open db", "err", err)
		os.Exit(1)
	}
	if err := store.InitSchema(); err != nil {
		logger.Error("init db schema", "err", err)
		os.Exit(1)
	}

	var messagingRuntime *messaging.Runtime
	if *messagingConfig != "" {
		cfg, err := messaging.LoadConfig(*messagingConfig)
		if err != nil {
			logger.Error("load messaging config", "err", err)
			os.Exit(1)
		}
		runtime, err := messaging.BuildRuntime(cfg, messaging.Options{
			Logger:        logger.With("component", "messaging"),
			WorkerCount:   *functionWorkers,
			ValidateFuncs: true,
		})
		if err != nil {
			logger.Error("init messaging runtime", "err", err)
			os.Exit(1)
		}
		messagingRuntime = runtime
	}

	tlsConfig, err := buildTLSConfig(*tlsCert, *tlsKey, *tlsClientCA)
	if err != nil {
		logger.Error("configure tls", "err", err)
		os.Exit(1)
	}

	b := broker.New(store, broker.Config{
		Logger:           logger.With("component", "broker"),
		MaxFrameSize:     uint32(*maxFrame),
		MaxMessageSize:   int32(*maxMessage),
		BrokerServiceURL: *brokerURL,
		ServerVersion:    *serverVersion,
		Messaging:        messagingRuntime,
		JWTSecret:        []byte(strings.TrimSpace(*jwtSecret)),
		TLSConfig:        tlsConfig,
	})

	if *metricsAddr != "" {
		metricsServer, err := metrics.NewServer(b, metrics.Config{
			Logger:         logger.With("component", "metrics"),
			ListenAddr:     *metricsAddr,
			Path:           *metricsPath,
			ScrapeInterval: *metricsInterval,
			TopTopicsLimit: *metricsTopTopics,
		})
		if err != nil {
			logger.Error("init metrics", "err", err)
			os.Exit(1)
		}
		metricsServer.Start()
		logger.Info("metrics endpoint started",
			"metrics_addr", *metricsAddr,
			"metrics_path", *metricsPath,
			"metrics_interval", metricsInterval.String(),
			"metrics_top", *metricsTopTopics,
		)
	}

	logger.Info("starting minipulsar",
		"addr", *addr,
		"db", *dbPath,
		"broker_url", *brokerURL,
		"max_frame", *maxFrame,
		"max_message", *maxMessage,
		"version", *serverVersion,
		"messaging", *messagingConfig,
		"tls_enabled", tlsConfig != nil,
	)

	if *enableTUI {
		logCh := make(chan string, 500)
		tuiLogger, err := newLogger(*logFormat, *logTimestamp, level, tui.NewLogWriter(func(line string) {
			select {
			case logCh <- line:
			default:
			}
		}))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}

		program := tui.NewProgram(b, logCh)
		go func() {
			if err := b.Serve(*addr); err != nil {
				tuiLogger.Error("listen", "err", err)
				os.Exit(1)
			}
		}()
		if err := program.Start(); err != nil {
			tuiLogger.Error("tui", "err", err)
			os.Exit(1)
		}
		return
	}

	if err := b.Serve(*addr); err != nil {
		logger.Error("listen", "err", err)
		os.Exit(1)
	}
}

func parseLogLevel(raw string) (slog.Level, error) {
	switch strings.ToLower(raw) {
	case "trace":
		return slog.LevelDebug - 4, nil
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("unknown level: %s", raw)
	}
}

func newLogger(format string, withTimestamp bool, level slog.Level, out io.Writer) (*slog.Logger, error) {
	opts := &slog.HandlerOptions{
		Level: level,
	}
	if !withTimestamp {
		opts.ReplaceAttr = func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		}
	}
	switch strings.ToLower(format) {
	case "text":
		return slog.New(slog.NewTextHandler(out, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(out, opts)), nil
	default:
		return nil, fmt.Errorf("invalid log-format %q (expected text or json)", format)
	}
}

func buildTLSConfig(certPath, keyPath, clientCAPath string) (*tls.Config, error) {
	if certPath == "" && keyPath == "" && clientCAPath == "" {
		return nil, nil
	}
	if certPath == "" || keyPath == "" {
		return nil, fmt.Errorf("tls requires both -tls-cert and -tls-key")
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load tls cert/key: %w", err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if clientCAPath != "" {
		caPEM, err := os.ReadFile(clientCAPath)
		if err != nil {
			return nil, fmt.Errorf("read client ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("invalid client ca pem")
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}
