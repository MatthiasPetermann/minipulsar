package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"minipulsar/internal/broker"
	"minipulsar/internal/logging"
	"minipulsar/internal/messaging"
	"minipulsar/internal/metrics"
	"minipulsar/internal/storage"
	"minipulsar/internal/tui"
)

// CLI wiring lives here so application concerns stay outside core broker logic.
func main() {
	addr := flag.String("addr", ":6650", "listen address for Pulsar binary protocol (empty to disable)")
	tlsAddr := flag.String("tls-addr", ":6651", "listen address for Pulsar TLS binary protocol (empty to disable)")
	dbPath := flag.String("db", "./minipulsar.db", "path to sqlite database")
	brokerURL := flag.String("broker-url", "pulsar://localhost:6650", "broker service URL advertised in LOOKUP responses")
	brokerURLTLS := flag.String("broker-url-tls", "pulsar+ssl://localhost:6651", "broker TLS service URL advertised in LOOKUP responses (empty to disable)")
	serverVersion := flag.String("server-version", "minipulsar-0.1", "server version reported to Pulsar clients")
	maxFrame := flag.Uint("max-frame", 10*1024*1024, "maximum inbound frame size in bytes")
	maxMessage := flag.Int("max-message", 5*1024*1024, "maximum message size advertised to clients")
	maxConnections := flag.Int("max-connections", 0, "maximum concurrent TCP connections (0 disables the limit)")
	maxProducers := flag.Int("max-producers", 0, "maximum concurrent producers (0 disables the limit)")
	maxConsumers := flag.Int("max-consumers", 0, "maximum concurrent consumers (0 disables the limit)")
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
	namespaceMaintenanceInterval := flag.Duration("namespace-maintenance-interval", 30*time.Second, "interval between namespace maintenance sweeps")
	ackTimeout := flag.Duration("ack-timeout", 0, "ack timeout for redelivery of unacked messages (0 to disable)")
	ackTimeoutCheckInterval := flag.Duration("ack-timeout-check-interval", 30*time.Second, "interval between ack timeout scans")
	jwtSecret := flag.String("jwt-secret", os.Getenv("MINIPULSAR_JWT_SECRET"), "shared secret for HS256 JWT verification (or set MINIPULSAR_JWT_SECRET)")
	readTimeout := flag.Duration("read-timeout", 15*time.Second, "maximum time allowed to read a frame from a client (0 to disable)")
	writeTimeout := flag.Duration("write-timeout", 15*time.Second, "maximum time allowed to write a frame to a client (0 to disable)")
	tlsCert := flag.String("tls-cert", "", "path to TLS certificate PEM (enables TLS)")
	tlsKey := flag.String("tls-key", "", "path to TLS private key PEM (enables TLS)")
	flag.Parse()

	level, err := parseLogLevel(*logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid log-level %q: %v\n", *logLevel, err)
		os.Exit(2)
	}
	logger, err := logging.New(logging.Options{
		Format:        *logFormat,
		WithTimestamp: *logTimestamp,
		Level:         level,
		Writer:        os.Stdout,
	})
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
	defer store.DB().Close()

	var messagingCfg *messaging.Config
	if *messagingConfig != "" {
		cfg, err := messaging.LoadConfig(*messagingConfig)
		if err != nil {
			logger.Error("load messaging config", "err", err)
			os.Exit(1)
		}
		messagingCfg = cfg
	}

	tlsConfig, err := buildTLSConfig(*tlsCert, *tlsKey)
	if err != nil {
		logger.Error("configure tls", "err", err)
		os.Exit(1)
	}
	if tlsConfig == nil || *tlsAddr == "" {
		*brokerURLTLS = ""
	}

	if *enableTUI {
		logCh := make(chan string, 500)
		logLevelVar := &slog.LevelVar{}
		logLevelVar.Set(level)
		tuiLogger, err := logging.New(logging.Options{
			Format:        *logFormat,
			WithTimestamp: *logTimestamp,
			LevelVar:      logLevelVar,
			TimeFormat:    "15:04:05.000",
			Writer: tui.NewLogWriter(func(line string) {
				select {
				case logCh <- line:
				default:
				}
			}),
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		logger = tuiLogger

		messagingRuntime, err := buildMessagingRuntime(messagingCfg, logger, *functionWorkers)
		if err != nil {
			logger.Error("init messaging runtime", "err", err)
			os.Exit(1)
		}

		b := broker.New(store, broker.Config{
			Logger:                       logger.With("component", "broker"),
			MaxFrameSize:                 uint32(*maxFrame),
			MaxMessageSize:               int32(*maxMessage),
			BrokerServiceURL:             *brokerURL,
			BrokerServiceURLTLS:          *brokerURLTLS,
			ServerVersion:                *serverVersion,
			Messaging:                    messagingRuntime,
			JWTSecret:                    []byte(strings.TrimSpace(*jwtSecret)),
			TLSConfig:                    tlsConfig,
			ReadTimeout:                  *readTimeout,
			WriteTimeout:                 *writeTimeout,
			NamespaceMaintenanceInterval: *namespaceMaintenanceInterval,
			AckTimeout:                   *ackTimeout,
			AckTimeoutCheckInterval:      *ackTimeoutCheckInterval,
			MaxConnections:               *maxConnections,
			MaxProducers:                 *maxProducers,
			MaxConsumers:                 *maxConsumers,
		})
		metricsServer, err := startMetricsServer(b, logger, metrics.Config{
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
		defer stopRuntime(b, metricsServer, logger)

		logger.Info("starting minipulsar",
			"addr", *addr,
			"tls_addr", *tlsAddr,
			"db", *dbPath,
			"broker_url", *brokerURL,
			"broker_url_tls", *brokerURLTLS,
			"max_frame", *maxFrame,
			"max_message", *maxMessage,
			"version", *serverVersion,
			"messaging", *messagingConfig,
			"tls_enabled", tlsConfig != nil,
			"read_timeout", readTimeout.String(),
			"write_timeout", writeTimeout.String(),
			"namespace_maintenance_interval", namespaceMaintenanceInterval.String(),
			"ack_timeout", ackTimeout.String(),
			"ack_timeout_check_interval", ackTimeoutCheckInterval.String(),
		)

		program := tui.NewProgram(b, logCh, logLevelVar, level)
		errCh, err := startBrokerListeners(b, logger, *addr, *tlsAddr, tlsConfig)
		if err != nil {
			tuiLogger.Error("listen", "err", err)
			os.Exit(1)
		}
		go func() {
			if err := <-errCh; err != nil {
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

	messagingRuntime, err := buildMessagingRuntime(messagingCfg, logger, *functionWorkers)
	if err != nil {
		logger.Error("init messaging runtime", "err", err)
		os.Exit(1)
	}

	b := broker.New(store, broker.Config{
		Logger:                       logger.With("component", "broker"),
		MaxFrameSize:                 uint32(*maxFrame),
		MaxMessageSize:               int32(*maxMessage),
		BrokerServiceURL:             *brokerURL,
		BrokerServiceURLTLS:          *brokerURLTLS,
		ServerVersion:                *serverVersion,
		Messaging:                    messagingRuntime,
		JWTSecret:                    []byte(strings.TrimSpace(*jwtSecret)),
		TLSConfig:                    tlsConfig,
		ReadTimeout:                  *readTimeout,
		WriteTimeout:                 *writeTimeout,
		NamespaceMaintenanceInterval: *namespaceMaintenanceInterval,
		AckTimeout:                   *ackTimeout,
		AckTimeoutCheckInterval:      *ackTimeoutCheckInterval,
		MaxConnections:               *maxConnections,
		MaxProducers:                 *maxProducers,
		MaxConsumers:                 *maxConsumers,
	})
	metricsServer, err := startMetricsServer(b, logger, metrics.Config{
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
	defer stopRuntime(b, metricsServer, logger)

	logger.Info("starting minipulsar",
		"addr", *addr,
		"tls_addr", *tlsAddr,
		"db", *dbPath,
		"broker_url", *brokerURL,
		"broker_url_tls", *brokerURLTLS,
		"max_frame", *maxFrame,
		"max_message", *maxMessage,
		"version", *serverVersion,
		"messaging", *messagingConfig,
		"tls_enabled", tlsConfig != nil,
		"read_timeout", readTimeout.String(),
		"write_timeout", writeTimeout.String(),
		"namespace_maintenance_interval", namespaceMaintenanceInterval.String(),
		"ack_timeout", ackTimeout.String(),
		"ack_timeout_check_interval", ackTimeoutCheckInterval.String(),
	)

	errCh, err := startBrokerListeners(b, logger, *addr, *tlsAddr, tlsConfig)
	if err != nil {
		logger.Error("listen", "err", err)
		os.Exit(1)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	select {
	case err := <-errCh:
		if err != nil {
			logger.Error("listen", "err", err)
			os.Exit(1)
		}
	case sig := <-signals:
		logger.Info("shutdown requested", "signal", sig)
	}
}

// buildMessagingRuntime wires the messaging control plane runtime and pool.
func buildMessagingRuntime(cfg *messaging.Config, logger *logging.Logger, workers int) (*messaging.Runtime, error) {
	if cfg == nil {
		return nil, nil
	}
	return messaging.BuildRuntime(cfg, messaging.Options{
		Logger:        logger.With("component", "messaging"),
		WorkerCount:   workers,
		ValidateFuncs: true,
	})
}

// startMetricsServer starts the Prometheus exporter if configured.
func startMetricsServer(b *broker.Broker, logger *logging.Logger, cfg metrics.Config) (*metrics.Server, error) {
	if cfg.ListenAddr == "" {
		return nil, nil
	}
	metricsServer, err := metrics.NewServer(b, cfg)
	if err != nil {
		return nil, err
	}
	metricsServer.Start()
	logger.Info("metrics endpoint started",
		"metrics_addr", cfg.ListenAddr,
		"metrics_path", cfg.Path,
		"metrics_interval", cfg.ScrapeInterval.String(),
		"metrics_top", cfg.TopTopicsLimit,
	)
	return metricsServer, nil
}

func stopRuntime(b *broker.Broker, metricsServer *metrics.Server, logger *logging.Logger) {
	if metricsServer != nil {
		metricsServer.Stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.Shutdown(ctx); err != nil {
		logger.Warn("broker shutdown failed", "err", err)
	}
}

// startBrokerListeners starts TCP/TLS listeners and aggregates errors.
func startBrokerListeners(b *broker.Broker, logger *logging.Logger, addr string, tlsAddr string, tlsConfig *tls.Config) (<-chan error, error) {
	errCh := make(chan error, 2)
	started := 0
	if addr != "" {
		started++
		go func() {
			errCh <- b.ServeWithTLS(addr, nil)
		}()
	}
	if tlsConfig != nil && tlsAddr != "" {
		started++
		go func() {
			errCh <- b.ServeWithTLS(tlsAddr, tlsConfig)
		}()
	}
	if started == 0 {
		return nil, fmt.Errorf("no broker listeners configured")
	}
	logger.Info("broker listeners started",
		"addr", addr,
		"tls_addr", tlsAddr,
		"tls_enabled", tlsConfig != nil,
	)
	return errCh, nil
}

// parseLogLevel normalizes CLI strings into slog levels.
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

// buildTLSConfig loads TLS certificates when both paths are provided.
func buildTLSConfig(certPath, keyPath string) (*tls.Config, error) {
	if certPath == "" && keyPath == "" {
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
	return cfg, nil
}
