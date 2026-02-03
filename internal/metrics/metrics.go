package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"minipulsar/internal/broker"
	"minipulsar/internal/logging"
	"minipulsar/internal/storage"
)

const (
	defaultPath           = "/metrics"
	defaultScrapeInterval = 5 * time.Second
	defaultTopTopics      = 10
)

// Config controls the Prometheus metrics server.
type Config struct {
	Logger         *logging.Logger
	ListenAddr     string
	Path           string
	ScrapeInterval time.Duration
	TopTopicsLimit int
}

// Server manages a Prometheus endpoint with broker metrics.
type Server struct {
	broker *broker.Broker
	cfg    Config
	server *http.Server
	stopCh chan struct{}
	wg     sync.WaitGroup
	logger *logging.Logger

	mu       sync.RWMutex
	snapshot metricsSnapshot
}

type metricsSnapshot struct {
	Producers               int
	Consumers               int
	Namespaces              int
	Topics                  int
	Subscriptions           int
	Pending                 int
	Messages                int
	TopTopics               []storage.TopicStat
	TopSubscriptionsBacklog []storage.SubscriptionBacklogStat

	ScrapeErrors uint64
	ScrapeTime   time.Time
	ScrapeCost   time.Duration
}

// NewServer constructs a metrics server for the broker.
func NewServer(b *broker.Broker, cfg Config) (*Server, error) {
	if cfg.Path == "" {
		cfg.Path = defaultPath
	}
	if cfg.ScrapeInterval <= 0 {
		cfg.ScrapeInterval = defaultScrapeInterval
	}
	if cfg.TopTopicsLimit <= 0 {
		cfg.TopTopicsLimit = defaultTopTopics
	}
	logger := cfg.Logger
	if logger == nil {
		defaultLogger, err := logging.New(logging.Options{
			Format:        "text",
			WithTimestamp: true,
			Level:         slog.LevelInfo,
			Writer:        os.Stdout,
		})
		if err == nil {
			logger = defaultLogger
		}
	}

	mux := http.NewServeMux()
	server := &Server{
		broker: b,
		cfg:    cfg,
		server: &http.Server{
			Addr:              cfg.ListenAddr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		stopCh: make(chan struct{}),
		logger: logger,
	}

	mux.HandleFunc(cfg.Path, server.handleMetrics)

	return server, nil
}

// Start begins serving metrics and collecting broker stats.
func (s *Server) Start() {
	s.wg.Add(1)
	go s.collectLoop()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Warn("metrics server stopped", "err", err)
		}
	}()
}

// Stop shuts down the metrics server.
func (s *Server) Stop() {
	close(s.stopCh)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.server.Shutdown(shutdownCtx); err != nil {
		s.logger.Warn("metrics shutdown failed", "err", err)
	}
	s.wg.Wait()
}

func (s *Server) collectLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.ScrapeInterval)
	defer ticker.Stop()

	s.collectOnce()
	for {
		select {
		case <-ticker.C:
			s.collectOnce()
		case <-s.stopCh:
			return
		}
	}
}

func (s *Server) collectOnce() {
	start := time.Now()
	stats, err := s.broker.StatsSnapshot(s.cfg.TopTopicsLimit)
	if err != nil {
		s.mu.Lock()
		s.snapshot.ScrapeErrors++
		s.mu.Unlock()
		s.logger.Warn("metrics scrape failed", "err", err)
		return
	}

	top := make([]storage.TopicStat, len(stats.TopTopics))
	copy(top, stats.TopTopics)
	subBacklog := make([]storage.SubscriptionBacklogStat, len(stats.TopSubscriptionsBacklog))
	copy(subBacklog, stats.TopSubscriptionsBacklog)

	s.mu.Lock()
	s.snapshot = metricsSnapshot{
		Producers:               stats.Producers,
		Consumers:               stats.Consumers,
		Namespaces:              stats.Namespaces,
		Topics:                  stats.Topics,
		Subscriptions:           stats.Subscriptions,
		Pending:                 stats.Pending,
		Messages:                stats.Messages,
		TopTopics:               top,
		TopSubscriptionsBacklog: subBacklog,
		ScrapeErrors:            s.snapshot.ScrapeErrors,
		ScrapeTime:              time.Now(),
		ScrapeCost:              time.Since(start),
	}
	s.mu.Unlock()
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	s.mu.RLock()
	snap := s.snapshot
	s.mu.RUnlock()

	writeGauge(w, "minipulsar_broker_producers", "Number of connected producers.", snap.Producers)
	writeGauge(w, "minipulsar_broker_consumers", "Number of connected consumers.", snap.Consumers)
	writeGauge(w, "minipulsar_storage_namespaces", "Number of namespaces known to storage.", snap.Namespaces)
	writeGauge(w, "minipulsar_storage_topics", "Number of topics known to storage.", snap.Topics)
	writeGauge(w, "minipulsar_storage_subscriptions", "Number of subscriptions known to storage.", snap.Subscriptions)
	writeGauge(w, "minipulsar_storage_pending_messages", "Pending (delivered, unacked) messages across subscriptions.", snap.Pending)
	writeGauge(w, "minipulsar_storage_stored_messages", "Stored messages across topics.", snap.Messages)

	writeGaugeHeader(w, "minipulsar_storage_topic_messages", "Stored messages per topic (top topics only).")
	for _, topic := range snap.TopTopics {
		writeGaugeWithLabels(w, "minipulsar_storage_topic_messages", float64(topic.MessageCount), map[string]string{
			"topic": topic.Topic,
		})
	}

	writeGaugeHeader(w, "minipulsar_storage_topic_pending_messages", "Pending (delivered, unacked) messages per topic (top topics only).")
	for _, topic := range snap.TopTopics {
		writeGaugeWithLabels(w, "minipulsar_storage_topic_pending_messages", float64(topic.PendingCount), map[string]string{
			"topic": topic.Topic,
		})
	}

	writeGaugeHeader(w, "minipulsar_storage_topic_backlog_messages", "Retention-delayed backlog messages per topic (top topics only).")
	for _, topic := range snap.TopTopics {
		writeGaugeWithLabels(w, "minipulsar_storage_topic_backlog_messages", float64(topic.BacklogCount), map[string]string{
			"topic": topic.Topic,
		})
	}

	writeGaugeHeader(w, "minipulsar_storage_subscription_backlog_messages", "Retention-delayed backlog messages per subscription (top subscriptions only).")
	for _, sub := range snap.TopSubscriptionsBacklog {
		writeGaugeWithLabels(w, "minipulsar_storage_subscription_backlog_messages", float64(sub.BacklogCount), map[string]string{
			"topic":        sub.Topic,
			"subscription": sub.Subscription,
		})
	}

	writeCounter(w, "minipulsar_metrics_scrape_errors_total", "Total number of errors while collecting metrics.", snap.ScrapeErrors)
	if !snap.ScrapeTime.IsZero() {
		writeGaugeFloat(w, "minipulsar_metrics_last_scrape_timestamp_seconds", "Unix timestamp of the last successful metrics collection.", float64(snap.ScrapeTime.Unix()))
		writeGaugeFloat(w, "minipulsar_metrics_scrape_duration_seconds", "Latency for collecting broker metrics.", snap.ScrapeCost.Seconds())
	}
}

func writeGauge(w http.ResponseWriter, name, help string, value int) {
	writeGaugeHeader(w, name, help)
	fmt.Fprintf(w, "%s %d\n", name, value)
}

func writeGaugeFloat(w http.ResponseWriter, name, help string, value float64) {
	writeGaugeHeader(w, name, help)
	fmt.Fprintf(w, "%s %f\n", name, value)
}

func writeCounter(w http.ResponseWriter, name, help string, value uint64) {
	writeCounterHeader(w, name, help)
	fmt.Fprintf(w, "%s %d\n", name, value)
}

func writeGaugeHeader(w http.ResponseWriter, name, help string) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s gauge\n", name)
}

func writeCounterHeader(w http.ResponseWriter, name, help string) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s counter\n", name)
}

func writeGaugeWithLabels(w http.ResponseWriter, name string, value float64, labels map[string]string) {
	fmt.Fprintf(w, "%s{%s} %f\n", name, renderLabels(labels), value)
}

func renderLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(labels))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=\"%s\"", key, escapeLabel(labels[key])))
	}
	return strings.Join(parts, ",")
}

func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\\\n")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	return value
}
