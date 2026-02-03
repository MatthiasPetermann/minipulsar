package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"minipulsar/internal/storage"
)

func TestRenderLabelsEscapesAndSorts(t *testing.T) {
	// Prometheus label formatting must be deterministic and escaped,
	// so we verify labels are sorted and special characters are escaped.
	labels := map[string]string{
		"topic": "persistent://public/default/a\nb",
		"role":  `writer"1`,
	}
	rendered := renderLabels(labels)
	if !strings.Contains(rendered, `role="writer\"1"`) {
		t.Fatalf("expected escaped quote in labels: %s", rendered)
	}
	if !strings.Contains(rendered, `topic="persistent://public/default/a\\nb"`) {
		t.Fatalf("expected escaped newline in labels: %s", rendered)
	}
	if !strings.HasPrefix(rendered, `role=`) {
		t.Fatalf("expected sorted labels by key, got: %s", rendered)
	}
}

func TestHandleMetricsWritesSnapshot(t *testing.T) {
	// Pulsar exposes broker and storage metrics in Prometheus format,
	// so we verify the handler renders gauges and counters from the snapshot.
	server := &Server{
		snapshot: metricsSnapshot{
			Producers:     2,
			Consumers:     3,
			Namespaces:    1,
			Topics:        1,
			Subscriptions: 4,
			Pending:       5,
			Messages:      6,
			TopTopics: []storage.TopicStat{
				{Topic: "persistent://public/default/demo", MessageCount: 6, PendingCount: 5},
			},
			TopSubscriptionsBacklog: []storage.SubscriptionBacklogStat{
				{Topic: "persistent://public/default/demo", Subscription: "sub", BacklogCount: 2},
			},
			ScrapeErrors: 1,
			ScrapeTime:   time.Unix(1700000000, 0),
			ScrapeCost:   125 * time.Millisecond,
		},
	}
	recorder := httptest.NewRecorder()
	server.handleMetrics(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	if !strings.Contains(body, "minipulsar_broker_producers 2") {
		t.Fatalf("missing producers gauge: %s", body)
	}
	if !strings.Contains(body, `minipulsar_storage_topic_messages{topic="persistent://public/default/demo"} 6.000000`) {
		t.Fatalf("missing topic gauge: %s", body)
	}
	if !strings.Contains(body, `minipulsar_storage_subscription_backlog_messages{subscription="sub",topic="persistent://public/default/demo"} 2.000000`) {
		t.Fatalf("missing subscription backlog gauge: %s", body)
	}
	if !strings.Contains(body, "minipulsar_metrics_scrape_errors_total 1") {
		t.Fatalf("missing scrape errors counter: %s", body)
	}
}
