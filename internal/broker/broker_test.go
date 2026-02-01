package broker

import (
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"minipulsar/internal/storage"
)

func openTestStore(t *testing.T) *storage.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "broker.db")
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := store.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	t.Cleanup(func() {
		_ = store.DB().Close()
	})
	return store
}

func TestNewBrokerDefaults(t *testing.T) {
	// Pulsar brokers must advertise defaults such as max frame size and broker URL,
	// so we ensure New fills in zero-value config fields.
	store := openTestStore(t)
	b := New(store, Config{})
	if b.cfg.MaxFrameSize == 0 || b.cfg.MaxMessageSize == 0 {
		t.Fatalf("expected default sizes, got frame=%d message=%d", b.cfg.MaxFrameSize, b.cfg.MaxMessageSize)
	}
	if b.cfg.BrokerServiceURL == "" || b.cfg.ServerVersion == "" {
		t.Fatalf("expected default broker URL and version")
	}
	if b.cfg.ReadTimeout == 0 || b.cfg.WriteTimeout == 0 {
		t.Fatalf("expected default timeouts")
	}
}

func TestStatsSnapshotUsesStoreStats(t *testing.T) {
	// Pulsar brokers surface storage-backed stats alongside connection counts,
	// so we confirm snapshot includes store metrics and tracked producers/consumers.
	store := openTestStore(t)
	b := New(store, Config{})
	b.mu.Lock()
	b.producers[producerKey{id: 1}] = &producer{id: 1}
	b.consumers[consumerKey{id: 1}] = &consumer{id: 1}
	b.mu.Unlock()

	if err := store.EnsureSubscription("persistent://public/default/demo", "sub", storage.InitialPositionEarliest); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}
	if err := store.InsertMessage(&storage.Message{
		Topic:   "persistent://public/default/demo",
		Payload: []byte("payload"),
	}); err != nil {
		t.Fatalf("insert message: %v", err)
	}

	stats, err := b.StatsSnapshot(5)
	if err != nil {
		t.Fatalf("stats snapshot: %v", err)
	}
	if stats.Producers != 1 || stats.Consumers != 1 {
		t.Fatalf("unexpected connection counts: %+v", stats)
	}
	if stats.Topics != 1 || stats.Messages != 1 {
		t.Fatalf("unexpected storage counts: %+v", stats)
	}
}
