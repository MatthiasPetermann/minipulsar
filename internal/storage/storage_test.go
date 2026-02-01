package storage

import (
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "store.db")
	store, err := Open(dbPath)
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

func TestEnsureSubscriptionInitialPositionLatest(t *testing.T) {
	// Pulsar subscriptions created at "latest" should start after the last stored entry,
	// so the cursor is set to max message id + 1 to avoid replaying history.
	store := openTestStore(t)
	msg := &Message{Topic: "persistent://public/default/metrics", Payload: []byte("a")}
	if err := store.InsertMessage(msg); err != nil {
		t.Fatalf("insert message: %v", err)
	}
	if err := store.EnsureSubscription(msg.Topic, "sub-a", InitialPositionLatest, SubscriptionTypeShared); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}

	var nextID int64
	if err := store.db.QueryRow(
		`SELECT next_message_id FROM subscription_cursor WHERE name=?`,
		"sub-a",
	).Scan(&nextID); err != nil {
		t.Fatalf("read cursor: %v", err)
	}
	if nextID != msg.ID+1 {
		t.Fatalf("unexpected cursor: got %d want %d", nextID, msg.ID+1)
	}
}

func TestEnsureSubscriptionInitialPositionEarliest(t *testing.T) {
	// Pulsar subscriptions created at "earliest" should start from the first entry id,
	// so the cursor is initialized to 1 even if the topic is currently empty.
	store := openTestStore(t)
	if err := store.EnsureSubscription("persistent://public/default/audit", "sub-b", InitialPositionEarliest, SubscriptionTypeShared); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}

	var nextID int64
	if err := store.db.QueryRow(
		`SELECT next_message_id FROM subscription_cursor WHERE name=?`,
		"sub-b",
	).Scan(&nextID); err != nil {
		t.Fatalf("read cursor: %v", err)
	}
	if nextID != 1 {
		t.Fatalf("unexpected cursor: got %d want 1", nextID)
	}
}

func TestInsertMessageRejectsNonPersistent(t *testing.T) {
	// Pulsar non-persistent topics should not be written to durable storage,
	// so the store must reject inserts for non-persistent:// topics.
	store := openTestStore(t)
	err := store.InsertMessage(&Message{
		Topic:   "non-persistent://public/default/ephemeral",
		Payload: []byte("nope"),
	})
	if err == nil {
		t.Fatalf("expected error for non-persistent topic")
	}
}

func TestClaimBatchAndAckIndividual(t *testing.T) {
	// Pulsar shared subscriptions claim messages into a pending set and clear them on ACK,
	// so we verify the batch claim advances the cursor and individual ACK removes pending rows.
	store := openTestStore(t)
	topicName := "persistent://public/default/orders"

	if err := store.EnsureSubscription(topicName, "sub", InitialPositionEarliest, SubscriptionTypeShared); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := store.InsertMessage(&Message{
			Topic:      topicName,
			Payload:    []byte{byte('a' + i)},
			SequenceID: uint64(i + 1),
		}); err != nil {
			t.Fatalf("insert message %d: %v", i, err)
		}
	}

	batch, err := store.ClaimBatch(topicName, "sub", 100, 2)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("unexpected batch length: %d", len(batch))
	}

	var pendingCount int
	if err := store.db.QueryRow(
		`SELECT COUNT(*) FROM subscription_pending`,
	).Scan(&pendingCount); err != nil {
		t.Fatalf("read pending count: %v", err)
	}
	if pendingCount != 2 {
		t.Fatalf("unexpected pending count: %d", pendingCount)
	}

	if err := store.AckIndividual(topicName, "sub", 100, []int64{batch[0].ID}); err != nil {
		t.Fatalf("ack individual: %v", err)
	}
	if err := store.db.QueryRow(
		`SELECT COUNT(*) FROM subscription_pending`,
	).Scan(&pendingCount); err != nil {
		t.Fatalf("read pending count: %v", err)
	}
	if pendingCount != 1 {
		t.Fatalf("unexpected pending count after ack: %d", pendingCount)
	}
}

func TestDropPendingByConsumer(t *testing.T) {
	// Pulsar brokers clear pending entries when a consumer disconnects,
	// so we ensure the storage layer removes the backlog for a specific consumer.
	store := openTestStore(t)
	topicName := "persistent://public/default/metrics"
	if err := store.EnsureSubscription(topicName, "sub", InitialPositionEarliest, SubscriptionTypeShared); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}
	if err := store.InsertMessage(&Message{Topic: topicName, Payload: []byte("a")}); err != nil {
		t.Fatalf("insert message: %v", err)
	}
	if _, err := store.ClaimBatch(topicName, "sub", 55, 1); err != nil {
		t.Fatalf("claim batch: %v", err)
	}
	if err := store.DropPendingByConsumer(topicName, "sub", 55); err != nil {
		t.Fatalf("drop pending: %v", err)
	}

	var pendingCount int
	if err := store.db.QueryRow(
		`SELECT COUNT(*) FROM subscription_pending`,
	).Scan(&pendingCount); err != nil {
		t.Fatalf("read pending count: %v", err)
	}
	if pendingCount != 0 {
		t.Fatalf("unexpected pending count: %d", pendingCount)
	}
}

func TestHasSubscriptions(t *testing.T) {
	// Pulsar topics may exist without subscriptions, so HasSubscriptions must reflect reality.
	store := openTestStore(t)
	topicName := "persistent://public/default/telemetry"

	has, err := store.HasSubscriptions(topicName)
	if err != nil {
		t.Fatalf("has subscriptions: %v", err)
	}
	if has {
		t.Fatalf("unexpected subscriptions before creation")
	}

	if err := store.EnsureSubscription(topicName, "sub", InitialPositionEarliest, SubscriptionTypeShared); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}
	has, err = store.HasSubscriptions(topicName)
	if err != nil {
		t.Fatalf("has subscriptions: %v", err)
	}
	if !has {
		t.Fatalf("expected subscriptions to exist")
	}
}

func TestStatsSnapshot(t *testing.T) {
	// Pulsar brokers expose counts of namespaces, topics, subscriptions, and pending messages,
	// so the storage stats snapshot should aggregate those values correctly.
	store := openTestStore(t)
	topicName := "persistent://public/default/stats"
	if err := store.EnsureSubscription(topicName, "sub", InitialPositionEarliest, SubscriptionTypeShared); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}
	if err := store.InsertMessage(&Message{Topic: topicName, Payload: []byte("a")}); err != nil {
		t.Fatalf("insert message: %v", err)
	}
	if _, err := store.ClaimBatch(topicName, "sub", 999, 1); err != nil {
		t.Fatalf("claim batch: %v", err)
	}

	snap, err := store.StatsSnapshot(5)
	if err != nil {
		t.Fatalf("stats snapshot: %v", err)
	}
	if snap.Namespaces != 1 || snap.Topics != 1 || snap.Subscriptions != 1 {
		t.Fatalf("unexpected namespace/topic/subscription counts: %+v", snap)
	}
	if snap.Messages != 1 || snap.Pending != 1 {
		t.Fatalf("unexpected message/pending counts: %+v", snap)
	}
	if len(snap.TopTopics) != 1 {
		t.Fatalf("expected one top topic, got %d", len(snap.TopTopics))
	}
}
