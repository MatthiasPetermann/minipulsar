package storage

import (
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "minipulsar.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := store.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	return store
}

func TestEnsureSubscriptionCreatesCursor(t *testing.T) {
	store := openTestStore(t)

	topic := "persistent://public/default/test"
	if err := store.EnsureSubscription(topic, "sub", InitialPositionEarliest); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}

	nextID, err := scanSubscriptionCursor(store.db, "sub")
	if err != nil {
		t.Fatalf("query cursor: %v", err)
	}
	if nextID != 1 {
		t.Fatalf("unexpected cursor: %d", nextID)
	}
}

func TestInsertMessageRejectsNonPersistent(t *testing.T) {
	store := openTestStore(t)
	msg := Message{
		Topic:   "non-persistent://public/default/test",
		Payload: []byte("payload"),
	}
	if err := store.InsertMessage(&msg); err == nil {
		t.Fatalf("expected error for non-persistent topic")
	}
}

func TestClaimBatchAdvancesCursorAndAckClearsPending(t *testing.T) {
	store := openTestStore(t)
	topic := "persistent://public/default/test"

	for i := 0; i < 2; i++ {
		msg := Message{
			Topic:   topic,
			Payload: []byte("payload"),
		}
		if err := store.InsertMessage(&msg); err != nil {
			t.Fatalf("insert message: %v", err)
		}
	}

	batch, err := store.ClaimBatch(topic, "sub", 1, 1)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 message, got %d", len(batch))
	}

	nextID, err := scanSubscriptionCursor(store.db, "sub")
	if err != nil {
		t.Fatalf("query cursor: %v", err)
	}
	if nextID != batch[0].ID+1 {
		t.Fatalf("unexpected cursor: %d", nextID)
	}

	if err := store.AckIndividual(topic, "sub", 1, []int64{batch[0].ID}); err != nil {
		t.Fatalf("ack individual: %v", err)
	}

	var pending int64
	if err := store.db.QueryRow(
		"SELECT COUNT(*) FROM subscription_pending WHERE name=?",
		"sub",
	).Scan(&pending); err != nil {
		t.Fatalf("query pending: %v", err)
	}
	if pending != 0 {
		t.Fatalf("expected pending to be cleared, got %d", pending)
	}
}

func TestEnsureSubscriptionLatestStartsAfterNewestMessage(t *testing.T) {
	store := openTestStore(t)
	topic := "persistent://public/default/latest-test"

	for i := 0; i < 2; i++ {
		msg := Message{
			Topic:   topic,
			Payload: []byte("payload"),
		}
		if err := store.InsertMessage(&msg); err != nil {
			t.Fatalf("insert message: %v", err)
		}
	}

	if err := store.EnsureSubscription(topic, "sub", InitialPositionLatest); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}

	nextID, err := scanSubscriptionCursor(store.db, "sub")
	if err != nil {
		t.Fatalf("query cursor: %v", err)
	}
	if nextID != 3 {
		t.Fatalf("expected cursor 3, got %d", nextID)
	}
}

func TestPruneStaleSubscriptions(t *testing.T) {
	store := openTestStore(t)
	topic := "persistent://public/default/stale-test"

	if err := store.EnsureSubscription(topic, "sub", InitialPositionEarliest); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}

	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	if _, err := store.db.Exec(
		"UPDATE subscriptions SET last_consumer_at=?",
		old,
	); err != nil {
		t.Fatalf("update last_consumer_at: %v", err)
	}

	dropped, err := store.PruneStaleSubscriptions("persistent://public/default", time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("prune subscriptions: %v", err)
	}
	if len(dropped) != 1 {
		t.Fatalf("expected 1 dropped subscription, got %d", len(dropped))
	}

	var count int
	if err := store.db.QueryRow(
		"SELECT COUNT(*) FROM subscriptions",
	).Scan(&count); err != nil {
		t.Fatalf("count subscriptions: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected subscriptions to be removed, got %d", count)
	}
}

func TestPruneOrphanedMessages(t *testing.T) {
	store := openTestStore(t)
	topic := "persistent://public/default/retention-test"

	oldTime := time.Now().Add(-2 * time.Hour).UnixMilli()
	msg := Message{
		Topic:       topic,
		Payload:     []byte("payload"),
		PublishTime: oldTime,
	}
	if err := store.InsertMessage(&msg); err != nil {
		t.Fatalf("insert message: %v", err)
	}

	deleted, err := store.PruneOrphanedMessages("persistent://public/default", time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("prune orphaned messages: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 message deleted, got %d", deleted)
	}

	var count int
	if err := store.db.QueryRow(
		"SELECT COUNT(*) FROM messages",
	).Scan(&count); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected messages to be removed, got %d", count)
	}
}

func TestPruneConsumedMessages(t *testing.T) {
	store := openTestStore(t)
	topic := "persistent://public/default/consumed-retention-test"

	if err := store.EnsureSubscription(topic, "sub", InitialPositionEarliest); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}

	oldTime := time.Now().Add(-2 * time.Hour).UnixMilli()
	msg1 := Message{
		Topic:       topic,
		Payload:     []byte("payload-1"),
		PublishTime: oldTime,
	}
	if err := store.InsertMessage(&msg1); err != nil {
		t.Fatalf("insert message 1: %v", err)
	}
	msg2 := Message{
		Topic:       topic,
		Payload:     []byte("payload-2"),
		PublishTime: oldTime,
	}
	if err := store.InsertMessage(&msg2); err != nil {
		t.Fatalf("insert message 2: %v", err)
	}

	batch, err := store.ClaimBatch(topic, "sub", 1, 1)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 message claimed, got %d", len(batch))
	}
	if err := store.AckIndividual(topic, "sub", 1, []int64{batch[0].ID}); err != nil {
		t.Fatalf("ack message: %v", err)
	}

	deleted, err := store.PruneConsumedMessages("persistent://public/default", time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("prune consumed messages: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 message deleted, got %d", deleted)
	}

	var count int
	if err := store.db.QueryRow(
		"SELECT COUNT(*) FROM messages",
	).Scan(&count); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 message to remain, got %d", count)
	}
}
