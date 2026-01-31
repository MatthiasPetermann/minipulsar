package storage

import (
	"path/filepath"
	"testing"

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
	if err := store.EnsureSubscription(topic, "sub"); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}

	var nextID int64
	if err := store.db.QueryRow(
		"SELECT next_message_id FROM subscription_cursor WHERE name=?",
		"sub",
	).Scan(&nextID); err != nil {
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

	var nextID int64
	if err := store.db.QueryRow(
		"SELECT next_message_id FROM subscription_cursor WHERE name=?",
		"sub",
	).Scan(&nextID); err != nil {
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
