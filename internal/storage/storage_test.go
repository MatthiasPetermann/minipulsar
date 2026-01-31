package storage

import (
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestClaimBatchNoDuplicateAcrossConsumers(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "minipulsar.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := store.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	topic := "persistent://public/default/test"
	sub := "sub"

	if err := store.EnsureSubscription(topic, sub, InitialPositionEarliest); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}

	for i := 0; i < 3; i++ {
		msg := Message{
			Topic:   topic,
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		}
		if err := store.InsertMessage(&msg); err != nil {
			t.Fatalf("insert message: %v", err)
		}
	}

	seen := make(map[int64]struct{})
	firstBatch, err := store.ClaimBatch(topic, sub, 1, 2)
	if err != nil {
		t.Fatalf("claim batch 1: %v", err)
	}
	if len(firstBatch) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(firstBatch))
	}
	for _, msg := range firstBatch {
		if _, ok := seen[msg.ID]; ok {
			t.Fatalf("duplicate message claim: %d", msg.ID)
		}
		seen[msg.ID] = struct{}{}
	}

	secondBatch, err := store.ClaimBatch(topic, sub, 2, 2)
	if err != nil {
		t.Fatalf("claim batch 2: %v", err)
	}
	if len(secondBatch) != 1 {
		t.Fatalf("expected 1 message, got %d", len(secondBatch))
	}
	for _, msg := range secondBatch {
		if _, ok := seen[msg.ID]; ok {
			t.Fatalf("duplicate message claim: %d", msg.ID)
		}
		seen[msg.ID] = struct{}{}
	}
}
