package storage

import (
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func openExtraTestStore(t *testing.T) *Store {
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
	store := openExtraTestStore(t)

	topic := "persistent://public/default/test"
	if err := store.EnsureSubscription(topic, "sub", InitialPositionEarliest, SubscriptionTypeShared); err != nil {
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

func TestClaimBatchAdvancesCursorAndAckClearsPending(t *testing.T) {
	store := openExtraTestStore(t)
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
	store := openExtraTestStore(t)
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

	if err := store.EnsureSubscription(topic, "sub", InitialPositionLatest, SubscriptionTypeShared); err != nil {
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

func TestClaimBatchSkipsPendingMessagesAdvancesCursor(t *testing.T) {
	store := openExtraTestStore(t)
	topic := "persistent://public/default/pending-skip-test"
	sub := "sub"

	if err := store.EnsureSubscription(topic, sub, InitialPositionEarliest, SubscriptionTypeShared); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}

	var ids []int64
	for i := 0; i < 3; i++ {
		msg := Message{
			Topic:   topic,
			Payload: []byte("payload"),
		}
		if err := store.InsertMessage(&msg); err != nil {
			t.Fatalf("insert message: %v", err)
		}
		ids = append(ids, msg.ID)
	}

	batch, err := store.ClaimBatch(topic, sub, 1, 1)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 message claimed, got %d", len(batch))
	}

	var topicID int64
	if err := store.db.QueryRow(
		"SELECT id FROM topics WHERE full_name=?",
		topic,
	).Scan(&topicID); err != nil {
		t.Fatalf("lookup topic id: %v", err)
	}

	if _, err := store.db.Exec(
		"INSERT INTO subscription_pending(topic_id, name, message_id, consumer_id, delivered_at) VALUES(?,?,?,?,?)",
		topicID, sub, ids[1], int64(2), time.Now().UnixMilli(),
	); err != nil {
		t.Fatalf("insert pending: %v", err)
	}

	nextBatch, err := store.ClaimBatch(topic, sub, 3, 5)
	if err != nil {
		t.Fatalf("claim batch 2: %v", err)
	}
	if len(nextBatch) != 1 {
		t.Fatalf("expected 1 message claimed, got %d", len(nextBatch))
	}
	if nextBatch[0].ID != ids[2] {
		t.Fatalf("expected message %d, got %d", ids[2], nextBatch[0].ID)
	}

	nextID, err := scanSubscriptionCursor(store.db, sub)
	if err != nil {
		t.Fatalf("query cursor: %v", err)
	}
	if nextID != ids[2]+1 {
		t.Fatalf("expected cursor %d, got %d", ids[2]+1, nextID)
	}
}

func TestAckIndividualRespectsConsumerID(t *testing.T) {
	store := openExtraTestStore(t)
	topic := "persistent://public/default/ack-consumer-test"
	sub := "sub"

	if err := store.EnsureSubscription(topic, sub, InitialPositionEarliest, SubscriptionTypeShared); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}

	msg := Message{
		Topic:   topic,
		Payload: []byte("payload"),
	}
	if err := store.InsertMessage(&msg); err != nil {
		t.Fatalf("insert message: %v", err)
	}

	batch, err := store.ClaimBatch(topic, sub, 1, 1)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 message claimed, got %d", len(batch))
	}

	if err := store.AckIndividual(topic, sub, 2, []int64{batch[0].ID}); err != nil {
		t.Fatalf("ack with wrong consumer: %v", err)
	}

	var pending int
	if err := store.db.QueryRow(
		"SELECT COUNT(*) FROM subscription_pending WHERE name=?",
		sub,
	).Scan(&pending); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if pending != 1 {
		t.Fatalf("expected 1 pending message, got %d", pending)
	}

	if err := store.AckIndividual(topic, sub, 1, []int64{batch[0].ID}); err != nil {
		t.Fatalf("ack with correct consumer: %v", err)
	}

	if err := store.db.QueryRow(
		"SELECT COUNT(*) FROM subscription_pending WHERE name=?",
		sub,
	).Scan(&pending); err != nil {
		t.Fatalf("count pending after ack: %v", err)
	}
	if pending != 0 {
		t.Fatalf("expected pending cleared, got %d", pending)
	}
}

func TestAckCumulativeClearsPendingUpToMessage(t *testing.T) {
	store := openExtraTestStore(t)
	topic := "persistent://public/default/ack-cumulative-test"
	sub := "sub"

	if err := store.EnsureSubscription(topic, sub, InitialPositionEarliest, SubscriptionTypeExclusive); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}

	for i := 0; i < 3; i++ {
		msg := Message{
			Topic:   topic,
			Payload: []byte("payload"),
		}
		if err := store.InsertMessage(&msg); err != nil {
			t.Fatalf("insert message: %v", err)
		}
	}

	batch, err := store.ClaimBatch(topic, sub, 1, 3)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}
	if len(batch) != 3 {
		t.Fatalf("expected 3 messages claimed, got %d", len(batch))
	}

	if err := store.AckCumulative(topic, sub, 1, batch[1].ID); err != nil {
		t.Fatalf("ack cumulative: %v", err)
	}

	var pending int
	if err := store.db.QueryRow(
		"SELECT COUNT(*) FROM subscription_pending WHERE name=?",
		sub,
	).Scan(&pending); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if pending != 1 {
		t.Fatalf("expected 1 pending message remaining, got %d", pending)
	}
}

func TestExpirePendingBeforeRequeuesMessages(t *testing.T) {
	store := openExtraTestStore(t)
	topic := "persistent://public/default/ack-timeout-test"
	sub := "sub"

	if err := store.EnsureSubscription(topic, sub, InitialPositionEarliest, SubscriptionTypeShared); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}

	msg := Message{
		Topic:   topic,
		Payload: []byte("payload"),
	}
	if err := store.InsertMessage(&msg); err != nil {
		t.Fatalf("insert message: %v", err)
	}

	batch, err := store.ClaimBatch(topic, sub, 1, 1)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 message claimed, got %d", len(batch))
	}

	var topicID int64
	if err := store.db.QueryRow(
		"SELECT id FROM topics WHERE full_name=?",
		topic,
	).Scan(&topicID); err != nil {
		t.Fatalf("lookup topic id: %v", err)
	}

	expiredAt := time.Now().Add(-2 * time.Hour).UnixMilli()
	if _, err := store.db.Exec(
		"UPDATE subscription_pending SET delivered_at=? WHERE topic_id=? AND name=?",
		expiredAt, topicID, sub,
	); err != nil {
		t.Fatalf("expire pending: %v", err)
	}

	cleared, subs, err := store.ExpirePendingBefore(time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("expire pending before: %v", err)
	}
	if cleared != 1 {
		t.Fatalf("expected 1 pending cleared, got %d", cleared)
	}
	if len(subs) != 1 || subs[0].Topic != topic || subs[0].Subscription != sub {
		t.Fatalf("unexpected subscriptions affected: %+v", subs)
	}

	nextID, err := scanSubscriptionCursor(store.db, sub)
	if err != nil {
		t.Fatalf("query cursor: %v", err)
	}
	if nextID != batch[0].ID {
		t.Fatalf("expected cursor reset to %d, got %d", batch[0].ID, nextID)
	}

	redeliver, err := store.ClaimBatch(topic, sub, 2, 1)
	if err != nil {
		t.Fatalf("claim batch after expire: %v", err)
	}
	if len(redeliver) != 1 || redeliver[0].ID != batch[0].ID {
		t.Fatalf("expected message %d to be redelivered, got %+v", batch[0].ID, redeliver)
	}
}

func TestPruneStaleSubscriptions(t *testing.T) {
	store := openExtraTestStore(t)
	topic := "persistent://public/default/stale-test"

	if err := store.EnsureSubscription(topic, "sub", InitialPositionEarliest, SubscriptionTypeShared); err != nil {
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

func TestSubscriptionBacklogIncludesPending(t *testing.T) {
	store := openExtraTestStore(t)
	topic := "persistent://public/default/backlog-pending-test"
	sub := "sub"

	if err := store.EnsureSubscription(topic, sub, InitialPositionEarliest, SubscriptionTypeShared); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}

	for i := 0; i < 3; i++ {
		msg := Message{
			Topic:   topic,
			Payload: []byte("payload"),
		}
		if err := store.InsertMessage(&msg); err != nil {
			t.Fatalf("insert message: %v", err)
		}
	}

	batch, err := store.ClaimBatch(topic, sub, 1, 2)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("expected 2 messages claimed, got %d", len(batch))
	}

	stats, err := store.SubscriptionBacklogStats("persistent://public/default", 10)
	if err != nil {
		t.Fatalf("subscription backlog stats: %v", err)
	}

	var backlog int
	for _, stat := range stats {
		if stat.Topic == topic && stat.Subscription == sub {
			backlog = stat.BacklogCount
		}
	}
	if backlog != 3 {
		t.Fatalf("expected backlog 3 (pending + undelivered), got %d", backlog)
	}
}

func TestEnsureSubscriptionReplacesOrphanedCursor(t *testing.T) {
	store := openExtraTestStore(t)
	topic := "persistent://public/default/orphaned-cursor-test"

	if err := store.EnsureSubscription(topic, "sub", InitialPositionEarliest, SubscriptionTypeShared); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}

	if _, err := store.db.Exec(
		"DELETE FROM subscriptions WHERE name=?",
		"sub",
	); err != nil {
		t.Fatalf("delete subscription: %v", err)
	}

	if err := store.EnsureSubscription(topic, "sub", InitialPositionEarliest, SubscriptionTypeShared); err != nil {
		t.Fatalf("recreate subscription: %v", err)
	}

	nextID, err := scanSubscriptionCursor(store.db, "sub")
	if err != nil {
		t.Fatalf("query cursor: %v", err)
	}
	if nextID != 1 {
		t.Fatalf("expected cursor reset to 1, got %d", nextID)
	}
}

func TestPruneOrphanedSubscriptionData(t *testing.T) {
	store := openExtraTestStore(t)
	topic := "persistent://public/default/orphaned-sub-data-test"

	if err := store.EnsureSubscription(topic, "sub", InitialPositionEarliest, SubscriptionTypeShared); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}
	if err := store.EnsureSubscription(topic, "active", InitialPositionEarliest, SubscriptionTypeShared); err != nil {
		t.Fatalf("ensure subscription: %v", err)
	}

	var topicID int64
	if err := store.db.QueryRow(
		"SELECT id FROM topics WHERE full_name=?",
		topic,
	).Scan(&topicID); err != nil {
		t.Fatalf("lookup topic id: %v", err)
	}

	if _, err := store.db.Exec(
		"DELETE FROM subscriptions WHERE topic_id=? AND name=?",
		topicID, "sub",
	); err != nil {
		t.Fatalf("delete subscription: %v", err)
	}

	if _, err := store.db.Exec(
		"INSERT INTO subscription_pending(topic_id, name, message_id, consumer_id, delivered_at) VALUES(?,?,?,?,?)",
		topicID, "sub", int64(101), int64(7), time.Now().UnixMilli(),
	); err != nil {
		t.Fatalf("insert orphaned pending: %v", err)
	}

	cursors, pending, err := store.PruneOrphanedSubscriptionData("persistent://public/default")
	if err != nil {
		t.Fatalf("prune orphaned subscription data: %v", err)
	}
	if cursors != 1 {
		t.Fatalf("expected 1 cursor pruned, got %d", cursors)
	}
	if pending != 1 {
		t.Fatalf("expected 1 pending pruned, got %d", pending)
	}

	var activeCursor int
	if err := store.db.QueryRow(
		"SELECT COUNT(*) FROM subscription_cursor WHERE name=?",
		"active",
	).Scan(&activeCursor); err != nil {
		t.Fatalf("count active cursor: %v", err)
	}
	if activeCursor != 1 {
		t.Fatalf("expected active cursor to remain, got %d", activeCursor)
	}
}

func TestPruneOrphanedMessages(t *testing.T) {
	store := openExtraTestStore(t)
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
	store := openExtraTestStore(t)
	topic := "persistent://public/default/consumed-retention-test"

	if err := store.EnsureSubscription(topic, "sub", InitialPositionEarliest, SubscriptionTypeShared); err != nil {
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
