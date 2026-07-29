package broker

import (
	"net"
	"testing"
	"time"

	"minipulsar/internal/messaging"
	"minipulsar/internal/storage"
	"minipulsar/internal/topic"
	pulsar "minipulsar/pb"
)

func TestNamespaceFromTopicAndAuthorize(t *testing.T) {
	// Pulsar authorizes clients at the namespace level, so we verify the broker derives
	// the expected namespace string and enforces role-based access for produce actions.
	store := openTestStore(t)
	security := &messaging.SecurityIR{
		Mode: messaging.ModeStrict,
		Namespaces: map[string]messaging.SecurityNamespacePolicy{
			"persistent://public/default": {
				Allowed: map[messaging.Action]map[string]struct{}{
					messaging.ActionProduce: {"role-a": {}},
				},
			},
		},
	}
	b := New(store, Config{Messaging: &messaging.Runtime{Security: security}})

	info, err := topic.Parse("persistent://public/default/orders")
	if err != nil {
		t.Fatalf("parse topic: %v", err)
	}
	namespace := b.namespaceFromTopic(info)
	if namespace != "persistent://public/default" {
		t.Fatalf("unexpected namespace: %s", namespace)
	}

	connAllowed, connDenied := net.Pipe()
	t.Cleanup(func() {
		_ = connAllowed.Close()
		_ = connDenied.Close()
	})

	b.mu.Lock()
	b.connRoles[connAllowed] = []string{"role-a"}
	b.mu.Unlock()

	if err := b.authorize(connAllowed, info, messaging.ActionProduce); err != nil {
		t.Fatalf("expected authorization, got %v", err)
	}
	if err := b.authorize(connDenied, info, messaging.ActionProduce); err == nil {
		t.Fatalf("expected authorization error for missing roles")
	}
}

func TestNamespaceFromTopicNonPersistent(t *testing.T) {
	// Pulsar distinguishes non-persistent namespaces in authorization, so we ensure
	// the broker returns the non-persistent namespace prefix for non-persistent topics.
	store := openTestStore(t)
	b := New(store, Config{})

	info, err := topic.Parse("non-persistent://acme/ops/alerts")
	if err != nil {
		t.Fatalf("parse topic: %v", err)
	}
	namespace := b.namespaceFromTopic(info)
	if namespace != "non-persistent://acme/ops" {
		t.Fatalf("unexpected namespace: %s", namespace)
	}
}

func TestProducerAccessHelpers(t *testing.T) {
	// Pulsar exclusive producers require fencing on a per-topic basis, so we verify
	// the broker tracks producer presence and clears fenced producers correctly.
	store := openTestStore(t)
	b := New(store, Config{})

	b.mu.Lock()
	condA := b.getProducerCondLocked("persistent://public/default/a")
	condARepeat := b.getProducerCondLocked("persistent://public/default/a")
	if condA != condARepeat {
		t.Fatalf("expected producer condition to be reused")
	}

	b.producers[producerKey{id: 1}] = &producer{id: 1, topic: "persistent://public/default/a"}
	b.producers[producerKey{id: 2}] = &producer{id: 2, topic: "persistent://public/default/b"}

	if !b.topicHasProducersLocked("persistent://public/default/a") {
		t.Fatalf("expected topic to have producers")
	}

	b.fenceProducersLocked("persistent://public/default/a")
	if b.topicHasProducersLocked("persistent://public/default/a") {
		t.Fatalf("expected producers for topic to be fenced")
	}
	if !b.topicHasProducersLocked("persistent://public/default/b") {
		t.Fatalf("expected other topic producers to remain")
	}
	b.mu.Unlock()
}

func TestGetOrCreateSubStateTypeMismatch(t *testing.T) {
	// Pulsar subscription types must remain consistent for a subscription name,
	// so we ensure the broker rejects mismatched subscription types.
	store := openTestStore(t)
	b := New(store, Config{})

	if _, err := b.getOrCreateSubState("persistent://public/default/events", "sub", true, pulsar.CommandSubscribe_Shared); err != nil {
		t.Fatalf("create sub state: %v", err)
	}
	if _, err := b.getOrCreateSubState("persistent://public/default/events", "sub", true, pulsar.CommandSubscribe_Failover); err == nil {
		t.Fatalf("expected type mismatch error")
	}
}

func TestNonPersistentIDAndThroughput(t *testing.T) {
	// Pulsar non-persistent topics use broker-local ids and throughput stats, so we
	// confirm the ID sequence advances and throughput counters update as messages flow.
	store := openTestStore(t)
	b := New(store, Config{})

	first := b.nextNonPersistentID("non-persistent://public/default/ephemeral")
	second := b.nextNonPersistentID("non-persistent://public/default/ephemeral")
	if first != 1 || second != 2 {
		t.Fatalf("unexpected non-persistent IDs: %d, %d", first, second)
	}

	b.recordMessage()
	b.recordMessage()
	if got := b.messageCounter.Load(); got != 2 {
		t.Fatalf("unexpected message counter: %d", got)
	}

	b.messageCounter.Store(10)
	b.lastThroughputCnt = 4
	b.lastThroughputAt = time.Now().Add(-2 * time.Second)
	throughput := b.throughputPerSecond()
	if throughput <= 0 {
		t.Fatalf("expected throughput to be positive, got %f", throughput)
	}
}

func TestPersistentPublishIsDurableWithZeroRetentionAndNoSubscription(t *testing.T) {
	store := openTestStore(t)
	b := New(store, Config{Messaging: &messaging.Runtime{
		NamespacePolicies: map[string]messaging.NamespacePolicy{
			"persistent://public/default": {},
		},
	}})
	info, err := topic.Parse("persistent://public/default/audit")
	if err != nil {
		t.Fatalf("parse topic: %v", err)
	}
	message, err := b.publishMessage(info, storage.Message{Payload: []byte("durable")})
	if err != nil {
		t.Fatalf("publish message: %v", err)
	}
	if message.ID == 0 {
		t.Fatal("persistent publish did not receive a SQLite entry ID")
	}
	stats, err := store.StatsSnapshot(1)
	if err != nil {
		t.Fatalf("storage stats: %v", err)
	}
	if stats.Messages != 1 {
		t.Fatalf("stored messages = %d, want 1", stats.Messages)
	}
}
