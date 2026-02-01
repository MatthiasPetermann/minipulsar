package broker

import (
	"runtime"
	"time"

	"minipulsar/internal/storage"
)

// StatsSnapshot represents a snapshot of broker activity for observability.
type StatsSnapshot struct {
	Producers     int
	Consumers     int
	Namespaces    int
	Messages      int
	Topics        int
	Subscriptions int
	Pending       int
	MemoryAlloc   uint64
	ThroughputPS  float64
	TopTopics     []storage.TopicStat
}

// StatsSnapshot returns a point-in-time view of broker and storage stats.
func (b *Broker) StatsSnapshot(limit int) (StatsSnapshot, error) {
	b.mu.RLock()
	producers := len(b.producers)
	consumers := len(b.consumers)
	b.mu.RUnlock()

	allocBytes := currentAlloc()
	throughput := b.throughputPerSecond()

	storeStats, err := b.store.StatsSnapshot(limit)
	if err != nil {
		return StatsSnapshot{
			Producers:    producers,
			Consumers:    consumers,
			MemoryAlloc:  allocBytes,
			ThroughputPS: throughput,
		}, err
	}

	return StatsSnapshot{
		Producers:     producers,
		Consumers:     consumers,
		Namespaces:    storeStats.Namespaces,
		Messages:      storeStats.Messages,
		Topics:        storeStats.Topics,
		Subscriptions: storeStats.Subscriptions,
		Pending:       storeStats.Pending,
		MemoryAlloc:   allocBytes,
		ThroughputPS:  throughput,
		TopTopics:     storeStats.TopTopics,
	}, nil
}

func (b *Broker) recordMessage() {
	b.messageCounter.Add(1)
}

func (b *Broker) throughputPerSecond() float64 {
	now := time.Now()
	total := b.messageCounter.Load()

	b.throughputMu.Lock()
	defer b.throughputMu.Unlock()

	if b.lastThroughputAt.IsZero() {
		b.lastThroughputAt = now
		b.lastThroughputCnt = total
		return 0
	}

	elapsed := now.Sub(b.lastThroughputAt).Seconds()
	if elapsed <= 0 {
		return 0
	}
	throughput := float64(total-b.lastThroughputCnt) / elapsed
	b.lastThroughputAt = now
	b.lastThroughputCnt = total
	return throughput
}

func currentAlloc() uint64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.Alloc
}
