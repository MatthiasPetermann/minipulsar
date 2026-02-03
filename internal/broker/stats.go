package broker

import (
	"runtime"
	"sort"
	"time"

	"minipulsar/internal/storage"
)

// StatsSnapshot represents a snapshot of broker activity for observability.
type StatsSnapshot struct {
	Producers               int
	Consumers               int
	Namespaces              int
	Messages                int
	Topics                  int
	Subscriptions           int
	Pending                 int
	MemoryAlloc             uint64
	ThroughputPS            float64
	TopTopics               []storage.TopicStat
	TopSubscriptionsBacklog []storage.SubscriptionBacklogStat
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

	topTopics := storeStats.TopTopics
	var subscriptionBacklog []storage.SubscriptionBacklogStat
	if b.cfg.Messaging != nil && len(b.cfg.Messaging.NamespacePolicies) > 0 {
		topTopics, subscriptionBacklog, err = b.backlogStats(limit)
		if err != nil {
			return StatsSnapshot{
				Producers:    producers,
				Consumers:    consumers,
				MemoryAlloc:  allocBytes,
				ThroughputPS: throughput,
			}, err
		}
	}

	return StatsSnapshot{
		Producers:               producers,
		Consumers:               consumers,
		Namespaces:              storeStats.Namespaces,
		Messages:                storeStats.Messages,
		Topics:                  storeStats.Topics,
		Subscriptions:           storeStats.Subscriptions,
		Pending:                 storeStats.Pending,
		MemoryAlloc:             allocBytes,
		ThroughputPS:            throughput,
		TopTopics:               topTopics,
		TopSubscriptionsBacklog: subscriptionBacklog,
	}, nil
}

func (b *Broker) backlogStats(limit int) ([]storage.TopicStat, []storage.SubscriptionBacklogStat, error) {
	if limit <= 0 {
		limit = 10
	}
	now := time.Now()
	var topics []storage.TopicStat
	var subs []storage.SubscriptionBacklogStat
	for namespace, policy := range b.cfg.Messaging.NamespacePolicies {
		cutoff := now.Add(-policy.Retention)
		nsTopics, err := b.store.TopicStatsWithBacklog(namespace, cutoff, limit)
		if err != nil {
			return nil, nil, err
		}
		if len(nsTopics) > 0 {
			topics = append(topics, nsTopics...)
		}
		nsSubs, err := b.store.SubscriptionBacklogStats(namespace, cutoff, limit)
		if err != nil {
			return nil, nil, err
		}
		if len(nsSubs) > 0 {
			subs = append(subs, nsSubs...)
		}
	}

	sort.Slice(topics, func(i, j int) bool {
		if topics[i].BacklogCount != topics[j].BacklogCount {
			return topics[i].BacklogCount > topics[j].BacklogCount
		}
		if topics[i].PendingCount != topics[j].PendingCount {
			return topics[i].PendingCount > topics[j].PendingCount
		}
		if topics[i].MessageCount != topics[j].MessageCount {
			return topics[i].MessageCount > topics[j].MessageCount
		}
		return topics[i].Topic < topics[j].Topic
	})
	if len(topics) > limit {
		topics = topics[:limit]
	}

	sort.Slice(subs, func(i, j int) bool {
		if subs[i].BacklogCount != subs[j].BacklogCount {
			return subs[i].BacklogCount > subs[j].BacklogCount
		}
		if subs[i].Topic != subs[j].Topic {
			return subs[i].Topic < subs[j].Topic
		}
		return subs[i].Subscription < subs[j].Subscription
	})
	if len(subs) > limit {
		subs = subs[:limit]
	}

	return topics, subs, nil
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
