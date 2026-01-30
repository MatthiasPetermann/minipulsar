package broker

import "minipulsar/internal/storage"

// StatsSnapshot represents a snapshot of broker activity for observability.
type StatsSnapshot struct {
	Producers     int
	Consumers     int
	Topics        int
	Subscriptions int
	Pending       int
	TopTopics     []storage.TopicStat
}

// StatsSnapshot returns a point-in-time view of broker and storage stats.
func (b *Broker) StatsSnapshot(limit int) (StatsSnapshot, error) {
	b.mu.RLock()
	producers := len(b.producers)
	consumers := len(b.consumers)
	b.mu.RUnlock()

	storeStats, err := b.store.StatsSnapshot(limit)
	if err != nil {
		return StatsSnapshot{Producers: producers, Consumers: consumers}, err
	}

	return StatsSnapshot{
		Producers:     producers,
		Consumers:     consumers,
		Topics:        storeStats.Topics,
		Subscriptions: storeStats.Subscriptions,
		Pending:       storeStats.Pending,
		TopTopics:     storeStats.TopTopics,
	}, nil
}
