package broker

import (
	"time"

	"minipulsar/internal/storage"
)

const namespaceMaintenanceInterval = 30 * time.Second

// startNamespaceMaintenance runs cleanup for namespace policies in the background.
func (b *Broker) startNamespaceMaintenance() {
	if b.cfg.Messaging == nil || len(b.cfg.Messaging.NamespacePolicies) == 0 {
		return
	}

	ticker := time.NewTicker(namespaceMaintenanceInterval)
	go func() {
		for range ticker.C {
			b.runNamespaceMaintenance()
		}
	}()
}

func (b *Broker) runNamespaceMaintenance() {
	now := time.Now()
	for namespace, policy := range b.cfg.Messaging.NamespacePolicies {
		if policy.SubscriptionTimeout > 0 {
			cutoff := now.Add(-policy.SubscriptionTimeout)
			dropped, err := b.store.PruneStaleSubscriptions(namespace, cutoff)
			if err != nil {
				b.cfg.Logger.Warn("subscription timeout cleanup failed", "namespace", namespace, "err", err)
			} else {
				b.dropSubscriptionStates(dropped)
			}
		}
		if policy.Retention > 0 {
			cutoff := now.Add(-policy.Retention)
			if _, err := b.store.PruneOrphanedMessages(namespace, cutoff); err != nil {
				b.cfg.Logger.Warn("orphaned message retention cleanup failed", "namespace", namespace, "err", err)
			}
		}
	}
}

func (b *Broker) dropSubscriptionStates(dropped []storage.DroppedSubscription) {
	for _, entry := range dropped {
		key := subKey{topic: entry.Topic, name: entry.Subscription}
		b.mu.RLock()
		state := b.subs[key]
		b.mu.RUnlock()
		if state == nil {
			continue
		}
		state.mu.Lock()
		hasConsumers := len(state.consumers) > 0
		state.mu.Unlock()
		if hasConsumers {
			continue
		}
		b.mu.Lock()
		delete(b.subs, key)
		b.mu.Unlock()
	}
}
