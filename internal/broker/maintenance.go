package broker

import (
	"time"

	"minipulsar/internal/storage"
	"minipulsar/internal/topic"
)

const defaultNamespaceMaintenanceInterval = 30 * time.Second

// startNamespaceMaintenance runs cleanup for namespace policies in the background.
func (b *Broker) startNamespaceMaintenance() {
	if b.cfg.Messaging == nil || len(b.cfg.Messaging.NamespacePolicies) == 0 {
		return
	}

	ticker := time.NewTicker(b.cfg.NamespaceMaintenanceInterval)
	go func() {
		for range ticker.C {
			b.runNamespaceMaintenance()
		}
	}()
}

func (b *Broker) runNamespaceMaintenance() {
	now := time.Now()
	for namespace, policy := range b.cfg.Messaging.NamespacePolicies {
		b.touchActiveSubscriptions(namespace)
		if policy.SubscriptionTimeout > 0 {
			cutoff := now.Add(-policy.SubscriptionTimeout)
			dropped, err := b.store.PruneStaleSubscriptions(namespace, cutoff)
			if err != nil {
				b.cfg.Logger.Warn("subscription timeout cleanup failed", "namespace", namespace, "err", err)
			} else {
				b.cfg.Logger.Info("subscription timeout cleanup completed", "namespace", namespace, "dropped_subscriptions", len(dropped))
				b.dropSubscriptionStates(dropped)
			}
		}
		cutoff := now.Add(-policy.Retention)
		consumedRemoved, err := b.store.PruneConsumedMessages(namespace, cutoff)
		if err != nil {
			b.cfg.Logger.Warn("consumed message retention cleanup failed", "namespace", namespace, "err", err)
		} else {
			b.cfg.Logger.Info("consumed message retention cleanup completed", "namespace", namespace, "deleted_messages", consumedRemoved)
		}
		removed, err := b.store.PruneOrphanedMessages(namespace, cutoff)
		if err != nil {
			b.cfg.Logger.Warn("orphaned message retention cleanup failed", "namespace", namespace, "err", err)
		} else {
			b.cfg.Logger.Info("orphaned message retention cleanup completed", "namespace", namespace, "deleted_messages", removed)
		}
		excluded := b.activeTopicsForNamespace(namespace)
		deleted, err := b.store.PruneEmptyTopics(namespace, excluded)
		if err != nil {
			b.cfg.Logger.Warn("empty topic cleanup failed", "namespace", namespace, "err", err)
		} else {
			b.cfg.Logger.Info("empty topic cleanup completed", "namespace", namespace, "deleted_topics", deleted)
		}
	}
}

func (b *Broker) touchActiveSubscriptions(namespace string) {
	subs := b.activeSubscriptionsForNamespace(namespace)
	if len(subs) == 0 {
		return
	}
	if err := b.store.TouchSubscriptions(namespace, subs); err != nil {
		b.cfg.Logger.Warn("refresh subscription activity failed", "namespace", namespace, "err", err)
	}
}

func (b *Broker) activeSubscriptionsForNamespace(namespace string) []storage.SubscriptionRef {
	info, err := topic.Parse(namespace + "/__validate")
	if err != nil {
		return nil
	}
	active := make([]storage.SubscriptionRef, 0)
	addIfMatch := func(topicName, subscription string) {
		topicInfo, err := topic.Parse(topicName)
		if err != nil {
			return
		}
		if topicInfo.Persistent != info.Persistent {
			return
		}
		if topicInfo.Tenant != info.Tenant || topicInfo.Namespace != info.Namespace {
			return
		}
		active = append(active, storage.SubscriptionRef{
			Topic:        topicInfo.FullName,
			Subscription: subscription,
		})
	}

	b.mu.RLock()
	for key, state := range b.subs {
		state.mu.Lock()
		hasConsumers := len(state.consumers) > 0
		state.mu.Unlock()
		if !hasConsumers {
			continue
		}
		addIfMatch(key.topic, key.name)
	}
	b.mu.RUnlock()

	return active
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

func (b *Broker) activeTopicsForNamespace(namespace string) []string {
	info, err := topic.Parse(namespace + "/__validate")
	if err != nil {
		return nil
	}
	active := make(map[string]struct{})
	addIfMatch := func(topicName string) {
		topicInfo, err := topic.Parse(topicName)
		if err != nil {
			return
		}
		if topicInfo.Persistent != info.Persistent {
			return
		}
		if topicInfo.Tenant != info.Tenant || topicInfo.Namespace != info.Namespace {
			return
		}
		active[topicInfo.FullName] = struct{}{}
	}

	b.mu.RLock()
	for _, producer := range b.producers {
		if producer.persistent {
			addIfMatch(producer.topic)
		}
	}
	for _, consumer := range b.consumers {
		if consumer.persistent {
			addIfMatch(consumer.topic)
		}
	}
	b.mu.RUnlock()

	if b.cfg.Messaging != nil {
		for source, bindings := range b.cfg.Messaging.Bindings {
			addIfMatch(source)
			for _, binding := range bindings {
				addIfMatch(binding.TargetTopic)
			}
		}
	}

	excluded := make([]string, 0, len(active))
	for topicName := range active {
		excluded = append(excluded, topicName)
	}
	return excluded
}
