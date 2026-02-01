package broker

import (
	"fmt"

	pulsar "minipulsar/pb"
)

// getOrCreateSubState returns the subscription state for a topic/subscription pair.
// It lazily allocates the state to keep memory usage proportional to active subscriptions.
func (b *Broker) getOrCreateSubState(topic, name string, persistent bool, subType pulsar.CommandSubscribe_SubType) (*subState, error) {
	key := subKey{topic: topic, name: name}

	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.subs[key]
	if s == nil {
		s = &subState{key: key, persistent: persistent, subType: subType}
		b.subs[key] = s
		return s, nil
	}
	if s.subType != subType {
		return nil, fmt.Errorf("subscription %s type mismatch: existing %s requested %s", name, s.subType, subType)
	}
	return s, nil
}
