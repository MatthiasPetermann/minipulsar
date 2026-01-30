package broker

// getOrCreateSubState returns the subscription state for a topic/subscription pair.
// It lazily allocates the state to keep memory usage proportional to active subscriptions.
func (b *Broker) getOrCreateSubState(topic, name string, persistent bool) *subState {
	key := subKey{topic: topic, name: name}

	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.subs[key]
	if s == nil {
		s = &subState{key: key, persistent: persistent}
		b.subs[key] = s
	}
	return s
}
