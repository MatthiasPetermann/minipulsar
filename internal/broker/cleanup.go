package broker

// removeConsumer unregisters a consumer and clears its pending messages.
// It also re-triggers delivery for remaining consumers on the subscription.
func (b *Broker) removeConsumer(key consumerKey) {
	var c *consumer

	b.mu.Lock()
	c = b.consumers[key]
	delete(b.consumers, key)
	b.mu.Unlock()

	if c == nil {
		return
	}

	// Remove from subState list.
	skey := subKey{topic: c.topic, name: c.subscription}

	b.mu.RLock()
	s := b.subs[skey]
	b.mu.RUnlock()

	if s != nil {
		s.mu.Lock()
		dst := s.consumers[:0]
		for _, x := range s.consumers {
			if x != c {
				dst = append(dst, x)
			}
		}
		s.consumers = dst
		s.mu.Unlock()
	}

	// Drop pending messages for this consumer UID (so others can progress).
	_ = b.store.DropPendingByConsumer(c.topic, c.subscription, c.uid)

	// Trigger delivery again (might unblock others).
	if s != nil {
		b.maybeStartSubDelivery(s)
	}
}
