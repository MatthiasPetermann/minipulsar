package broker

import (
	pulsar "minipulsar/pb"
)

// kickTopic triggers delivery for all subscriptions on a topic.
// It is invoked after message persistence so subscribers receive new data promptly.
func (b *Broker) kickTopic(topic string) {
	// snapshot subscription states for topic
	var subs []*subState
	b.mu.RLock()
	for k, s := range b.subs {
		if k.topic == topic {
			subs = append(subs, s)
		}
	}
	b.mu.RUnlock()

	for _, s := range subs {
		b.maybeStartSubDelivery(s)
	}
}

// maybeStartSubDelivery ensures only one delivery loop runs per subscription.
// It checks for available permits before spawning a goroutine.
func (b *Broker) maybeStartSubDelivery(s *subState) {
	s.mu.Lock()
	if !s.persistent {
		s.mu.Unlock()
		return
	}
	// already running?
	if s.delivering {
		s.mu.Unlock()
		return
	}

	if s.readyConsumer() == nil {
		s.mu.Unlock()
		return
	}

	s.delivering = true
	s.mu.Unlock()

	go b.deliveryLoop(s)
}

// nextConsumerWithPermits selects the next consumer eligible to receive messages.
// It rotates through consumers for shared subscription round-robin delivery.
func (s *subState) nextConsumerWithPermits() *consumer {
	n := len(s.consumers)
	if n == 0 {
		return nil
	}
	for i := 0; i < n; i++ {
		idx := (s.rr + i) % n
		c := s.consumers[idx]
		c.mu.Lock()
		p := c.permits
		c.mu.Unlock()
		if p > 0 {
			s.rr = (idx + 1) % n
			return c
		}
	}
	return nil
}

// deliveryLoop repeatedly claims messages and sends them to consumers.
// It respects permits and updates the pending set to avoid duplicate delivery.
func (b *Broker) deliveryLoop(s *subState) {
	defer func() {
		s.mu.Lock()
		s.delivering = false
		s.mu.Unlock()
	}()

	const maxBatch = 200

	for {
		s.mu.Lock()
		c := s.selectConsumerWithPermits()
		s.mu.Unlock()
		if c == nil {
			return
		}

		c.mu.Lock()
		permits := c.permits
		c.mu.Unlock()
		if permits <= 0 {
			continue
		}

		limit := permits
		if limit > maxBatch {
			limit = maxBatch
		}

		msgs, err := b.store.ClaimBatch(s.key.topic, s.key.name, c.uid, limit)
		if err != nil {
			b.cfg.Logger.Warn("claim error",
				"err", err,
				"topic", s.key.topic,
				"subscription", s.key.name,
			)
			return
		}
		if len(msgs) == 0 {
			return
		}

		for _, m := range msgs {
			b.waitForThrottle()
			// send (write serialized per conn)
			if err := b.writeMsgFrame(c.conn, c.id, m); err != nil {
				b.cfg.Logger.Warn("deliver write error", "err", err, "consumer_id", c.id)
				// keep pending; disconnect/timeout will clean it up
				return
			}

			c.mu.Lock()
			if c.permits > 0 {
				c.permits--
			}
			c.mu.Unlock()
		}
	}
}

func (s *subState) readyConsumer() *consumer {
	switch s.subType {
	case pulsar.CommandSubscribe_Shared:
		for _, c := range s.consumers {
			c.mu.Lock()
			p := c.permits
			c.mu.Unlock()
			if p > 0 {
				return c
			}
		}
		return nil
	case pulsar.CommandSubscribe_Exclusive:
		return s.firstConsumerWithPermits()
	case pulsar.CommandSubscribe_Failover:
		return s.primaryConsumerWithPermits()
	default:
		return nil
	}
}

func (s *subState) selectConsumerWithPermits() *consumer {
	switch s.subType {
	case pulsar.CommandSubscribe_Shared:
		return s.nextConsumerWithPermits()
	case pulsar.CommandSubscribe_Exclusive:
		return s.firstConsumerWithPermits()
	case pulsar.CommandSubscribe_Failover:
		return s.primaryConsumerWithPermits()
	default:
		return nil
	}
}

func (s *subState) firstConsumerWithPermits() *consumer {
	if len(s.consumers) == 0 {
		return nil
	}
	c := s.consumers[0]
	c.mu.Lock()
	p := c.permits
	c.mu.Unlock()
	if p > 0 {
		return c
	}
	return nil
}

func (s *subState) primaryConsumerWithPermits() *consumer {
	if len(s.consumers) == 0 {
		return nil
	}
	var best *consumer
	bestPriority := 0
	for _, c := range s.consumers {
		if best == nil || c.priority > bestPriority {
			best = c
			bestPriority = c.priority
		}
	}
	if best == nil {
		return nil
	}
	best.mu.Lock()
	p := best.permits
	best.mu.Unlock()
	if p > 0 {
		return best
	}
	return nil
}
