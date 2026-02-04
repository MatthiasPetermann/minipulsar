package broker

import "sync"

// getProducerCondLocked returns the condition variable for producer arbitration.
// Caller must hold b.mu to coordinate exclusive producer access.
func (b *Broker) getProducerCondLocked(topic string) *sync.Cond {
	cond := b.producerConds[topic]
	if cond == nil {
		cond = sync.NewCond(&b.mu)
		b.producerConds[topic] = cond
	}
	return cond
}

// topicHasProducersLocked checks if any producer currently owns the topic.
// Caller must hold b.mu to read the producer map safely.
func (b *Broker) topicHasProducersLocked(topic string) bool {
	for _, p := range b.producers {
		if p.topic == topic {
			return true
		}
	}
	return false
}

// fenceProducersLocked disconnects existing producers for fencing semantics.
// Caller must hold b.mu before invoking.
func (b *Broker) fenceProducersLocked(topic string) {
	for key, p := range b.producers {
		if p.topic == topic {
			delete(b.producers, key)
		}
	}
	b.signalProducerWaitersLocked(topic)
}

// signalProducerWaitersLocked wakes producers waiting for exclusive access.
// Caller must hold b.mu.
func (b *Broker) signalProducerWaitersLocked(topic string) {
	if cond := b.producerConds[topic]; cond != nil {
		cond.Broadcast()
	}
}

// signalProducerWaiters wraps the locked wake-up for callers without b.mu.
func (b *Broker) signalProducerWaiters(topic string) {
	b.mu.Lock()
	b.signalProducerWaitersLocked(topic)
	b.mu.Unlock()
}
