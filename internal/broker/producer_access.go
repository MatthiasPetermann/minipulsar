package broker

import "sync"

func (b *Broker) getProducerCondLocked(topic string) *sync.Cond {
	cond := b.producerConds[topic]
	if cond == nil {
		cond = sync.NewCond(&b.mu)
		b.producerConds[topic] = cond
	}
	return cond
}

func (b *Broker) topicHasProducersLocked(topic string) bool {
	for _, p := range b.producers {
		if p.topic == topic {
			return true
		}
	}
	return false
}

func (b *Broker) fenceProducersLocked(topic string) {
	for key, p := range b.producers {
		if p.topic == topic {
			delete(b.producers, key)
		}
	}
	b.signalProducerWaitersLocked(topic)
}

func (b *Broker) signalProducerWaitersLocked(topic string) {
	if cond := b.producerConds[topic]; cond != nil {
		cond.Broadcast()
	}
}

func (b *Broker) signalProducerWaiters(topic string) {
	b.mu.Lock()
	b.signalProducerWaitersLocked(topic)
	b.mu.Unlock()
}
