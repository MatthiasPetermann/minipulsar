package broker

import (
	"minipulsar/internal/storage"
)

func (b *Broker) nextNonPersistentID(topic string) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nonPersistentSeq[topic]++
	return int64(b.nonPersistentSeq[topic])
}

func (b *Broker) deliverNonPersistent(topic string, msg storage.Message) {
	var subs []*subState
	b.mu.RLock()
	for k, s := range b.subs {
		if k.topic == topic && !s.persistent {
			subs = append(subs, s)
		}
	}
	b.mu.RUnlock()

	for _, s := range subs {
		s.mu.Lock()
		c := s.nextConsumerWithPermits()
		s.mu.Unlock()
		if c == nil {
			continue
		}

		if err := b.writeMsgFrame(c.conn, c.id, msg); err != nil {
			b.cfg.Logger.WithField("consumer_id", c.id).WithError(err).Warn("deliver write error")
			continue
		}

		c.mu.Lock()
		if c.permits > 0 {
			c.permits--
		}
		c.mu.Unlock()
	}
}
