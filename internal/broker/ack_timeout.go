package broker

import "time"

const defaultAckTimeoutCheckInterval = 30 * time.Second

// startAckTimeoutMonitor requeues pending messages when acknowledgements expire.
func (b *Broker) startAckTimeoutMonitor() {
	if b.cfg.AckTimeout <= 0 {
		return
	}
	interval := b.cfg.AckTimeoutCheckInterval
	if interval <= 0 {
		interval = defaultAckTimeoutCheckInterval
	}
	ticker := time.NewTicker(interval)
	b.lifecycleWG.Add(1)
	go func() {
		defer b.lifecycleWG.Done()
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				b.expirePending()
			case <-b.lifecycleCtx.Done():
				return
			}
		}
	}()
}

func (b *Broker) expirePending() {
	cutoff := time.Now().Add(-b.cfg.AckTimeout)
	cleared, subs, err := b.store.ExpirePendingBefore(cutoff)
	if err != nil {
		b.cfg.Logger.Warn("ack timeout sweep failed", "err", err)
		return
	}
	if cleared == 0 {
		return
	}
	b.cfg.Logger.Info("ack timeout sweep completed",
		"expired_pending", cleared,
		"ack_timeout", b.cfg.AckTimeout.String(),
	)
	for _, sub := range subs {
		key := subKey{topic: sub.Topic, name: sub.Subscription}
		b.mu.RLock()
		state := b.subs[key]
		b.mu.RUnlock()
		if state == nil {
			continue
		}
		b.maybeStartSubDelivery(state)
	}
}
