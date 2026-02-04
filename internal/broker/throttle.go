package broker

import "time"

const (
	// MaxThrottleLevel is the maximum delay level for broker throttling.
	MaxThrottleLevel = 5
	throttleStep     = time.Second
	pausePollDelay   = 100 * time.Millisecond
)

// SetThrottleLevel updates the global delay level (0..MaxThrottleLevel) and returns the clamped value.
func (b *Broker) SetThrottleLevel(level int) int {
	if level < 0 {
		level = 0
	}
	if level > MaxThrottleLevel {
		level = MaxThrottleLevel
	}
	b.throttleDelay.Store(int64(level))
	return level
}

// ThrottleLevel returns the current throttle level.
func (b *Broker) ThrottleLevel() int {
	return int(b.throttleDelay.Load())
}

// SetThrottlePaused sets whether throttling is paused (halts ingress and delivery).
func (b *Broker) SetThrottlePaused(paused bool) {
	b.throttlePaused.Store(paused)
}

// ThrottlePaused reports whether the broker is paused.
func (b *Broker) ThrottlePaused() bool {
	return b.throttlePaused.Load()
}

// waitForThrottle blocks based on pause state and current throttle delay.
func (b *Broker) waitForThrottle() {
	for b.throttlePaused.Load() {
		time.Sleep(pausePollDelay)
	}
	delayLevel := b.throttleDelay.Load()
	if delayLevel <= 0 {
		return
	}
	time.Sleep(time.Duration(delayLevel) * throttleStep)
}
