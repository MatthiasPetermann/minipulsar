package broker

// Snapshot returns a point-in-time view of broker counters for display.
// It is safe to call from monitoring or UI goroutines.
func (b *Broker) Snapshot() Stats {
	return b.stats.snapshot()
}
