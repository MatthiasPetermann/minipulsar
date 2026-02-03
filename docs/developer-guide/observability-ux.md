# Observability & operator UX

Minipulsar includes observability features for operators and developers: a
Prometheus metrics endpoint, structured logging, and an optional terminal UI.

## Structured logging

`internal/logging` wraps the standard `slog` API and standardizes log output.
It supports:

- Text or JSON formatting.
- Optional timestamps.
- Consistent severity levels.

The broker and supporting subsystems receive a shared logger instance, often
annotated with `component` fields for easier filtering.

## Prometheus metrics

The metrics server (`internal/metrics`) exposes broker and storage stats at a
configurable HTTP endpoint.

### Collection model

- A background goroutine polls `broker.StatsSnapshot` at a fixed interval.
- The snapshot includes producer/consumer counts plus storage metrics.
- Errors are counted in `minipulsar_metrics_scrape_errors_total`.

### Metrics emitted

The server emits gauges for:

- Connected producers and consumers.
- Known namespaces, topics, subscriptions.
- Stored messages and pending delivery counts.
- Per-topic message and pending counts (top topics only).
- Last scrape timestamp and duration.

### Configuration

The CLI flags `-metrics-addr`, `-metrics-path`, `-metrics-interval`, and
`-metrics-top-topics` control the endpoint and sampling behavior.

## TUI dashboard

The optional TUI (`internal/tui`) uses Bubble Tea and Lip Gloss to render:

- Overview stats (producer/consumer counts, pending, etc.).
- Top topics by pending messages.
- Streaming logs collected from the logging subsystem.

The TUI is enabled with the `-tui` flag. Log lines are routed into the TUI using
`LogWriter`, which implements `io.Writer` and streams complete log lines into a
channel that the UI consumes.
