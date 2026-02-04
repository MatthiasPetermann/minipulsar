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
- Per-subscription backlog counts (top subscriptions only).
- Last scrape timestamp and duration.

### Metric definitions and SQL

The metrics endpoint is a point-in-time snapshot. Collection happens on a timer,
so values can lag the real-time state by up to the scrape interval. The snapshot
is constructed by `broker.StatsSnapshot`, which combines broker runtime counters
with storage SQL queries. The storage queries below are run against SQLite.

#### Broker gauges

- `minipulsar_broker_producers`: Number of active producer connections.
  - Source: in-memory broker map length (no SQL).
- `minipulsar_broker_consumers`: Number of active consumer connections.
  - Source: in-memory broker map length (no SQL).

#### Storage gauges (global)

- `minipulsar_storage_namespaces`: Number of namespaces known to storage.
  - SQL:
    ```
    SELECT COUNT(*) FROM namespaces;
    ```
- `minipulsar_storage_topics`: Number of topics known to storage.
  - SQL:
    ```
    SELECT COUNT(*) FROM topics;
    ```
- `minipulsar_storage_subscriptions`: Number of subscriptions known to storage.
  - SQL:
    ```
    SELECT COUNT(*) FROM subscriptions;
    ```
- `minipulsar_storage_pending_messages`: Pending (delivered, unacked) messages
  across all subscriptions.
  - SQL:
    ```
    SELECT COUNT(*) FROM subscription_pending;
    ```
- `minipulsar_storage_stored_messages`: Stored messages across all topics.
  - SQL:
    ```
    SELECT COUNT(*) FROM messages;
    ```

#### Per-topic gauges (top topics only)

These two gauges are emitted for the top topics (ordered by pending desc, then
message count desc). Topics beyond the configured `-metrics-top-topics` limit are
not exported.

- `minipulsar_storage_topic_messages{topic=...}`: Stored messages per topic.
- `minipulsar_storage_topic_pending_messages{topic=...}`: Pending messages per
  topic.

The topic snapshot is computed via:

```
SELECT t.full_name,
       COALESCE(m.message_count, 0) AS message_count,
       COALESCE(p.pending_count, 0) AS pending_count
  FROM topics t
  LEFT JOIN (
    SELECT topic_id, COUNT(*) AS message_count
      FROM messages
     GROUP BY topic_id
  ) m ON m.topic_id = t.id
  LEFT JOIN (
    SELECT topic_id, COUNT(*) AS pending_count
      FROM subscription_pending
     GROUP BY topic_id
  ) p ON p.topic_id = t.id
 ORDER BY pending_count DESC, message_count DESC, t.full_name
 LIMIT ?;
```

#### Per-subscription backlog gauge (top subscriptions only)

- `minipulsar_storage_subscription_backlog_messages{topic=...,subscription=...}`
  - Definition: **undelivered backlog per subscription**. This is the sum of:
    - `pending_count`: delivered-but-unacked rows in `subscription_pending`
    - plus the count of messages at or after the subscription cursor that are
      not already pending
  - Important: This is **not** a total of stored messages. It is derived from
    the subscription cursor and pending state.

SQL (scoped by tenant/namespace):

```
SELECT t.full_name,
       s.name,
       COALESCE(p.pending_count, 0)
         + COALESCE(SUM(CASE WHEN sp.message_id IS NULL THEN 1 ELSE 0 END), 0)
         AS backlog_count
  FROM subscriptions s
  JOIN topics t ON t.id = s.topic_id
  JOIN namespaces n ON n.id = t.namespace_id
  JOIN subscription_cursor c
    ON c.topic_id = s.topic_id AND c.name = s.name
  LEFT JOIN (
    SELECT topic_id, name, COUNT(*) AS pending_count
      FROM subscription_pending
     GROUP BY topic_id, name
  ) p ON p.topic_id = s.topic_id AND p.name = s.name
  LEFT JOIN messages m
    ON m.topic_id = t.id AND m.id >= c.next_message_id
  LEFT JOIN subscription_pending sp
    ON sp.topic_id = s.topic_id
   AND sp.name = s.name
   AND sp.message_id = m.id
 WHERE n.tenant = ? AND n.name = ?
 GROUP BY t.full_name, s.name, p.pending_count
 ORDER BY backlog_count DESC, t.full_name, s.name
 LIMIT ?;
```

If `messages` is empty and `subscription_pending` is empty, this query yields a
backlog count of `0`, even if the subscription cursor exists. A non-zero backlog
in that case implies other subscriptions or topics are contributing to the
top-N list, or that the metrics snapshot is stale.

#### Metrics scrape health

- `minipulsar_metrics_scrape_errors_total`: Number of errors while collecting
  metrics. Incremented when `broker.StatsSnapshot` fails.
- `minipulsar_metrics_last_scrape_timestamp_seconds`: Unix timestamp for the
  most recent successful snapshot.
- `minipulsar_metrics_scrape_duration_seconds`: Wall time for the last snapshot
  collection.

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
