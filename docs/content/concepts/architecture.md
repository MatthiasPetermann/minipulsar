---
title: "Architecture"
weight: 21
---

## Motivation and scope

Apache Pulsar normally couples protocol brokers, metadata services, BookKeeper,
and cluster coordination. minipulsar intentionally collapses that architecture
into a single Go process and SQLite database. The trade-off is explicit: it
provides a small operational footprint and familiar client protocol, not
horizontal scale or replicated durability.

## Runtime layers

```mermaid
flowchart LR
    Client[Pulsar client] --> Listener[TCP or TLS listener]
    Listener --> Broker[Broker session and command handlers]
    Broker --> Storage[SQLite storage]
    Broker --> Protocol[Protocol frame writer]
    Broker --> Messaging[Optional HCL / JWT / Lua runtime]
    Broker --> Metrics[Prometheus snapshot exporter]
    Broker --> TUI[Optional Bubble Tea control deck]
    Protocol --> Client
```

- `cmd/minipulsar` assembles runtime dependencies from CLI flags.
- `internal/broker` owns connection state, command dispatch, subscriptions,
  delivery, and lifecycle.
- `internal/storage` owns durable topic, cursor, pending, and cleanup state.
- `internal/protocol` serializes Pulsar-compatible command and message frames.
- `internal/messaging` is optional policy and transformation control plane.
- `internal/metrics`, `internal/logging`, and `internal/tui` are operator-facing
  adapters and do not own messaging state.

## Startup and shutdown

```mermaid
sequenceDiagram
    participant OS as OS / operator
    participant CLI as cmd/minipulsar
    participant DB as SQLite
    participant B as Broker
    participant M as Metrics server
    OS->>CLI: start with flags
    CLI->>DB: Open, enable foreign keys, initialise schema
    CLI->>B: New(store, config)
    CLI->>M: Start optional exporter
    CLI->>B: Start plain and optional TLS listeners
    OS->>CLI: SIGINT or SIGTERM
    CLI->>M: Stop HTTP server and collector
    CLI->>B: Shutdown(context)
    B->>B: Close listeners and active connections
    CLI->>DB: Close database handle
```

The broker tracks listeners and active connections. Shutdown cancels background
maintenance, closes network resources, and waits for tracked worker activity up
to the caller's context deadline.

## Concurrency boundaries

- One goroutine reads commands for each client connection.
- Outbound frames are serialized with one mutex per connection so control frames
  and delivered messages never interleave.
- At most one persistent delivery loop runs for a `(topic, subscription)` state.
- SQLite transactions protect claim, cursor, pending, acknowledgement, reset,
  and redelivery changes.
- The TUI and Prometheus exporter consume snapshots; neither mutates durable
  data except through explicit broker throttle controls.
