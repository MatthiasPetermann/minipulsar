---
title: "Quick start"
weight: 11
---

# Quick start

## Build and run

```bash
make build
./bin/minipulsar -addr :6650 -db ./minipulsar.db
```

The broker advertises `pulsar://localhost:6650` by default. Override the
advertised address with `-broker-url` when clients connect through a different
host name, container network, or load balancer.

## What happens on the wire

```mermaid
sequenceDiagram
    participant C as Pulsar client
    participant B as minipulsar broker
    participant S as SQLite store
    C->>B: CONNECT(client version, protocol version)
    B-->>C: CONNECTED(server version, max message size)
    C->>B: PRODUCER(topic, producer id)
    B-->>C: PRODUCER_SUCCESS
    C->>B: SEND(metadata, opaque payload bytes)
    B->>S: INSERT message in transaction
    S-->>B: SQLite entry id
    B-->>C: SEND_RECEIPT(entry id)
```

## First subscription

A consumer creates a durable subscription on a persistent topic, then grants
permits with `FLOW`. Delivery begins only when permits are available.

```mermaid
sequenceDiagram
    participant C as Consumer
    participant B as Broker
    participant S as SQLite
    C->>B: SUBSCRIBE(topic, subscription, Earliest)
    B->>S: Create subscription and cursor
    B-->>C: SUCCESS
    C->>B: FLOW(permits=10)
    B->>S: Claim messages and create pending rows
    B-->>C: MESSAGE x N
    C->>B: ACK(message ids)
    B->>S: Delete owned pending rows
```

Use `Earliest` to consume retained history for a **new** subscription and
`Latest` to start after the newest current entry. An existing subscription keeps
its persisted cursor regardless of a later subscribe request.

## Persistent versus non-persistent

`persistent://tenant/namespace/topic` is stored in SQLite. A
`non-persistent://...` topic is delivered only to currently connected consumers;
there is no disk log, pending state, or replay after a disconnect.

## Next steps

- Configure limits, shutdown, and metrics in [Configuration and lifecycle](../../operations/configuration/).
- Learn delivery guarantees in [Messaging semantics](../../concepts/messaging-semantics/).
