# Production Readiness Roadmap

## Current State

minipulsar is a compact, single-node Pulsar-compatible broker for edge and prototyping use cases. Its architecture separates the broker, SQLite storage, wire protocol, and messaging control plane well. It is intentionally a proof of concept and is not production ready.

## Strengths

- Persistent and non-persistent topics are available.
- Persistent subscriptions use cursors, pending-delivery tracking, acknowledgements, and redelivery after a disconnect or acknowledgement timeout.
- The Pulsar binary protocol supports the core producer and consumer flows.
- Frame and message limits, read/write timeouts, and CRC validation are present. TLS and HS256 JWT validation are optional deployment features.
- Unit tests, `go test -race ./...`, and `go vet ./...` pass.

## Risks and Gaps

- Unknown Pulsar commands are logged but do not receive an error response. Clients can wait indefinitely for a response.
- There is no controlled shutdown for listeners, connections, background tickers, metrics, and SQLite.
- Producer sequence IDs are stored but not used for deduplication. Retries can therefore create duplicates.
- Limits for connections, producers, consumers, subscriptions, backlog, and memory use are missing.
- SQLite foreign keys are not explicitly enabled.
- Schema migrations are not versioned, and there is no backup, restore, or integrity-check strategy.
- There are no end-to-end tests using official Pulsar clients or a real Pulsar broker as a reference.

## Subscription Start Position

New persistent subscriptions can set an initial position through `SUBSCRIBE.initialPosition`:

- `Earliest` starts at the first stored message.
- `Latest` starts after the newest message stored when the subscription is created.
- If omitted, the broker uses `Latest`.

An existing subscription is not repositioned, which is correct because its cursor is already persisted.

Targeted positioning by `MessageId` or timestamp is not supported. At minimum, this requires `SEEK` support.

## Performance Bottlenecks

- Despite WAL mode, SQLite has one effective writer. Each persistent publish uses its own transaction and several SQL operations.
- Delivery claims read the cursor, check pending records, and write one pending record per message.
- Acknowledgements delete records individually rather than in batches.
- Retention, pending-expiry, and metrics use costly aggregation and anti-join queries as data grows.
- Suitable indexes for `messages.publish_time` and `subscription_pending.delivered_at` are missing.
- Lua bindings run synchronously in the publish path and increase producer latency.
- Successful sends are logged at info level, causing avoidable I/O overhead under load.
- Shared subscriptions distribute batches rather than messages fairly; one consumer can receive up to 200 messages before another consumer is selected.

## Pulsar Compatibility Gaps

- Partitioned topics are absent; partition metadata always reports zero partitions.
- `Key_Shared` is rejected.
- `SEEK`, negative acknowledgements, explicit redelivery, and last-message-ID support are absent.
- Dead-letter and retry topics are absent.
- Batching, compression, chunking, and transactions are absent.
- Schemas are intentionally out of scope: minipulsar treats message payloads as opaque bytes.
- Producer deduplication is absent.
- There is no Pulsar admin API or Pulsar-style tenant, cluster, or topic administration.
- Discovery, load balancing, replication, and cluster recovery are absent.
- TLS and HS256 JWT authentication are optional and intentionally limited deployment features.
- Non-persistent topics are broker-local and lose messages when no consumer is connected.

## Backend Strategy

Adding a backend should not be the next step. Storage and delivery semantics must first be stable and covered by contract tests.

1. Define a small storage interface with contract tests for cursors, claims, acknowledgements, disconnects, redelivery, and retention.
2. Keep SQLite as the optimized edge and single-node backend.
3. Evaluate PostgreSQL only if multi-process operation, larger data volumes, or centralized deployments become concrete requirements.
4. Do not treat another SQL backend as a replacement for cluster semantics. Partitioning, replication, and broker ownership are separate broker features.

## Roadmap

### P0: Correctness and Operations

- Implement context-based startup and shutdown.
- Gracefully stop listeners, active connections, background jobs, metrics, and SQLite.
- Handle unknown and invalid commands with Pulsar error responses.
- Add limits for connections, producers, consumers, subscriptions, backlog, and payloads.
- Preserve optional TLS and authentication modes, and add rate limiting.
- Run SQLite with `foreign_keys=on`.
- Introduce versioned, transactional schema migrations.
- Document and test backup, restore, and integrity checks.
- Implement producer deduplication or explicitly document at-least-once delivery semantics.

### P1: Testing Foundation

- Write socket-level integration tests for connect, produce, subscribe, flow, acknowledgement, disconnect, timeout, and restart scenarios.
- Test against the official Go, Java, and Python Pulsar clients.
- Add differential tests against real Apache Pulsar and document intentional deviations in a compatibility matrix.
- Add crash and restart tests at commit boundaries.
- Add property tests for cursor and pending-delivery invariants, plus frame fuzzing.
- Build CI with `go test ./...`, the race detector, vet, fuzz smoke tests, coverage gates, benchmarks, and dependency/container scans.

### P2: Performance and Observability

- Define benchmarks for publishing, shared delivery, acknowledgements, backlog, and retention with realistic payloads.
- Batch inserts and acknowledgements, reuse statements, and reduce topic lookups.
- Add indexes for `messages(topic_id, publish_time)` and pending-delivery expiry.
- Track metrics incrementally or strictly bound expensive queries.
- Move successful-send logging to debug level or sample it.
- Make Lua bindings asynchronous and define explicit retry and failure semantics.
- Run load tests and publish tested SQLite sizing limits.

### P3: Pulsar Compatibility

- Implement `SEEK`, `REDELIVER_UNACKNOWLEDGED_MESSAGES`, negative acknowledgements, and last-message-ID support.
- Add delivery counts, dead-letter topics, and retry topics.
- Implement `Key_Shared`.
- Implement partitioned topics.
- Support message batches, compression, and producer idempotency where they preserve opaque byte payloads.
- Publish and maintain a compatibility matrix by client and feature.

### P4: Scale

- Stabilize the storage contract and only then evaluate PostgreSQL as a second backend.
- Plan clustering, replication, and ownership semantics as a separate initiative.
- Continue to use Apache Pulsar in production where horizontal scaling and full Pulsar semantics are required.

## Release Criterion

P0 and P1 are minimum requirements before minipulsar should be described as a production single-node edge broker. Additional backends and broad Pulsar feature work should start only after those requirements are met.
