---
title: "Codebase reference"
weight: 42
---

This is the file-level maintenance map for the current repository. Generated
code is documented by source and purpose rather than every generated symbol.
All package paths are relative to the repository root.

## Entrypoint and repository assets

### `cmd/minipulsar/main.go`

Owns process assembly. It parses all CLI flags, creates the logger and SQLite
store, loads optional HCL/Lua runtime and TLS configuration, constructs the
broker, starts metrics and listeners, and coordinates `SIGINT`/`SIGTERM`
shutdown. Its helpers isolate runtime concerns: `buildMessagingRuntime`,
`startMetricsServer`, `stopRuntime`, `startBrokerListeners`, `parseLogLevel`,
and `buildTLSConfig`.

### `cmd/minipulsar/main_test.go`

Tests CLI-local pure helpers, especially accepted log-level values and the
requirement that TLS certificate and key are configured as a pair.

### `go.mod` and `go.sum`

Define module `minipulsar`, Go version, direct dependencies, and immutable
dependency checksums. Direct dependencies include protobuf, SQLite, HCL,
GopherLua, and the Bubble Tea stack.

### `Makefile`

Provides the canonical `generate`, `build`, `test`, and `clean` commands.
`generate` is the only supported way to refresh generated Pulsar bindings.

### `pb/PulsarApi.proto`

The protocol source of truth. It defines Pulsar commands, metadata, IDs, error
codes, subscription fields, and the `BaseCommand` envelope consumed by broker
framing.

### `pb/PulsarApi.pb.go`

Generated Go protobuf bindings. Do not edit it. Regenerate from the `.proto`
file with `make generate`.

### `examples/messaging.hcl`, `examples/transform.lua`, `examples/README.md`

Provide an executable HCL policy/binding example, the Lua `handle` transform it
loads, and concise instructions for trying the control-plane feature.

## Broker package: state and lifecycle

### `internal/broker/types.go`

Defines `Config`, `Broker`, and all internal session models. `Config` is the
runtime contract for size/time/resource limits, optional TLS/JWT/messaging, and
maintenance timers. `Broker` holds maps keyed by connection, subscription
states, lifecycle cancellation/waiting structures, per-connection write locks,
throughput counters, and operator throttle state. `New` applies defaults,
allocates maps, and starts configured background monitors.

### `internal/broker/server.go`

Implements plain/TLS listening, connection admission limits, the one-reader
connection loop, serialized writes, and `Shutdown(context)`. It is the owner of
network resource tracking: shutdown cancels lifecycle work, closes listeners and
connections, then waits for tracked broker workers.

### `internal/broker/framing.go`

Reads the Pulsar outer length prefix, enforces `MaxFrameSize`, splits command and
payload sections, unmarshals `BaseCommand`, and dispatches handlers. It converts
rejected commands into `ERROR` or `SEND_ERROR` responses instead of dropping an
otherwise healthy session. Malformed frame reads remain connection errors.

### `internal/broker/handlers.go`

Contains client-visible command behavior: connection handshake, lookup and
partition metadata, producer registration, subscription creation and start
positions, checksum-validated sends, flow permits, ACKs, seek, redelivery,
last-message-ID, ping, and close operations. It is deliberately orchestration:
durable mutations belong to storage and message-frame output belongs to protocol.

### `internal/broker/state.go`

Creates or validates the in-memory `subState` for a topic/subscription pair.
It prevents a runtime subscription type from silently disagreeing with an
already-attached state.

### `internal/broker/publish.go`

Routes a normalized message to persistent SQLite insertion or ephemeral delivery.
Every persistent publish is stored before matching subscription delivery loops
are woken; retention policy is applied later by maintenance.

### `internal/broker/delivery.go`

Owns permit-gated persistent dispatch. One loop per subscription selects an
eligible consumer, asks storage to atomically claim a bounded batch, writes
message frames, and decrements permits. Shared rotates consumers; Exclusive uses
the first; Failover selects highest priority.

### `internal/broker/non_persistent.go`

Implements best-effort in-memory dispatch. It selects an eligible consumer and
writes directly; messages with no receiver disappear by design.

### `internal/broker/subscription_types.go`

Maps protobuf subscription enums to storage values. It is the explicit feature
gate that rejects `Key_Shared` rather than pretending to provide key ordering.

### `internal/broker/producer_access.go`

Implements local-process producer access arbitration. It detects active
producers, manages wait conditions, and fences prior producers for the fencing
mode. It has no distributed ownership semantics.

### `internal/broker/cleanup.go`

Removes a consumer from broker maps and subscription state, drops its durable
pending rows, rewinds storage through the storage API, and restarts remaining
delivery when appropriate.

### `internal/broker/ack_timeout.go`

Starts the optional periodic ack-timeout monitor. It expires old pending rows
through storage and restarts delivery for affected in-memory subscriptions.

### `internal/broker/maintenance.go`

Runs namespace-policy maintenance on the broker lifecycle context. It refreshes
active subscriptions, prunes stale subscriptions and retained/orphaned messages,
and excludes active producer/consumer/binding topics from empty-topic deletion.

### `internal/broker/authorization.go`

Converts a topic to its namespace boundary, retrieves connection roles, and asks
the optional messaging security IR whether a produce or consume action is
allowed. Without policy runtime, access is allowed.

### `internal/broker/auth.go`

Parses CONNECT auth bytes, normalizes optional `Bearer` form, verifies HS256 JWT
signatures and expiration, and extracts a single `role` or list of `roles`.
Unsupported algorithms and malformed tokens fail authentication parsing.

### `internal/broker/throttle.go`

Exposes TUI-controlled global pause and fixed delay levels. Publish and delivery
paths call the throttle wait helper; it is a diagnostic control, not a QoS
system.

### `internal/broker/stats.go`

Defines broker `StatsSnapshot` and merges in-memory producer/consumer counts,
heap allocation, throughput rate, storage totals, top topics, and policy-scoped
subscription backlog.

### `internal/broker/broker_test.go` and `broker_extra_test.go`

Cover broker defaults, stats composition, authorization/access helpers,
subscription type consistency, non-persistent behavior, and related local
invariants shared across the broker package.

### `internal/broker/socket_integration_test.go`

The highest-level broker test. It opens real TCP listeners and proves CONNECT /
PING, protocol errors, graceful shutdown, producer-send-consumer-flow-ACK,
explicit start position, last-message-ID, redelivery, and seek behavior.

## Storage package

### `internal/storage/storage.go`

Defines the SQLite-backed `Store`, `Message`, initial-position and subscription
type values, connection setup, and schema installation. It enables WAL, busy
timeout, and foreign keys; creates namespaces/topics/messages/subscriptions,
cursors/pending rows/migration metadata, and critical indexes. It also owns
subscription creation, timestamp and last-ID lookups, cursor reset, pending
redelivery, durable message insertion, individual/cumulative ACK removal,
disconnect cleanup, and atomic batch claims. This file is the authoritative
place for cursor and pending invariants.

### `internal/storage/subscriptions.go`

Defines `SubscriptionRef` and `TouchSubscriptions`. Maintenance uses it to
refresh `last_consumer_at` for subscriptions with connected consumers so a
stale-subscription policy does not remove active work.

### `internal/storage/cleanup.go`

Implements durable maintenance queries: stale subscription deletion, orphaned
and consumed message retention, empty topic deletion, repair of orphaned cursor
and pending records, and ack-timeout expiry. Pending expiry identifies the
lowest expired entry per subscription, removes pending rows, and rewinds the
dispatch cursor for replay.

### `internal/storage/stats.go`

Defines storage-level topic/backlog snapshot structs and aggregation queries.
`StatsSnapshot` returns global table counts plus top topic pressure;
`SubscriptionBacklogStats` calculates pending plus not-yet-claimed work per
subscription in a namespace.

### `internal/storage/storage_test.go`

Uses a temporary real SQLite database to cover initial positions, durable insert
rejection for non-persistent topics, claim/ACK behavior, disconnect redelivery,
subscription existence, and aggregate statistics.

### `internal/storage/storage_extra_test.go`

Exercises edge cases and maintenance: cursor advancement/skips, ownership-safe
ACKs, cumulative ACKs, expiry, cleanup, backlog, schema repair, and retention.
It is the regression suite for subtle SQL state transitions.

## Protocol and topic packages

### `internal/protocol/wire.go`

Writes Pulsar control and message frames. `WriteSimpleCommand` emits the outer
and command lengths. `WriteMessageFrame` builds `MESSAGE`, marshals metadata,
calculates CRC32C over metadata size/metadata/payload, prepends Pulsar magic
`0x0e01`, and writes the resulting outer frame. Stored SQLite IDs become ledger
`0` entry IDs.

### `internal/protocol/properties.go`

Translates between Go `map[string]string` and protobuf `KeyValue` slices.
Sorting when producing key-value slices makes outgoing metadata deterministic.

### `internal/protocol/wire_test.go`

Validates control-frame and message-frame serialization, lengths, metadata,
payload, and checksum-compatible structure.

### `internal/topic/topic.go`

Defines normalized `Info` and `Parse`. It accepts qualified persistent and
non-persistent Pulsar names, expands shorthand names to the public/default
namespace, and gives every layer a canonical full topic name plus tenant,
namespace, name, and durability flag.

### `internal/topic/topic_test.go`

Proves accepted qualified/shorthand forms, normalization, durability detection,
and invalid-name rejection.

## Messaging control-plane package

### `internal/messaging/config.go`

Defines the HCL decode model for security mode, namespace policies, functions,
and bindings. `LoadConfig` reads and validates the source configuration shape;
runtime-level semantic validation occurs later.

### `internal/messaging/security_ir.go`

Compiles configuration into a normalized security intermediate representation.
It defines produce/consume actions, open/strict modes, namespace allowlists, and
the `Allows` decision method used by the broker.

### `internal/messaging/runtime.go`

Turns decoded HCL into executable runtime state: namespace retention and timeout
policies, validated function specifications, normalized topic bindings, security
IR, and an optional Lua worker pool. It provides topic-policy and binding lookup
without reparsing configuration on every publish.

### `internal/messaging/lua_pool.go`

Implements the bounded Lua execution pool. Workers load scripts once, expose a
restricted standard-library set, call `handle(payload, context)`, enforce a
configured runtime limit, and return transformed bytes. Broker send handling
uses the pool synchronously for each matching binding.

### `internal/messaging/config_test.go`

Tests HCL parsing, required fields, and configuration validation errors.

### `internal/messaging/security_ir_test.go`

Tests strict/open policy compilation and role/action decisions.

### `internal/messaging/runtime_test.go`

Tests runtime construction, namespace policy lookup, function registry
validation, and binding normalization.

### `internal/messaging/lua_pool_test.go`

Tests worker execution, transform outputs, and validation failures in the Lua
adapter. Runtime saturation and timeout behavior remain integration-test gaps.

## Observability packages

### `internal/logging/logging.go`

Wraps `log/slog` with text/JSON selection, timestamp options, contextual child
loggers, dynamic level variables, and concise Debug/Info/Warn/Error methods.
Every runtime layer receives this wrapper instead of constructing its own logger.

### `internal/logging/logging_test.go`

Tests logger construction, formatting, contextual fields, and dynamic level
behavior.

### `internal/metrics/metrics.go`

Defines the Prometheus `Server`. It periodically calls broker snapshots, copies
top-dimensional data under a mutex, renders Prometheus text exposition, and
stops its collector and HTTP server cleanly. Snapshotting rather than querying
on each scrape bounds concurrent database work.

### `internal/metrics/metrics_test.go`

Tests metric server defaults and rendered Prometheus output using HTTP test
facilities and controlled snapshots.

### `internal/tui/tui.go`

Defines the Bubble Tea control deck. The model periodically fetches broker
snapshots and log lines, tracks trend history, renders Overview/Topics/Backlog/
Logs views, handles keyboard controls, and exposes pause/delay/log-level actions.
Its full-terminal canvas reapplies the base background after nested ANSI resets
so panel switches do not leave unpainted terminal areas.

### `internal/tui/log_hook.go`

Defines line-buffering `LogWriter`, an `io.Writer` bridge from structured logger
output to the TUI channel. It buffers partial writes and emits complete newline-
delimited log lines safely.

### `internal/tui/tui_test.go`

Tests navigation wraparound, selection clamping, data rendering, bounded trend
history, and exact terminal dimensions across all views after a panel switch.
