# Broker core (connections, commands, delivery)

The broker is responsible for handling Pulsar client connections, decoding
frames, authorizing access, storing messages, and delivering them to consumers.
It is intentionally simple but models core Pulsar semantics such as shared
subscriptions and message permits.

## Runtime state

The broker struct tracks:

- **Producers and consumers** keyed by `(connection, id)` to avoid collisions
  across separate TCP connections.
- **Subscription state (`subState`)** per `(topic, subscription)` pair for
  shared subscription delivery and round-robin selection.
- **Non-persistent sequence counters** to synthesize entry IDs without storage.
- **Connection roles** parsed from JWT tokens when messaging security is enabled.

See `internal/broker/types.go` for the configuration and state layout.

## Connection lifecycle

1. `ServeWithTLS` accepts TCP/TLS connections and spawns a goroutine per client.
2. `handleConnection` loops over `handleFrame` and cleans up producers/consumers
   when the connection closes.
3. `cleanupConnection` removes producers, consumers, connection roles, and
   pending messages for any consumer bound to the connection.

Per-connection writes are serialized with a mutex stored in `connWrite` to avoid
interleaving frames across goroutines.

## Frame dispatch

`handleFrame` performs length-prefixed framing, enforces maximum sizes, and
unmarshals `pulsar.BaseCommand`. It dispatches to handlers based on command type.
Supported commands include `CONNECT`, `PRODUCER`, `SEND`, `SUBSCRIBE`, `FLOW`,
`ACK`, `LOOKUP`, `PARTITIONED_METADATA`, and ping/close commands.

## Command handling

### CONNECT
- Records roles extracted from JWTs when messaging security is enabled.
- Responds with `CONNECTED` including server version and max message size.

### PRODUCER
- Validates the topic name, checks authorization if enabled, and registers the
  producer in the broker map.
- Responds with `PRODUCER_SUCCESS`.

### SUBSCRIBE
- Validates the topic, ensures storage subscription state for persistent topics,
  and registers the consumer with a unique server-side UID (used for pending
  delivery tracking).
- Shared subscription semantics are implemented with round-robin consumer
  selection.

### SEND
- Parses the message metadata frame (magic header + checksum + metadata).
- Validates checksum and message size limits.
- Writes persistent messages to SQLite or assigns a synthetic ID for
  non-persistent messages.
- Triggers delivery for persistent subscriptions and inline delivery for
  non-persistent subscriptions.
- Executes optional Lua bindings to transform and forward payloads.

### FLOW
- Increases consumer permits and triggers delivery for persistent subscriptions
  when permits become available.

### ACK
- Supports **individual ack** for all subscription types.
- Supports **cumulative ack** for Exclusive and Failover subscriptions; cumulative
  ack is ignored in Shared mode to preserve correctness.
- Acknowledgement removes pending rows for the consumer UID and message IDs.

### Ack timeout + redelivery

When an ack timeout is configured, the broker periodically scans pending
messages. Expired pending entries are cleared and the subscription cursor is
rewound so the messages can be redelivered.

### CLOSE_PRODUCER / CLOSE_CONSUMER
- Remove producer/consumer state and clear pending messages for the consumer.

## Delivery mechanics

Minipulsar implements shared subscriptions with **permit-based flow control**:

- Each consumer tracks a permit counter.
- A delivery loop (`deliveryLoopShared`) runs per subscription if any consumer
  has permits and claims messages in batches from storage.
- Claimed messages are recorded in the pending table, preventing other consumers
  from receiving them.
- The broker decrements permits as it successfully sends messages.

For **non-persistent topics**, messages are delivered directly to one available
consumer without persistence or pending tracking.

## Authorization

Authorization is driven by the messaging runtime. If a messaging config is
loaded and security policies are defined, the broker:

1. Extracts roles from JWTs during `CONNECT`.
2. Converts a topic name into a namespace key (`persistent://tenant/namespace`).
3. Evaluates whether the requested action (`produce` or `consume`) is allowed.

If security is not configured, all actions are permitted.

## Namespace maintenance

When namespace policies are configured, a background ticker periodically:

- Drops subscriptions that have not been served recently.
- Prunes consumed messages that are older than the retention window.
- Removes orphaned messages for topics with no subscriptions.
- Deletes empty topics that have no messages, subscriptions, or active producers
  or consumers.

Active topics (including those referenced by Lua bindings) are excluded from
empty-topic cleanup.
