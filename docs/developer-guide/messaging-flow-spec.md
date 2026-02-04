# Messaging flow specification (end-to-end)

This document describes the end-to-end messaging flow in minipulsar, starting
from client connection through message production, storage, delivery, and
acknowledgement. It complements the component-level docs with a system-level
specification of how commands and state transitions interact.

## Scope and goals

- Cover the **full lifecycle** of a message for both persistent and
  non-persistent topics.
- Highlight **authorization**, **storage**, **delivery**, and **ack** behavior.
- Show how **bindings** and **Lua functions** fit into the data path.
- Provide a deterministic, implementation-aligned view of the runtime.

## Actors and components

```
+--------------------+        +----------------------+        +-------------------+
| Pulsar Client(s)   |        | Minipulsar Broker    |        | SQLite Storage    |
| (producer/consumer)| <----> | (internal/broker)    | <----> | (internal/storage)|
+--------------------+        +----------------------+        +-------------------+
            |                             |
            |                             v
            |                  +-----------------------+
            |                  | Messaging Runtime     |
            |                  | (HCL + Lua bindings)  |
            |                  +-----------------------+
```

## Connection and session setup

1. Client opens a TCP/TLS connection and sends `CONNECT`.
2. Broker parses roles from JWT (if messaging security is configured) and
   replies with `CONNECTED`.
3. Producer and consumer sessions are established on the same connection via
   `PRODUCER` and `SUBSCRIBE` commands.

```
Client                        Broker
  | ---- CONNECT -----------> |
  | <--- CONNECTED ---------- |
  | ---- PRODUCER ----------> |
  | <--- PRODUCER_SUCCESS --- |
  | ---- SUBSCRIBE ---------> |
  | <--- SUCCESS ------------ |
```

## Producer flow (persistent topic)

### Command sequence

```
Producer                     Broker                     Storage
   | ---- SEND --------------> |
   |                           | -- insert message -----> |
   |                           | <- row id / entry id --- |
   | <--- SEND_RECEIPT ------- |
   |                           | -- trigger delivery ---> |
```

### Behavioral spec

1. **Frame parsing**: `SEND` includes a command frame plus a message metadata
   frame (magic header + checksum + metadata + payload).
2. **Validation**:
   - Check checksum and max message size.
   - Check authorization for `produce` if security is enabled.
3. **Storage write**:
   - For persistent topics, write the message to SQLite.
   - Use the row ID as the Pulsar entry ID.
4. **Receipt**: Reply with `SEND_RECEIPT` once the message is persisted.
5. **Delivery trigger**: Notify subscription delivery loops that new data is
   available.

## Producer flow (non-persistent topic)

```
Producer                     Broker                     Consumer
   | ---- SEND --------------> |
   |                           | -- choose consumer ---> |
   |                           | ---- MESSAGE --------> |
   | <--- SEND_RECEIPT ------- |
```

- No storage write occurs.
- The broker assigns a synthetic entry ID for ordering.
- If no consumer is available, the message is dropped (best-effort delivery).

## Consumer flow (persistent topic)

### Subscription lifecycle

1. **SUBSCRIBE** registers a consumer and (if needed) creates or loads the
   subscription state for `(topic, subscription)`.
2. **FLOW** grants permits to enable delivery.
3. A shared subscription delivery loop claims messages in batches and sends them
   to one available consumer at a time.

### Delivery and pending tracking

```
Broker                        Storage
  | -- claim batch ----------> |
  | <- message rows --------- |
  | -- insert pending --------> |
  | -- MESSAGE to consumer --> |
```

- Claimed messages are recorded in the pending table to prevent duplicate
  delivery across consumers.
- The broker decrements permits as messages are sent.
- Pending rows are inserted **before** a message is written to the consumer
  connection. This means a message can appear as pending even if delivery is
  delayed by throttling or interrupted by a disconnect. Pending entries are
  cleared on ACK or when the owning consumer disconnects.

### ACK flow

```
Consumer                     Broker                     Storage
   | ---- ACK --------------> |
   |                           | -- delete pending ----> |
```

- **Individual** ACKs are supported for all subscription types.
- **Cumulative** ACKs are supported for Exclusive and Failover subscriptions
  and ignored for Shared subscriptions.
- Acknowledging removes the pending record for the consumer UID + message ID.

### Ack timeout redelivery

When an ack timeout is configured, a background sweep clears expired pending
entries and rewinds the subscription cursor so the messages can be claimed and
redelivered.

## Consumer flow (non-persistent topic)

- `SUBSCRIBE` registers the consumer in-memory without storage state.
- `FLOW` grants permits.
- Each incoming message is delivered inline to one consumer with available
  permits.
- No pending table or ACK tracking is used; ACKs are accepted but have no
  storage effect.

## Shared subscription semantics

```
+-------------------+   round-robin   +-------------------+
| Consumer A        | <-------------> | Consumer B        |
| permits=10        |                 | permits=5         |
+-------------------+                 +-------------------+
           ^                                     ^
           |                                     |
           +----------- delivery loop -----------+
```

- The delivery loop selects consumers in round-robin order.
- Permits gate how many messages each consumer can receive before another `FLOW`.

## Authorization gates

Authorization is enforced for `produce` and `consume` actions when messaging
security is configured.

```
Client Roles + Topic
         |
         v
Namespace policy lookup
         |
         v
Allow / Deny
```

- Topics map to namespace strings (`persistent://tenant/namespace`).
- **Strict** mode denies unknown namespaces; **open** bypasses checks.

## Lua bindings and topic-to-topic transforms

When bindings are configured, a message produced on a source topic triggers
function execution and potentially a new write to a target topic.

```
Producer ---- SEND ----> Broker ---- Lua function ----> Target topic
                                  (payload in/out)
```

- Each binding has a source topic, function, and target topic.
- The function executes in a worker pool and returns a transformed payload.
- The broker writes the result as a new message to the target topic, applying
  persistence based on the target scheme.

## Error handling and edge cases

- **Oversized frames**: rejected during frame parsing.
- **Checksum failure**: message rejected and logged.
- **Authorization failure**: `PRODUCER`/`SUBSCRIBE` denied.
- **Missing permits**: delivery loop waits until `FLOW` increases permits.
- **Stale subscriptions**: periodic maintenance prunes inactive subscriptions
  and older retained messages according to namespace policies.

## End-to-end sequence summary

```
Producer             Broker                  Storage             Consumer
   | CONNECT          |                                            |
   |----------------->|                                            |
   |<-----------------| CONNECTED                                 |
   | PRODUCER         |                                            |
   |----------------->|                                            |
   |<-----------------| PRODUCER_SUCCESS                           |
   | SUBSCRIBE        |                                            |
   |----------------->|                                            |
   |<-----------------| SUCCESS                                    |
   | FLOW             |                                            |
   |----------------->|                                            |
   | SEND             |                                            |
   |----------------->|-- insert message ------------------------>|
   |                  |<------------------------ row id -----------|
   |<-----------------| SEND_RECEIPT                               |
   |                  |-- MESSAGE -------------------------------->|
   |                  |                                            | ACK
   |                  |<-------------------------------------------|
   |                  |-- delete pending ------------------------->|
```
