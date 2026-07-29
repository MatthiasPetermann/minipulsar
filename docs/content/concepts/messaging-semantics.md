---
title: "Messaging semantics"
weight: 22
---

# Messaging semantics

## Persistent publish, delivery, and acknowledgement

```mermaid
sequenceDiagram
    participant P as Producer
    participant B as Broker
    participant DB as SQLite
    participant D as Delivery loop
    participant C as Consumer
    P->>B: SEND(metadata + opaque bytes)
    B->>B: Validate magic, CRC32C, metadata, size
    B->>DB: Insert message transaction
    DB-->>B: entry id
    B-->>P: SEND_RECEIPT(entry id)
    B->>D: Kick subscription states for topic
    D->>DB: Claim batch, add pending rows, advance cursor
    DB-->>D: Claimed messages
    D-->>C: MESSAGE(entry id, metadata, payload)
    C->>B: ACK individual or cumulative
    B->>DB: Remove pending rows owned by consumer UID
```

Persistent delivery is at-least-once. A pending row is committed before socket
write. If a connection fails after the claim and before the consumer processes
the frame, the message is eligible for redelivery.

## Subscription types

| Type | Behaviour | Cumulative ACK |
|---|---|---|
| Exclusive | One attached consumer; another attachment is rejected. | Supported |
| Shared | Permitted consumers are selected round-robin. | Ignored to preserve correctness |
| Failover | Highest-priority permitted consumer receives messages. | Supported |
| Key_Shared | Not supported. | N/A |

## Start positions, seek, and redelivery

For a new persistent subscription:

- `initialPosition=Earliest` begins at the first stored entry.
- `initialPosition=Latest` begins after the newest stored entry.
- `start_message_id` selects an explicit entry ID on ledger `0`.
- `start_message_rollback_duration_sec` resolves the first message at or after
  the calculated timestamp when retained history exists.

`SEEK` resets the shared subscription cursor to a message ID or publish time and
clears outstanding pending rows. `REDELIVER_UNACKNOWLEDGED_MESSAGES` requeues
selected pending entries, or all entries owned by that consumer when no IDs are
provided. Either action can duplicate a frame already written to a consumer;
clients must keep acknowledgement handling idempotent.

```mermaid
sequenceDiagram
    participant C as Consumer
    participant B as Broker
    participant DB as SQLite
    C->>B: SEEK(message id or publish time)
    B->>DB: Delete pending rows for subscription
    B->>DB: Set next_message_id
    B-->>C: SUCCESS
    B->>DB: Claim from new cursor when permits exist
    B-->>C: MESSAGE replay
```

## Timeouts and disconnects

On disconnect, the broker removes that consumer's pending rows and rewinds the
cursor to the lowest removed entry. When `-ack-timeout` is set, a monitor does
the same for expired pending entries. Remaining eligible consumers then restart
delivery when they hold permits.

## Producer access

The broker supports Shared, Exclusive, WaitForExclusive, and
ExclusiveWithFencing modes. Fencing unregisters earlier producers in the local
broker process. It is not distributed fencing across nodes.
