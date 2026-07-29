---
title: "Subscription and producer behavior"
weight: 55
---

## Subscription position lifecycle

```mermaid
sequenceDiagram
    participant C as Consumer
    participant B as Broker
    participant DB as SQLite
    C->>B: SUBSCRIBE(name, initial position/start ID)
    B->>DB: Does subscription exist?
    alt new subscription
        B->>DB: Persist type and initial cursor
    else existing subscription
        B->>DB: Preserve cursor; validate type
    end
    B-->>C: SUCCESS
    C->>B: FLOW(permits)
    B->>DB: Claim from persisted cursor
    B-->>C: MESSAGE
```

Initial position, explicit `start_message_id`, and rollback duration apply only
when creating a subscription. Reattaching an existing name never silently moves
its cursor. `start_message_id`, `SEEK`, redelivery IDs, and outbound IDs support
ledger `0` only.

A timestamp SEEK finds the first stored entry at or after the requested time. If
none exists, the cursor is moved to maximum signed entry ID, so no current or
future normal SQLite entry is delivered until another explicit seek resets it.
SEEK clears every pending row for that subscription; messages already written to
a socket may therefore be duplicated.

Non-persistent subscriptions have no SQLite cursor or pending set. ACK, seek,
and redelivery have no durable replay meaning for them.

## Producer access is local-process arbitration

| Mode | Current behavior |
|---|---|
| Shared | Registers alongside other producers. |
| Exclusive | Rejects creation when any producer is currently registered on the topic. |
| WaitForExclusive | Blocks the connection's command reader until the topic has no producer. There is no request timeout. |
| ExclusiveWithFencing | Removes earlier producers from the broker map. Their later SEND gets `SEND_ERROR`; their TCP connection is not closed. |

These modes are scoped to one minipulsar process. They are not distributed
fencing or ownership. Also note the current asymmetry: after an Exclusive
producer exists, a later Shared producer is not prevented by a reciprocal check.
Do not use these modes as a substitute for cluster-wide exclusivity.
