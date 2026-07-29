---
title: "Protocol compatibility"
weight: 51
---

minipulsar accepts Pulsar binary protocol frames over TCP or optional TLS. The
table describes implemented server behavior, not a claim of full Apache Pulsar
feature parity.

| Area | Implemented |
|---|---|
| Session | `CONNECT`/`CONNECTED`, `PING`/`PONG`, close producer/consumer |
| Discovery | `LOOKUP` to configured self URL; partition metadata reporting zero partitions |
| Producer | producer creation, local Shared/Exclusive/Wait/Fencing access modes, `SEND`/receipt/error |
| Consumer | subscribe, `FLOW`, Exclusive/Shared/Failover scheduling, close |
| Delivery | individual ACK, cumulative ACK for Exclusive/Failover, pending tracking, disconnect/timeout redelivery |
| Positioning | `initialPosition`, `start_message_id`, rollback duration, `SEEK`, `GET_LAST_MESSAGE_ID` |
| Replay | `REDELIVER_UNACKNOWLEDGED_MESSAGES` for messages owned by the requesting consumer |
| Integrity | frame/message bounds, Pulsar magic marker, CRC32C validation |

## Deliberate boundaries

- No partitions, broker discovery cluster, load balancing, replication, or
  BookKeeper ledger semantics.
- No `Key_Shared` subscription, schema registry, payload schema validation,
  transactions, chunking, compression, or batch decoding.
- No admin REST API, topic-watch API, OAuth2, mTLS authentication, or external
  authorization provider.
- IDs use ledger `0` and SQLite auto-increment entry IDs. Clients must not infer
  full Apache Pulsar ledger semantics from them.
- Delivery is at-least-once. Producer retries are not deduplicated by sequence
  ID; consumers should be idempotent.

## Error behavior

Malformed framing closes the affected connection because the stream cannot be
safely recovered. A decoded but rejected command returns `ERROR`; a rejected
send returns `SEND_ERROR`. Unsupported command types return
`UnsupportedVersionError`.
