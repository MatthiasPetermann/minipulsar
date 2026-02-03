# Storage layer

The storage layer (`internal/storage`) is responsible for persistence and
tracking delivery progress. It uses SQLite to store topics, messages,
subscriptions, and pending acknowledgements.

## Schema overview

The schema includes these tables:

- `namespaces`: tenant/namespace pairs.
- `topics`: topic metadata and full names.
- `messages`: stored message payloads with publish time and sequence ID.
- `subscriptions`: per-topic subscription metadata.
- `subscription_cursor`: the next message ID to deliver per subscription.
- `subscription_pending`: messages delivered but not yet acknowledged.

The schema is created in `Store.InitSchema` and is migrated with
`addColumnIfMissing` for incremental changes.

For a detailed entity diagram and consistency expectations, see
`docs/database-model.md`.

## Topic and namespace normalization

All storage operations parse the topic name with `internal/topic.Parse` and
ensure that only **persistent** topics are stored. Non-persistent topics bypass
SQLite entirely.

## Subscriptions

`EnsureSubscription` creates the subscription row and initializes the cursor
based on the requested starting position:

- **Latest**: cursor starts after the most recent message ID.
- **Earliest**: cursor starts at message ID 1.

The `last_consumer_at` timestamp is updated whenever a consumer subscribes or
claims a batch, enabling inactivity cleanup.

## Message persistence

`InsertMessage` stores the payload, publish time, and sequence ID. The SQLite
row ID becomes the broker's message ID and is used for Pulsar entry IDs.

## Delivery cursor and pending set

`ClaimBatch` is the core delivery primitive:

1. Ensures the subscription cursor exists.
2. Reads the cursor (next message ID).
3. Selects messages at or after the cursor that are *not* pending.
4. Inserts each selected message into `subscription_pending` to prevent
   duplicate delivery.
5. Advances the cursor to the last seen ID + 1.

This design ensures:

- Messages are delivered in order per subscription.
- Shared subscription consumers do not double-deliver messages.
- Pending rows are the source of truth for acknowledgements.

## Acknowledgements

- `AckIndividual` removes pending rows for a specific consumer UID and message
  IDs (used by `ACK` commands).
- `DropPendingByConsumer` clears all pending rows for a consumer when a
  connection closes.

## Retention and cleanup

Namespace-level maintenance is handled in `internal/storage/cleanup.go`:

- `PruneStaleSubscriptions`: deletes subscriptions that have not been served
  recently (based on `last_consumer_at`).
- `PruneConsumedMessages`: removes messages that all subscriptions have advanced
  past (cursor minimum).
- `PruneOrphanedMessages`: removes messages for topics with **no subscriptions**.
- `PruneEmptyTopics`: removes topics that have no messages or subscriptions,
  excluding any topics explicitly marked as active by the broker.
- `PruneOrphanedSubscriptionData`: removes cursor/pending rows that no longer
  have matching subscriptions.

## Stats

`StatsSnapshot` aggregates counts for namespaces, topics, messages,
subscriptions, and pending rows. It also computes “top topics” by pending
messages for metrics and the TUI dashboard.

The metrics subsystem separately reports subscription backlog as the count of
undelivered messages per subscription (derived from subscription cursors).
