# Database model

This document describes the SQLite persistence model used by minipulsar's storage layer. The
schema lives in `internal/storage/storage.go` and is initialized by `Store.InitSchema`.【F:internal/storage/storage.go†L56-L143】

## Entity overview

```
  namespaces
  ┌────────────────────────────────────┐
  │ id (PK)                             │
  │ tenant                              │
  │ name                                │
  └────────────────────────────────────┘
                1
                │
                │
                ▼
  topics
  ┌────────────────────────────────────┐
  │ id (PK)                             │
  │ namespace_id (FK -> namespaces.id)  │
  │ name                                │
  │ full_name (unique)                  │
  └────────────────────────────────────┘
           1           1
           │           │
           │           │
           ▼           ▼
  messages             subscriptions
  ┌──────────────────┐  ┌────────────────────────────────────────────┐
  │ id (PK)           │  │ topic_id (PK/FK -> topics.id)              │
  │ topic_id (FK)     │  │ name (PK)                                   │
  │ payload           │  │ type                                        │
  │ publish_time      │  │ created_at                                  │
  │ sequence_id       │  │ last_consumer_at                            │
  └──────────────────┘  └────────────────────────────────────────────┘
                               │
                               │
                               ▼
  subscription_cursor
  ┌────────────────────────────────────────────┐
  │ topic_id (PK/FK -> subscriptions.topic_id) │
  │ name (PK/FK -> subscriptions.name)         │
  │ next_message_id                            │
  └────────────────────────────────────────────┘
                               │
                               │
                               ▼
  subscription_pending
  ┌────────────────────────────────────────────┐
  │ topic_id (PK)                              │
  │ name (PK)                                  │
  │ message_id (PK)                            │
  │ consumer_id                                │
  │ delivered_at                               │
  └────────────────────────────────────────────┘
```

## Table descriptions

### namespaces
* The namespace boundary for persistent topics (tenant + namespace).【F:internal/storage/storage.go†L68-L74】
* Uniqueness is enforced across `(tenant, name)` to avoid duplicates.【F:internal/storage/storage.go†L68-L74】

### topics
* One row per persistent topic within a namespace.【F:internal/storage/storage.go†L76-L82】
* `full_name` is unique so topics can be looked up quickly without joining on namespaces.【F:internal/storage/storage.go†L76-L82】

### messages
* Each message is tied to a topic and uses the auto-incremented ID as the Pulsar entry ID.【F:internal/storage/storage.go†L84-L90】【F:internal/storage/storage.go†L223-L260】
* `publish_time` and `sequence_id` are stored to support retention and client metadata.【F:internal/storage/storage.go†L84-L90】

### subscriptions
* `(topic_id, name)` is the composite primary key; a topic can have multiple subscriptions, but names are unique per topic.【F:internal/storage/storage.go†L92-L100】
* `created_at` and `last_consumer_at` track subscription age and activity for maintenance tasks.【F:internal/storage/storage.go†L92-L100】【F:internal/storage/storage.go†L136-L143】

### subscription_cursor
* Stores the **dispatch cursor** (`next_message_id`) per subscription, i.e. the next message ID to claim for delivery.【F:internal/storage/storage.go†L102-L111】【F:internal/storage/storage.go†L318-L413】
* This cursor is advanced when messages are claimed (not when they are acknowledged).【F:internal/storage/storage.go†L372-L413】

### subscription_pending
* Stores message IDs that have been claimed for a subscription but not yet acknowledged.【F:internal/storage/storage.go†L113-L121】【F:internal/storage/storage.go†L353-L387】
* It prevents other consumers from claiming the same message until it is acknowledged or dropped.【F:internal/storage/storage.go†L353-L387】

## Consistency expectations

* Every subscription should have exactly one cursor row; cursor rows should not exist without a subscription.
* Pending rows must reference an existing subscription and a message ID for the same topic.
* Maintenance tasks remove stale subscriptions and can prune orphaned cursor/pending rows to repair inconsistencies.【F:internal/storage/cleanup.go†L17-L230】

### Repair operations

* `PruneStaleSubscriptions` deletes stale subscriptions and their cursor/pending rows.【F:internal/storage/cleanup.go†L17-L86】
* `PruneOrphanedSubscriptionData` removes cursor/pending rows that no longer have matching subscriptions, fixing cases where maintenance removed subscriptions but left data behind.【F:internal/storage/cleanup.go†L200-L266】
