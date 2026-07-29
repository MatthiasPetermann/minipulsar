---
title: "Configuration and lifecycle"
weight: 31
---

## Core flags

| Flag | Purpose |
|---|---|
| `-addr`, `-tls-addr` | Plain and optional TLS Pulsar listener addresses. Empty disables a listener. |
| `-db` | SQLite database path. WAL, busy timeout, and foreign keys are enabled. |
| `-broker-url`, `-broker-url-tls` | URLs returned by `LOOKUP`. |
| `-max-frame`, `-max-message` | Inbound allocation and payload limits. |
| `-max-connections`, `-max-producers`, `-max-consumers` | Optional global resource caps; `0` disables each cap. |
| `-read-timeout`, `-write-timeout` | Per-frame network deadlines. |
| `-ack-timeout`, `-ack-timeout-check-interval` | Pending-delivery expiry and scan interval. |
| `-namespace-maintenance-interval` | Retention and stale-subscription maintenance cadence. |

The process handles `SIGINT` and `SIGTERM`. It stops metrics first, then closes
broker listeners and connections. Use a process supervisor with a shutdown grace
period of at least ten seconds.

## SQLite operation

SQLite is the durable single-node log. Keep the database, `-wal`, and `-shm`
files on the same local filesystem. Do not place a live database on an
unreliable network share. Retention controls later deletion, not initial durable
publication; see [Persistence, retention, and deletion](../../concepts/persistence-and-retention/).
Back up using a SQLite-consistent backup mechanism,
not a blind copy while writers are active.

The schema includes `schema_migrations`; startup applies additive schema setup.
Run `PRAGMA integrity_check` against an offline copy before restoring a backup.

## Retention and cleanup

With an HCL namespace policy, maintenance can delete stale subscriptions,
consumed retained messages, orphaned messages, and empty topics. A zero retention
period means messages without subscriptions need not be retained. Verify policy
semantics on staging data before enabling automatic cleanup in an edge fleet.
