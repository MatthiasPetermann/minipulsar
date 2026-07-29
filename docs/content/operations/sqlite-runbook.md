---
title: "SQLite runbook"
weight: 35
---

## Connection settings and capacity

The store opens SQLite with WAL journal mode, a five-second busy timeout,
foreign keys, and a `database/sql` pool capped at four open and idle
connections. WAL allows readers while a writer commits, but SQLite remains a
single-writer system. High producer fan-in, ACK churn, retention, and expensive
statistics contend for that writer.

Capacity planning must include:

- payload bytes plus metadata/property JSON and SQLite page overhead;
- one pending row per unacknowledged delivery **per subscription**;
- WAL growth during sustained writes and long readers;
- retention and backup headroom; and
- write latency under the configured busy timeout.

`stored_messages` measures retained message rows, not total historical publishes.
Aggregate pending may exceed stored-message count because one retained entry can
be pending independently for multiple subscriptions.

## Backup and restore procedure

1. Prefer a SQLite online backup mechanism or stop minipulsar cleanly.
2. If copying files, copy the database with its matching `-wal` and `-shm`
   files as one consistent unit.
3. Store backups outside the live database directory and verify restoration in
   an isolated path.
4. Run `PRAGMA integrity_check` against the restored copy before serving it.
5. Start minipulsar with the restored path; schema initialization is additive.

Never replace the live database file under a running process. Do not manually
delete pending or cursor rows as a way to “unstick” consumers; use consumer
disconnect, redelivery, seek, or a controlled restore.

## Migration model

`schema_migrations` currently records version `1`. Startup uses
`CREATE TABLE IF NOT EXISTS`, index creation, and additive-column checks. It is
not a generalized rollback-capable migration engine. Test any future destructive
schema change against a copy of real edge data and supply a dedicated migration
tool rather than adding implicit destructive startup SQL.

## First-response checklist

1. Check available disk, inode capacity, filesystem errors, and database/WAL
   ownership.
2. Check logs for `database is locked`, busy-timeout, or integrity errors.
3. Inspect pending, stored messages, and maintenance timing through metrics/TUI.
4. Reduce producer rate or disable nonessential Lua bindings while investigating.
5. Take a consistent backup before repair or upgrade action.
