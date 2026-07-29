---
title: "CLI reference"
weight: 53
---

## Networking and identity

| Flag | Default | Meaning |
|---|---:|---|
| `-addr` | `:6650` | Plain Pulsar TCP listener; empty disables it. |
| `-tls-addr` | `:6651` | TLS listener; active only with certificate and key. |
| `-broker-url` | `pulsar://localhost:6650` | URL returned in plain lookup responses. |
| `-broker-url-tls` | `pulsar+ssl://localhost:6651` | TLS URL returned in lookup responses. |
| `-server-version` | `minipulsar-0.1` | Value returned in `CONNECTED`. |
| `-tls-cert`, `-tls-key` | empty | PEM pair enabling TLS 1.2+. |

## Storage and message limits

| Flag | Default | Meaning |
|---|---:|---|
| `-db` | `./minipulsar.db` | SQLite file path. |
| `-max-frame` | 10 MiB | Largest accepted inbound frame. |
| `-max-message` | 5 MiB | Largest accepted payload and advertised client limit. |
| `-max-connections` | `0` | Concurrent TCP cap; zero means unlimited. |
| `-max-producers` | `0` | Broker-wide producer cap; zero means unlimited. |
| `-max-consumers` | `0` | Broker-wide consumer cap; zero means unlimited. |
| `-read-timeout`, `-write-timeout` | 15s | Network operation deadlines; zero disables a deadline. |

## Delivery and policy

| Flag | Default | Meaning |
|---|---:|---|
| `-ack-timeout` | `0` | Age after which pending messages are requeued; zero disables it. |
| `-ack-timeout-check-interval` | 30s | Pending expiry scan cadence. |
| `-namespace-maintenance-interval` | 30s | HCL retention/staleness maintenance cadence. |
| `-messaging-config` | empty | HCL control-plane file. |
| `-function-workers` | 4 | Lua worker count. |
| `-jwt-secret` | env value | Optional HS256 JWT secret. |

## Operator output

| Flag | Default | Meaning |
|---|---:|---|
| `-log-level` | `info` | `trace`, `debug`, `info`, `warn`, or `error`. |
| `-log-format` | `text` | `text` or `json`. |
| `-log-timestamp` | `true` | Include timestamps in logs. |
| `-metrics-addr` | `127.0.0.1:8080` | Prometheus listener; empty disables it. |
| `-metrics-path` | `/metrics` | Prometheus HTTP path. |
| `-metrics-interval` | 5s | Snapshot collection interval. |
| `-metrics-top-topics` | 10 | Top-N label bound for metrics. |
| `-tui` | `false` | Start the full-screen operator interface. |
