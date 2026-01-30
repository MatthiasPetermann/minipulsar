# minipulsar – Minimal Pulsar-Compatible Broker (PoC)

A deliberately minimal Apache Pulsar-compatible broker implemented in Go. It is
meant for local experiments, protocol inspection, and learning—not production.

## Why this exists

minipulsar focuses on the Pulsar binary protocol and a tiny persistence layer so
it can serve as a compact reference implementation. It intentionally omits many
Pulsar features while still letting standard clients connect and exchange
messages.

## Features (intentionally reduced)

- Pulsar binary protocol over TCP (default `:6650`)
- Supported commands:
  - `CONNECT` / `CONNECTED`
  - `PARTITIONED_METADATA` / `PARTITIONED_METADATA_RESPONSE` (always 0 partitions)
  - `LOOKUP` / `LOOKUP_RESPONSE` (redirects to itself)
  - `PRODUCER` / `PRODUCER_SUCCESS`
  - `SEND` / `SEND_RECEIPT`
  - `SUBSCRIBE` / `SUCCESS`
  - `FLOW` / `MESSAGE`
  - `ACK` (individual ack only)
  - `PING` / `PONG`
- Persistence:
  - SQLite log per topic in `messages`
  - Subscription cursor and pending delivery tables
  - Shared subscription delivery with round-robin consumers

## Non-features

- Authentication / authorization
- TLS
- Partitions (topics are treated as non-partitioned)
- Schema registry
- Transactions
- Retention, DLQ, policies, compaction, or tiered storage

## Project layout

- `cmd/minipulsar`
  - CLI entrypoint and runtime wiring (flags, logging, broker config)
- `internal/broker`
  - Connection lifecycle, protocol handlers, and delivery orchestration
- `internal/storage`
  - SQLite schema and persistence primitives
- `internal/protocol`
  - Pulsar wire framing helpers
- `pb/PulsarApi.proto`
  - Pulsar protocol definition used to generate Go types

## Requirements

- Go >= 1.21
- `protoc` + `protoc-gen-go` in your `PATH`

Install the Go plugin if needed:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
export PATH="$PATH:$HOME/go/bin"
```

## Build

```bash
make generate   # generate pb/PulsarApi.pb.go from pb/PulsarApi.proto
make build      # compile cmd/minipulsar into ./bin/minipulsar
```

## Run

```bash
./bin/minipulsar \
  -addr :6650 \
  -db ./minipulsar.db
```

### CLI flags

- `-addr` – listen address for the Pulsar binary protocol
- `-db` – path to the SQLite database file
- `-broker-url` – broker URL advertised in `LOOKUP` responses
- `-server-version` – server version reported in `CONNECTED`
- `-max-frame` – maximum inbound frame size (bytes)
- `-max-message` – maximum message size advertised to clients (bytes)
- `-log-level` – log level (`trace`, `debug`, `info`, `warn`, `error`)
- `-log-format` – log format (`text` or `json`)
- `-log-timestamp` – include timestamps in output (`true`/`false`)

## Protocol flow (typical client session)

1. Client sends `PARTITIONED_METADATA` → broker replies with 0 partitions.
2. Client sends `LOOKUP` → broker responds with `LOOKUP_RESPONSE` pointing to itself.
3. Client sends `CONNECT` → broker replies `CONNECTED` with protocol metadata.
4. Client sends `PRODUCER` → broker replies `PRODUCER_SUCCESS`.
5. Client sends `SEND` → broker persists payload and replies `SEND_RECEIPT`.
6. Client sends `SUBSCRIBE` → broker replies `SUCCESS`.
7. Client sends `FLOW` → broker delivers messages as `MESSAGE` frames.
8. Client sends `ACK` → broker removes pending entries for that consumer.
9. Client/Broker exchange `PING`/`PONG` keepalives.

## Notes

- This project is a learning and test platform only.
- For production use, rely on a full Apache Pulsar deployment.
