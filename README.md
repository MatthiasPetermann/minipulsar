# minipulsar – Minimal Pulsar-Compatible Broker (PoC)

A deliberately minimal Apache Pulsar-compatible broker implemented in Go. It is
meant for local experiments, protocol inspection, and learning, and we are open
to using it as a standalone component at the edge—not production.

## Why this exists

minipulsar focuses on the Pulsar binary protocol and a tiny persistence layer so
it can serve as a compact reference implementation. It intentionally omits many
Pulsar features while still letting standard clients connect and exchange
messages.

## Goals and deliberate constraints

### Goals

- Learn and experiment with the Pulsar protocol.
- Explore use cases where a complex Pulsar cluster is unnecessary, but the
  protocol itself is still useful.
- Build a highly portable standalone broker for those use cases (yes, it already
  runs on NetBSD) with as few external dependencies as is reasonable.
- Keep it viable as a standalone component for edge deployments.
- Support JWT-based authentication.
- Support Pulsar Functions in Lua.
- Support policies expressed as Lua DSLs.

### Non-goals

- Restrict to a byte schema, meaning no broker-side schema validation.
- At least for now, no highly available persistence model in the broker and no
  multidimensional scaling like Apache Pulsar (if you need that, Apache Pulsar is
  honestly the better choice).

## Features (intentionally reduced)

- Pulsar binary protocol over TCP (default `:6650`)
- Persistent and non-persistent topics (`persistent://` and `non-persistent://`)
- JWT authentication (HS256) with role/roles claims for authorization
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
  - SQLite log per topic in `messages` (persistent topics only)
  - Normalized schema to reflect tenant → namespace → topic
  - Subscription cursor and pending delivery tables
  - Shared subscription delivery with round-robin consumers
  - Non-persistent topics are kept in memory only (no backlog)
- Messaging control plane (HCL) with Lua functions and bindings
- Prometheus metrics endpoint
- Optional synthwave TUI dashboard

## Non-features

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

## Quickstart

Minimal startup with JWT auth (HS256) and messaging policies:

```bash
export MINIPULSAR_JWT_SECRET="dev-secret"
./bin/minipulsar \
  -addr :6650 \
  -db ./minipulsar.db \
  -messaging-config ./examples/messaging.hcl
```

A tiny script (no external dependencies) to mint an HS256 JWT with `role`:

```bash
python - <<'PY'
import base64, json, hmac, hashlib, time

secret = b"dev-secret"
header = {"alg": "HS256", "typ": "JWT"}
payload = {"role": "tester", "exp": int(time.time()) + 3600}

def b64(obj):
    raw = json.dumps(obj, separators=(",", ":"), sort_keys=True).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

msg = f"{b64(header)}.{b64(payload)}"
sig = base64.urlsafe_b64encode(hmac.new(secret, msg.encode(), hashlib.sha256).digest()).rstrip(b"=").decode()
print(f"{msg}.{sig}")
PY
```

Set the token as `Authorization: Bearer <token>` in the Pulsar client config
or send it directly as `auth_data` in the CONNECT command.

## Messaging config (HCL)

The messaging control plane config is optional. When provided, it can:

- Define authorization policies for namespaces.
- Register Lua functions.
- Bind a source topic through a function into a target topic.

Example:

```hcl
security {
  mode = "strict"
}

namespace "persistent://public/default" {
  produce = ["tester"]
  consume = ["tester"]
}

function "transform" {
  path = "transform.lua"
}

binding {
  source = "persistent://public/default/temperature.f"
  function = "transform"
  target = "persistent://public/default/temperature.c"
}
```

### Security mode behavior

`mode` controls how strictly namespace policies are enforced:

- `strict`: namespace rules are enforced for produce/consume. If a namespace is
  not declared, or an action has no matching role, access is denied.
- `open`: all authorization checks are bypassed. Namespace rules are ignored.

This keeps the model simple: either you enforce explicit allowlists (`strict`)
or you run in an open mode for local/testing setups (`open`).

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
- `-messaging-config` – path to messaging control-plane HCL config
- `-function-workers` – number of Lua function workers
- `-metrics-addr` – listen address for Prometheus metrics endpoint (empty to disable)
- `-metrics-path` – HTTP path for Prometheus metrics endpoint
- `-metrics-interval` – interval between metrics collection
- `-metrics-top-topics` – number of top topics to export metrics for
- `-jwt-secret` – shared secret for HS256 JWT verification (or set `MINIPULSAR_JWT_SECRET`)
- `-tui` – enable synthwave TUI dashboard

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
