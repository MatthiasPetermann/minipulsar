# minipulsar

**Edge-ready, minimal Pulsar-compatible broker (PoC)**

minipulsar is a deliberately lean Apache Pulsar-compatible broker written in Go.
It focuses on protocol clarity and a small footprint so you can experiment with
Pulsar semantics on edge devices or constrained environments.

> **Status:** Proof-of-Concept (PoC). Not production-ready.

---

## Highlights

- **Pulsar protocol compatible** for standard clients.
- **Edge-first** runtime: small binary, fast start, minimal dependencies.
- **Persistent + non-persistent topics** with a SQLite-backed log.
- **JWT auth + HCL policies** for explicit access control.
- **Prometheus metrics** and optional synthwave TUI dashboard.

---

## Quickstart

### Build

```bash
make generate   # generate pb/PulsarApi.pb.go
make build      # compile ./bin/minipulsar
```

### Run (plain TCP)

```bash
./bin/minipulsar \
  -addr :6650 \
  -db ./minipulsar.db
```

### Run with TLS (plain + TLS listeners)

minipulsar can listen on both the plain Pulsar port (`:6650`) and a TLS port
(`:6651`) at the same time. The defaults are configurable via `-addr` and
`-tls-addr`.

Create a dev certificate (self-signed):

```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout server.key \
  -out server.crt \
  -days 365 \
  -subj "/CN=localhost"
```

Run with both listeners enabled:

```bash
./bin/minipulsar \
  -addr :6650 \
  -tls-addr :6651 \
  -db ./minipulsar.db \
  -tls-cert ./server.crt \
  -tls-key ./server.key
```

If you only want TLS, disable the plain listener by passing an empty `-addr`:

```bash
./bin/minipulsar \
  -addr "" \
  -tls-addr :6651 \
  -db ./minipulsar.db \
  -tls-cert ./server.crt \
  -tls-key ./server.key
```

You can also override the advertised broker URLs used in LOOKUP responses:

```bash
./bin/minipulsar \
  -addr :6650 \
  -tls-addr :6651 \
  -broker-url pulsar://127.0.0.1:6650 \
  -broker-url-tls pulsar+ssl://127.0.0.1:6651 \
  -db ./minipulsar.db \
  -tls-cert ./server.crt \
  -tls-key ./server.key
```

---

## Authentication & Policies (JWT + HCL)

```bash
export MINIPULSAR_JWT_SECRET="dev-secret"
./bin/minipulsar \
  -addr :6650 \
  -db ./minipulsar.db \
  -messaging-config ./examples/messaging.hcl
```

Generate a tiny HS256 JWT (no external deps):

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

Set the token as `Authorization: Bearer <token>` in your Pulsar client or pass it
in the `CONNECT` command `auth_data` field.

### Messaging config example (HCL)

```hcl
security {
  mode = "strict"
}

namespace "persistent://public/default" {
  produce = ["tester"]
  consume = ["tester"]
  subscription_timeout_seconds = 3600
  retention_seconds = 300
}

function "transform" {
  path = "transform.lua"
  max_runtime = "250ms"
}

binding {
  source = "persistent://public/default/temperature.f"
  function = "transform"
  target = "persistent://public/default/temperature.c"
}
```

Namespace policies (within a `namespace` block):

- `subscription_timeout_seconds`: delete subscriptions that have not been served by any consumer within the timeout.
  Defaults to `0`, which disables subscription timeouts.
- `retention_seconds`: retain messages for this duration. Messages are removed once they are older than
  the retention window and have been consumed by all subscriptions. Topics with no subscriptions are
  cleaned up based on the same retention window. Defaults to `0`, which skips persisting messages when
  there are no subscriptions and removes consumed messages as soon as possible.
- Empty topic cleanup runs during namespace maintenance and deletes topics with no messages, no subscriptions,
  and no active producers/consumers. Topics referenced by function bindings are preserved.

Subscription start positions are driven by the Pulsar `SUBSCRIBE` command (`initialPosition`), not by the namespace config.

---

## Features

- Pulsar binary protocol over TCP (default `:6650`)
- Persistent and non-persistent topics
- JWT authentication (HS256)
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
  - SQLite log per topic
  - Subscription cursors + pending delivery tracking
  - Subscription delivery types: Exclusive, Shared (round-robin), Failover (priority)
- Producer access modes: Shared, Exclusive, WaitForExclusive, ExclusiveWithFencing
- Messaging control plane (HCL) with Lua functions and bindings
- Namespace policies for subscription timeouts and orphaned message retention
- Prometheus metrics endpoint
- Optional synthwave TUI dashboard

### Observability notes

- The TUI "Top Topics" view shows total stored messages (`msgs`) and delivered-but-unacked messages (`pending`) per topic.
- Subscription backlog is tracked as undelivered messages per subscription and is exposed via Prometheus metrics.

---

## Project layout

- `cmd/minipulsar` – CLI entrypoint, flags, logging, config
- `internal/broker` – Connection lifecycle, protocol handlers, delivery orchestration
- `internal/storage` – SQLite schema and persistence primitives
- `internal/protocol` – Pulsar wire framing helpers
- `pb/PulsarApi.proto` – Pulsar protocol definition (generated via `protoc`)

---

## Development

### Requirements

- Go >= 1.21
- `protoc` + `protoc-gen-go`

Install the Go plugin if needed:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
export PATH="$PATH:$HOME/go/bin"
```

### Tests

```bash
go test ./...
```

---

## CLI flags (selection)

- `-addr` – listen address for Pulsar protocol
- `-tls-addr` – listen address for Pulsar TLS protocol
- `-db` – path to SQLite DB
- `-broker-url` – broker URL for `LOOKUP`
- `-broker-url-tls` – broker TLS URL for `LOOKUP` (empty disables)
- `-server-version` – server version in `CONNECTED`
- `-max-frame` – max frame size (bytes)
- `-max-message` – max message size (bytes)
- `-log-level` – log level (`trace`, `debug`, `info`, `warn`, `error`)
- `-log-format` – log format (`text` or `json`)
- `-log-timestamp` – timestamps (`true`/`false`)
- `-messaging-config` – path to messaging control-plane HCL config
- `-function-workers` – number of Lua function workers
- `-metrics-addr` – Prometheus endpoint (empty disables)
- `-metrics-path` – HTTP path
- `-metrics-interval` – export interval
- `-metrics-top-topics` – number of top topics in metrics
- `-namespace-maintenance-interval` – interval between namespace maintenance sweeps
- `-jwt-secret` – HS256 secret (or set `MINIPULSAR_JWT_SECRET`)
- `-tui` – enable synthwave TUI
- `-read-timeout` – maximum duration to read a frame (0 disables)
- `-write-timeout` – maximum duration to write a frame (0 disables)
- `-tls-cert` – TLS server certificate PEM (enables TLS)
- `-tls-key` – TLS server private key PEM

---

## Notes

This project is for **learning, prototyping, and edge experiments**.
For production environments, full Apache Pulsar is the right choice.

If you want to help make the **Edge Broker** real: issues, feedback, and PRs are welcome.
For more on my Pulsar work, see the ongoing notes and articles tagged here:
https://www.petermann-digital.de/tags/pulsar/
If you are looking for a **Pulsar expert**, feel free to reach out via:
https://www.petermann-digital.de

---

## Author

minipulsar is an experiment by Matthias Petermann. More background and write-ups at:
https://www.petermann-digital.de/blog/
