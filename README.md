# minipulsar – Edge-ready, minimal Pulsar-compatible broker (PoC)

**minipulsar** is a deliberately lean, Apache Pulsar-compatible broker written in Go.
It is built for protocol exploration, edge experiments, and lightweight deployments
where you want Pulsar compatibility without running a full Pulsar cluster.

> **Status:** Proof-of-Concept (PoC). Not production-ready — yet.

---

## Vision: from PoC to a real Edge Broker

The goal is a **real Edge Broker**: small, portable, deterministic — and still
compatible with the Pulsar protocol. That means:

- **Edge-first:** runs on small hardware, minimal footprint, simple deploys.
- **Protocol-compatible:** standard Pulsar clients can connect.
- **Secure-by-design:** authentication and policies already exist today.
- **TLS is a near-term goal:** transport encryption is explicitly on the roadmap.

Why this matters:

- Edge devices need local brokers that **start fast** and are **easy to operate**.
- Many use cases need the Pulsar protocol **but not** the full Pulsar infrastructure.
- A lightweight edge gateway can preprocess, buffer, and distribute data locally
  before it is aggregated centrally.

---

## Why minipulsar exists

minipulsar serves as a **compact reference implementation** of the Pulsar binary
protocol — with minimal persistence, authentication, and a small control plane.

We intentionally build **only what’s necessary**, so you can:

- understand the Pulsar protocol **concretely**,
- run a broker **easily in edge environments**,
- add features **iteratively** without monolithic dependencies.

Personal motivation: I originally wanted to run Apache Pulsar on NetBSD. While Pulsar is implemented in Java and should run anywhere a JDK exists, in practice native libs bundled in some JARs (e.g., BookKeeper → RocksDB) make non‑mainstream platforms fail. That friction was one of the reasons to dig deeper into the Pulsar stack.

---

## Goals (and deliberate constraints)

### Goals

- Understand, test, and implement the Pulsar protocol.
- A minimal broker for edge deployments.
- Portable (already runs on NetBSD).
- JWT-based authentication.
- Policies as a human-readable DSL (HCL)
- Pulsar Functions (Lua) for edge transformations.

### Non-goals (for now)

- Full Pulsar feature parity.
- High availability, multi-cluster, or cluster sharding.
- Schema registry or broker-side schema validation.

---

## Features (intentionally reduced)

- Pulsar binary protocol over TCP (default `:6650`)
- Persistent and non-persistent topics (`persistent://` / `non-persistent://`)
- JWT authentication (HS256) with role-based policies
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
  - SQLite log per topic (`messages`) for persistent topics
  - Normalized schema (tenant → namespace → topic)
  - Subscription cursor and pending delivery tables
  - Shared subscription delivery (round-robin)
  - Non-persistent topics are memory-only
- Messaging control plane (HCL) with Lua functions and bindings
- Prometheus metrics endpoint
- Optional synthwave TUI dashboard

---

## Roadmap (short-term, deliberately prioritized)

**Short-term goals:**

- ✅ Stabilize the protocol stack
- ✅ Improve edge ergonomics (smaller, simpler, more reliable)
- 🔜 **TLS support** (transport encryption as a requirement for real edge deployments)

**Why TLS now:**

- Edge systems often communicate over public networks or insecure Wi‑Fi.
- TLS is the minimum required for confidentiality and integrity.
- It enables secure client‑to‑broker connections without extra tunnels.

---

## Project layout

- `cmd/minipulsar`
  - CLI entrypoint, flags, logging, config
- `internal/broker`
  - Connection lifecycle, protocol handlers, delivery orchestration
- `internal/storage`
  - SQLite schema and persistence primitives
- `internal/protocol`
  - Pulsar wire framing helpers
- `pb/PulsarApi.proto`
  - Pulsar protocol definition (Go types via `protoc`)

---

## Requirements

- Go >= 1.21
- `protoc` + `protoc-gen-go`

Install the Go plugin if needed:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
export PATH="$PATH:$HOME/go/bin"
```

---

## Build

```bash
make generate   # generate pb/PulsarApi.pb.go
make build      # compile ./bin/minipulsar
```

---

## Run

```bash
./bin/minipulsar \
  -addr :6650 \
  -db ./minipulsar.db
```

---

## TLS (recommended for non-local deployments)

Minipulsar can listen with TLS by providing a certificate and private key.

### Quick self-signed cert (local/dev)

```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout server.key -out server.crt \
  -days 365 -subj "/CN=localhost"
```

Run the broker with TLS:

```bash
./bin/minipulsar \
  -addr :6650 \
  -db ./minipulsar.db \
  -tls-cert ./server.crt \
  -tls-key ./server.key
```

### mTLS (optional client certificate verification)

Create a simple CA and sign a server cert:

```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout ca.key -out ca.crt \
  -days 365 -subj "/CN=minipulsar-ca"

openssl req -newkey rsa:2048 -nodes \
  -keyout server.key -out server.csr \
  -subj "/CN=localhost"

openssl x509 -req -in server.csr \
  -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 365
```

Start the broker and require client certificates:

```bash
./bin/minipulsar \
  -addr :6650 \
  -db ./minipulsar.db \
  -tls-cert ./server.crt \
  -tls-key ./server.key \
  -tls-client-ca ./ca.crt
```

---

## Quickstart (JWT auth + policies)

```bash
export MINIPULSAR_JWT_SECRET="dev-secret"
./bin/minipulsar \
  -addr :6650 \
  -db ./minipulsar.db \
  -messaging-config ./examples/messaging.hcl
```

Tiny HS256 JWT script (no external dependencies):

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

Set the token as `Authorization: Bearer <token>` in your Pulsar client,
or pass it in `CONNECT` as `auth_data`.

---

## Messaging config (HCL)

The messaging control plane config is optional. It can:

- define namespace authorization policies,
- register Lua functions,
- bind a source topic through a function into a target topic.

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
  max_runtime = "250ms"
}

binding {
  source = "persistent://public/default/temperature.f"
  function = "transform"
  target = "persistent://public/default/temperature.c"
}
```

`max_runtime` is optional per function. When set, the Lua worker aborts execution
that exceeds the duration (e.g. `250ms`, `2s`). Omitted or empty means unlimited.

### Security modes

`mode` controls policy strictness:

- `strict`: namespace rules are enforced; without a match, access is denied.
- `open`: all authorization checks are skipped (local/testing).

---

## CLI flags (selection)

- `-addr` – listen address for Pulsar protocol
- `-db` – path to SQLite DB
- `-broker-url` – broker URL for `LOOKUP`
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
- `-jwt-secret` – HS256 secret (or set `MINIPULSAR_JWT_SECRET`)
- `-tui` – enable synthwave TUI
- `-tls-cert` – TLS server certificate PEM (enables TLS)
- `-tls-key` – TLS server private key PEM
- `-tls-client-ca` – optional client CA bundle (mTLS)

---

## Protocol flow (typical client session)

1. Client sends `PARTITIONED_METADATA` → broker replies with 0 partitions.
2. Client sends `LOOKUP` → broker responds pointing to itself.
3. Client sends `CONNECT` → broker replies with `CONNECTED`.
4. Client sends `PRODUCER` → broker replies with `PRODUCER_SUCCESS`.
5. Client sends `SEND` → broker persists payload and replies `SEND_RECEIPT`.
6. Client sends `SUBSCRIBE` → broker replies with `SUCCESS`.
7. Client sends `FLOW` → broker delivers messages as `MESSAGE` frames.
8. Client sends `ACK` → broker removes pending entries for that consumer.
9. `PING`/`PONG` keepalives.

---

## Notes

This project is for **learning, prototyping, and edge experiments**.
For production environments, full Apache Pulsar is the right choice.

If you want to help make the **Edge Broker** real:
issues, feedback, and PRs are welcome.

---

## Author

minipulsar is an experiment by Matthias Petermann. More background and write‑ups at: https://www.petermann-digital.de/blog/  
If you need support beyond the open-source scope, feel free to reach out.
