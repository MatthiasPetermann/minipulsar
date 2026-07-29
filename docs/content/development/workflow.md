---
title: "Development workflow"
weight: 41
---

# Development workflow

## Toolchain

The module targets Go 1.21. Building generated protocol bindings requires
`protoc` and `protoc-gen-go`; normal development only needs Go and the C toolchain
required by `github.com/mattn/go-sqlite3`.

```bash
make generate  # regenerate pb/PulsarApi.pb.go from pb/PulsarApi.proto
make build     # build bin/minipulsar
make test      # go test ./...
go test -race ./...
go vet ./...
```

Never hand-edit `pb/PulsarApi.pb.go`. Change `pb/PulsarApi.proto`, regenerate,
then update protocol dispatch, compatibility documentation, and socket tests.

## Change discipline

1. Define expected wire and storage semantics first.
2. Change the narrowest layer that owns the invariant.
3. Add storage tests for transaction/cursor invariants.
4. Add a socket integration test for client-visible commands.
5. Run regular tests, race tests, `go vet`, and formatting.
6. Update the compatibility matrix and codebase reference.

## Where to add a feature

| Need | Primary files |
|---|---|
| New Pulsar command | `pb/PulsarApi.proto`, `internal/broker/framing.go`, `internal/broker/handlers.go`, socket tests |
| Durable delivery state | `internal/storage/storage.go`, storage tests, cleanup/metrics as appropriate |
| Subscription scheduling | `internal/broker/delivery.go`, `state.go`, broker tests |
| Security policy | `internal/messaging/config.go`, `security_ir.go`, `internal/broker/authorization.go` |
| Wire encoding | `internal/protocol/wire.go`, protocol tests |
| Operator data | `internal/broker/stats.go`, metrics or TUI package |

## Test layers

- Storage tests use real temporary SQLite databases and prove cursor, pending,
  ack, cleanup, reset, and redelivery behavior.
- Package tests cover parsing, serialization, policy compilation, Lua execution,
  logging, metrics rendering, CLI parsing, and TUI layout.
- `socket_integration_test.go` uses real TCP connections and Pulsar protobuf
  frames to exercise sessions end to end.

Do not treat a passing unit test as full client compatibility. Keep an external
client matrix for the Go, Java, and Python Pulsar clients when extending the
protocol.
