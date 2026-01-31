# Project layout

This section describes where functionality lives in the repository and how the
packages fit together at a high level.

## Top-level structure

- `cmd/minipulsar`: CLI entrypoint that assembles all runtime dependencies and
  starts listeners.
- `internal/broker`: Core broker logic, connection lifecycle, command handlers,
  delivery loops, and authorization hooks.
- `internal/protocol`: Pulsar frame serialization utilities.
- `internal/storage`: SQLite-backed persistence and retention cleanup.
- `internal/messaging`: HCL config loader, security policy IR, Lua runtime, and
  bindings between topics.
- `internal/metrics`: Prometheus endpoint and periodic broker stats collection.
- `internal/logging`: Uniform slog-based logger wrapper.
- `internal/tui`: Optional Bubble Tea dashboard and log streaming helper.
- `internal/topic`: Parsing utilities for Pulsar topic names.
- `pb`: Pulsar protocol protobuf definition and generated Go bindings.
- `examples`: Sample HCL config and Lua function.

## Go module organization

The module root (`go.mod`) defines `minipulsar` as the module path. All internal
packages are referenced through `minipulsar/internal/...`. The generated Pulsar
protocol types live in `minipulsar/pb` and are imported by the broker and
protocol helpers.

## Navigation tips

If you are reading the code to follow a request, this is the typical path:

1. `cmd/minipulsar/main.go` bootstraps the broker.
2. `internal/broker/server.go` accepts a client connection.
3. `internal/broker/framing.go` reads and dispatches a command frame.
4. `internal/broker/handlers.go` executes the command (e.g., PRODUCER, SEND,
   SUBSCRIBE, ACK).
5. `internal/storage` persists messages and updates cursor/pending state.
6. `internal/protocol` writes responses or message frames back to the client.

The rest of the guide dives into each package to explain the behavior behind
these steps.
