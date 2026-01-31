# Development workflow

This section covers the local build, code generation, and test workflow based
on the repository's Makefile and existing tests.

## Build and code generation

The Makefile provides convenience targets:

- `make generate`: runs `protoc` to regenerate `pb/PulsarApi.pb.go` from the
  Pulsar protobuf definition.
- `make build`: compiles the broker binary to `bin/minipulsar`.
- `make clean`: removes build output and regenerated protobuf bindings.

The protobuf definition in `pb/PulsarApi.proto` is copied from Apache Pulsar and
kept in the repository so that the broker can marshal and unmarshal Pulsar wire
frames without external dependencies.

## Tests

The codebase includes unit tests for:

- `internal/protocol` framing helpers.
- `internal/topic` parsing.
- `internal/storage` persistence, cursors, and cleanup behaviors.

Run tests with `go test ./...` (or via `make test`). The tests are primarily
focused on ensuring SQLite semantics and protocol framing are stable.

## Examples

The `examples` directory contains:

- `messaging.hcl`: a sample messaging control-plane configuration.
- `transform.lua`: a Lua function for message transformation.

These examples are designed to be used together when enabling the messaging
runtime with `-messaging-config` and function bindings.
