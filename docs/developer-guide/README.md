# Minipulsar Developer Guide

Welcome to the developer guide for **minipulsar**. This guide documents the
current implementation in detail, both as discrete components and as a full
system, so you can reason about behavior, extend it, or debug it effectively.

## Table of contents

1. [Project layout](./project-layout.md)
2. [Runtime entrypoint & configuration](./runtime-entrypoint.md)
3. [Broker core (connections, commands, delivery)](./broker-core.md)
4. [Protocol framing](./protocol-framing.md)
5. [Storage layer](./storage-layer.md)
6. [Messaging control plane (HCL + Lua)](./messaging-control-plane.md)
7. [Messaging flow specification (end-to-end)](./messaging-flow-spec.md)
8. [Observability & operator UX](./observability-ux.md)
9. [Development workflow](./development-workflow.md)

## How to use this guide

- **Read in order** if you are new to the codebase. Each section builds on the
  previous one to show how configuration flows into runtime behavior.
- **Jump to a subsystem** when you want implementation details: every section
  links directly to the relevant packages and call paths.

## Commenting conventions

Minipulsar favors Go-style doc comments and short inline notes that explain the
intent behind broker actions, especially where multiple layers interact.

- **Public types and functions** include doc comments describing their purpose,
  the layer they belong to, and the upstream/downstream dependencies they serve.
- **Cross-layer flow** (for example broker → storage or broker → messaging) is
  called out inline so readers can trace why work is delegated.
- **Operational helpers** (metrics, TUI, logging) include comments explaining
  how they surface broker state without coupling to core logic.

When you add new behavior, update comments to reflect *why* the logic exists
and which layer consumes it so the guide and code stay aligned.

## Architectural summary

Minipulsar is a small Pulsar-compatible broker with these major runtime layers:

1. **CLI / runtime assembly**: `cmd/minipulsar` wires configuration, storage,
   messaging runtime, metrics, and broker listeners.
2. **Broker core**: `internal/broker` owns connections, protocol command handling,
   subscriptions, delivery loops, and authorization.
3. **Wire protocol helpers**: `internal/protocol` writes Pulsar-compatible frames.
4. **Persistence**: `internal/storage` handles SQLite schema, cursor tracking,
   pending acknowledgements, and cleanup.
5. **Messaging control plane**: `internal/messaging` loads HCL policies, Lua
   functions, and topic bindings.
6. **Operator features**: `internal/metrics`, `internal/logging`, and `internal/tui`
   provide Prometheus metrics, structured logging, and an optional terminal UI.

The rest of the guide expands each layer in isolation and then shows the
end-to-end request/response flow across those layers.
