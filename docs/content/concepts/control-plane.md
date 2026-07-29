---
title: "HCL and Lua control plane"
weight: 23
---

# HCL and Lua control plane

The messaging configuration is optional. It supplies three independent concerns:
namespace authorization/retention policy, named Lua functions, and bindings that
republish transformed bytes from a source topic to a target topic.

## Configuration shape

```hcl
security {
  mode = "strict"
}

namespace "persistent://public/default" {
  produce = ["writer"]
  consume = ["reader"]
  retention_seconds = 300
  subscription_timeout_seconds = 3600
}

function "transform" {
  path = "transform.lua"
  max_runtime = "250ms"
}

binding {
  source = "persistent://public/default/input"
  function = "transform"
  target = "persistent://public/default/output"
}
```

The Lua source must expose `handle(payload, ctx)`. Both payload and result are
bytes. A function binding is not an Apache Pulsar Functions deployment: it runs
in the minipulsar process, uses a bounded worker pool, and has no external state
or distributed checkpointing.

## Binding publish path

```mermaid
sequenceDiagram
    participant P as Producer
    participant B as Broker
    participant S as Source SQLite topic
    participant L as Lua worker pool
    participant T as Target topic
    P->>B: SEND(source, opaque bytes)
    B->>S: Persist source message
    B->>L: Execute handle(payload, context)
    L-->>B: Transformed bytes
    B->>T: Publish transformed bytes
    B-->>P: SEND_RECEIPT for source message
```

Bindings run synchronously after source publication and before the producer
receipt is written. A function failure is logged and does not roll back the
already persisted source message. Design transformations to be bounded, pure,
and idempotent; avoid binding cycles because the runtime does not provide a
cycle detector.

## Namespace policy consequences

- `produce` and `consume` lists are role allowlists.
- `retention_seconds` controls cleanup age for consumed and orphaned messages.
- `subscription_timeout_seconds` removes subscriptions that have not been
  actively served for the configured duration.
- Policies apply only to persistent namespace topics; non-persistent topics
  remain memory-only delivery paths.
