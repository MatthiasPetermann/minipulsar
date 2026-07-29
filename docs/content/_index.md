---
title: "minipulsar"
weight: 1
---

# minipulsar

minipulsar is a compact, single-node broker that speaks the Apache Pulsar binary
protocol. It is designed for learning, prototypes, and constrained edge
deployments where a full Pulsar cluster is not appropriate. It preserves the
useful core: producers, persistent subscriptions, flow permits, acknowledgements,
redelivery, SQLite durability, and standard Pulsar client framing.

It is **not** a replacement for a replicated Apache Pulsar deployment. There is
no partition ownership, BookKeeper ledger layer, multi-broker routing, or
replication. Payloads are intentionally opaque bytes: schemas are not a product
goal.

## Read this site by role

- **Application integrator:** start with [Quick start](getting-started/quick-start/), then
  [Messaging semantics](concepts/messaging-semantics/) and [Protocol compatibility](reference/protocol-compatibility/).
- **Operator:** use [Configuration and lifecycle](operations/configuration/),
  [Security](operations/security/), and [Observability](operations/observability/).
- **Developer:** begin with [Architecture](concepts/architecture/), then the
  [Codebase reference](development/codebase-reference/) and [Development workflow](development/workflow/).
- **Control-plane integrator:** read [HCL and Lua control plane](concepts/control-plane/)
  before deploying topic bindings.

## Design principles

1. **Small, explicit runtime.** The binary has a narrow dependency surface and
   starts a TCP listener directly.
2. **Correctness before breadth.** Persistent delivery records a claim before a
   message is written to a consumer socket.
3. **Pulsar where clients need it.** The wire protocol and core session flow are
   compatible; advanced cluster features are deliberately absent.
4. **Opaque payloads.** The broker validates Pulsar framing and preserves bytes,
   but does not impose schemas or decode application data.
