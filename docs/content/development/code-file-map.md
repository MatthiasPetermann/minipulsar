---
title: "Code file map"
weight: 42
---

Use this page as the fastest entry point into the repository. Arrows show the
normal direction of dependency and runtime calls; they do not imply that every
file imports every other file in its cluster.

```mermaid
flowchart TB
    CLI["cmd/minipulsar/main.go<br/>CLI assembly, signals, listeners"]
    PB["pb/PulsarApi.proto<br/>pb/PulsarApi.pb.go<br/>wire types"]
    CLI --> B
    CLI --> OBS
    B --> PB
    P --> PB

    subgraph B["internal/broker: session and delivery core"]
        T["types.go<br/>Config, Broker, state"]
        S["server.go<br/>listeners, connections, shutdown"]
        F["framing.go<br/>read, decode, dispatch, errors"]
        H["handlers.go<br/>Pulsar commands"]
        PU["publish.go<br/>persistent vs ephemeral route"]
        D["delivery.go<br/>permits and scheduling"]
        ST["state.go<br/>subscription state"]
        NP["non_persistent.go<br/>best-effort delivery"]
        CL["cleanup.go / ack_timeout.go<br/>requeue paths"]
        PA["producer_access.go<br/>local arbitration"]
        AU["auth.go / authorization.go<br/>JWT and policy gate"]
        MA["maintenance.go<br/>retention scheduler"]
        TH["throttle.go / stats.go<br/>operator controls"]
        TY["subscription_types.go<br/>feature gate"]
        S --> F --> H
        H --> PU --> D
        H --> ST
        H --> AU
        D --> ST
        PU --> NP
        CL --> D
        MA --> CL
        PA --> H
        TH --> D
        TY --> H
    end

    subgraph DB["internal/storage: SQLite durable state"]
        SO["storage.go<br/>schema, insert, claim, ack, seek"]
        SC["cleanup.go<br/>retention and timeout cleanup"]
        SS["stats.go<br/>totals and backlog"]
        SU["subscriptions.go<br/>activity touch"]
        SO --> SC
        SO --> SS
        SU --> SO
    end

    B --> SO
    MA --> SC
    TH --> SS

    subgraph P["internal/protocol and topic"]
        WI["protocol/wire.go<br/>command and MESSAGE frames"]
        PR["protocol/properties.go<br/>KeyValue conversion"]
        TO["topic/topic.go<br/>normalization and parsing"]
        WI --> PR
        TO --> SO
    end

    H --> WI
    H --> TO
    D --> WI

    subgraph M["internal/messaging: optional control plane"]
        MC["config.go<br/>HCL decoding"]
        MR["runtime.go<br/>compiled policy and bindings"]
        MS["security_ir.go<br/>authorization IR"]
        ML["lua_pool.go<br/>bounded Lua workers"]
        MC --> MR
        MR --> MS
        MR --> ML
    end

    CLI --> MC
    AU --> MS
    H --> ML

    subgraph OBS["operator adapters"]
        LO["logging/logging.go<br/>structured logger"]
        ME["metrics/metrics.go<br/>Prometheus snapshots"]
        TU["tui/tui.go<br/>Bubble Tea control deck"]
        LH["tui/log_hook.go<br/>log stream bridge"]
        LO --> LH --> TU
    end

    B --> LO
    TH --> ME
    TH --> TU
```

## Read a feature end to end

| Question | Start here | Follow next |
|---|---|---|
| Why did a client receive an error? | `broker/framing.go` | `broker/handlers.go`, `protocol/wire.go` |
| Where was this message persisted? | `broker/publish.go` | `storage/storage.go` |
| Why was a message replayed? | `broker/cleanup.go` or `ack_timeout.go` | `storage/storage.go`, `storage/cleanup.go` |
| Why is a consumer idle? | `broker/delivery.go` | `handlers.go` FLOW, `state.go` |
| Why was access denied? | `broker/authorization.go` | `broker/auth.go`, `messaging/security_ir.go` |
| Why is producer latency high? | `broker/handlers.go` SEND | `messaging/lua_pool.go`, `storage/storage.go` |
| Why does a dashboard number look wrong? | `broker/stats.go` | `storage/stats.go`, `metrics/metrics.go`, `tui/tui.go` |

## Test map

Every handwritten package has focused tests. `socket_integration_test.go` is the
cross-layer proof point for a real TCP session. Storage tests exercise actual
temporary SQLite files. Start with the matching `*_test.go` file before changing
an invariant, then add a socket test whenever a Pulsar client can observe the
new behavior.

For the exhaustive per-file explanation, continue to the
[Codebase reference](../codebase-reference/).
