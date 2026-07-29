---
title: "Lua bindings and execution model"
weight: 36
---

Lua bindings are an in-process transformation feature, not a distributed
function platform. Paths are resolved at process startup from its working
directory; scripts are loaded once per worker and are not hot-reloaded.

```mermaid
sequenceDiagram
    participant P as Producer
    participant B as Broker
    participant DB as Source storage
    participant W as Lua worker pool
    participant T as Target storage
    P->>B: SEND(source payload)
    B->>DB: Commit source message
    B->>W: Submit task
    Note over B,W: Unbuffered queue: SEND waits if workers are busy
    W-->>B: transformed bytes or error
    alt transform succeeds
        B->>T: Publish target bytes directly
    else transform fails
        B->>B: Log warning; keep source committed
    end
    B-->>P: SEND_RECEIPT
```

The pool has a fixed number of worker goroutines and an unbuffered task channel.
When all workers are busy, the producer's SEND handler blocks before its receipt.
Bindings for one source are executed sequentially; the first execution or target
publish failure stops remaining bindings for that source send. Source persistence
is not rolled back and the producer still receives a source receipt after a
binding warning.

Each worker exposes only Lua Base, Table, String, and Math libraries. Scripts
on one worker share a Lua state; global changes can therefore affect later calls
on that worker. Set `max_runtime` for every untrusted script. Without it, the
execution has no deadline. Function-pool workers are not explicitly joined at
broker shutdown.

Targets are published directly through the broker storage path and do **not**
recursively trigger another binding lookup. Binding cycles therefore do not
recurse in the current implementation, but they still create confusing topology
and should be avoided.
