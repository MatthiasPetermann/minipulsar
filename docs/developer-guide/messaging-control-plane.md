# Messaging control plane (HCL + Lua)

The messaging control plane is an optional layer that adds:

- **Authorization policies** (namespace-based allowlists).
- **Namespace retention and subscription timeouts**.
- **Lua functions and topic bindings** for in-broker transformations.

It is configured by an HCL file and compiled into a runtime representation.

## Configuration model

The HCL schema is defined in `internal/messaging/config.go` and supports:

- `security` block: chooses authorization mode (`strict` or `open`).
- `namespace` blocks: define allowed `produce`/`consume` roles plus retention and
  subscription timeouts.
- `function` blocks: define Lua scripts and optional max runtime.
- `binding` blocks: connect a source topic to a function and a target topic.

The example configuration in `examples/messaging.hcl` demonstrates the
structure.

## Security IR

`BuildSecurityIR` validates and normalizes namespace policies into an efficient
in-memory representation that supports fast access checks. Each namespace
contains allowlists per action (`produce` and `consume`).

Authorization flow:

1. The broker extracts roles from JWTs in `CONNECT`.
2. The broker maps a topic to a namespace string.
3. The security IR checks whether any role is allowed for the action.

If security is configured in **strict** mode, unknown namespaces are denied. In
**open** mode, authorization is bypassed even if the config includes policies.

## Namespace policies

Namespace policies are compiled into `NamespacePolicy` entries that store:

- `SubscriptionTimeout`: how long to retain inactive subscriptions.
- `Retention`: how long to keep messages after consumption.

The broker reads these policies during its periodic maintenance pass to prune
subscriptions and messages.

## Lua function registry

`BuildRuntime` validates the Lua function list by:

- Enforcing function IDs to be alphanumeric plus dashes.
- Checking each script file exists on disk.
- Parsing optional runtime limits (e.g., `250ms`).

The registry is used by a worker pool to execute functions concurrently.

## Lua execution model

The Lua worker pool (`FunctionPool`) is built with a fixed number of workers.
Each worker:

- Loads all configured Lua scripts once at startup.
- Exposes a single entrypoint named `handle` in each script.
- Executes the function with two parameters: `payload` and a context table
  containing `function_id`, `source_topic`, and `target_topic`.

If `max_runtime` is configured, execution is bounded with a context timeout.

## Bindings

Bindings connect a source topic to a function and target topic. When a message
is produced on the source topic:

1. The broker executes the Lua function with the incoming payload.
2. The function returns a transformed payload string.
3. The broker writes a new message to the target topic (persistent or
   non-persistent depending on the target topic scheme).

Bindings are resolved at runtime via `Runtime.BindingsFor(sourceTopic)`.
