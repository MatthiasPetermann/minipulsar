# Runtime entrypoint & configuration

The runtime entrypoint lives in `cmd/minipulsar/main.go`. It is intentionally
thin: it parses CLI flags, builds the supporting subsystems, and starts the
broker listeners.

## Configuration flow

1. **Flags & environment**
   - All CLI flags are defined in `main` and parsed using `flag`.
   - The JWT secret defaults to the `MINIPULSAR_JWT_SECRET` environment variable.
   - TLS flags (`-tls-cert`, `-tls-key`) are optional; the TLS listener is only
     started when both are provided.

2. **Logging**
   - The logging subsystem is built first so that subsequent setup errors are
     emitted consistently.
   - `internal/logging` wraps `slog` and supports text or JSON output as well as
     optional timestamps.

3. **Storage**
   - `internal/storage.Open` opens SQLite and applies connection pooling.
   - `InitSchema` creates or migrates the required tables.

4. **Messaging runtime (optional)**
   - When `-messaging-config` is provided, `internal/messaging.LoadConfig` parses
     the HCL file and `BuildRuntime` validates policies, functions, and bindings.

5. **TLS setup**
   - `buildTLSConfig` loads a certificate/key pair and enforces TLS 1.2.
   - If TLS is disabled, the advertised TLS broker URL is cleared to prevent
     LOOKUP responses from advertising TLS endpoints.

6. **TUI and standard modes**
   - When `-tui` is enabled, logs are routed into a `tui.LogWriter`, the broker
     is started, and the Bubble Tea program renders stats plus log output.
   - Otherwise, the broker runs headless and logs to stdout.

7. **Metrics**
   - The Prometheus metrics server is started if `-metrics-addr` is non-empty.

8. **Broker listeners**
   - The broker can listen on a plaintext listener and an optional TLS listener.
   - `startBrokerListeners` starts both concurrently and returns an error channel
     that is awaited in the main goroutine.

## Why the CLI owns wiring

The intent is to keep the broker package focused on protocol and storage
behavior. All environment-specific concerns (TLS, metrics, logging format, TUI,
etc.) are configured at the CLI level and passed down via the broker
configuration struct. This keeps the core implementation small and easy to test
in isolation.
