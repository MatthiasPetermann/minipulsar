# Protocol framing

Minipulsar implements a minimal subset of the Pulsar binary protocol. The wire
format is handled in `internal/protocol` and decoded in `internal/broker`.

## Inbound frame parsing

`Broker.handleFrame` reads a length-prefixed frame:

1. 4 bytes: total frame size.
2. 4 bytes: command size.
3. `command size` bytes: protobuf-encoded `BaseCommand`.
4. Remaining bytes (if any): payload section for `SEND` commands.

The broker enforces a maximum frame size to prevent unbounded memory usage.

## Outbound command frames

`protocol.WriteSimpleCommand` writes command-only frames:

- It marshals a `BaseCommand` protobuf.
- It prefixes the command with the total frame size and command size.

This is used for control-plane responses such as `CONNECTED`, `SUCCESS`,
`PRODUCER_SUCCESS`, `LOOKUP_RESPONSE`, and `SEND_RECEIPT`.

## Outbound message frames

`protocol.WriteMessageFrame` writes the Pulsar message frame format:

1. Command portion:
   - `BaseCommand` with type `MESSAGE` and the consumer ID.
2. Message metadata + payload:
   - Magic number `0x0e01`.
   - CRC32C checksum of `[metadata size][metadata][payload]`.
   - 4-byte metadata size.
   - Protobuf-encoded `MessageMetadata`.
   - Raw payload bytes.

The message ID uses the SQLite row ID as the Pulsar entry ID, which makes the
storage cursor directly compatible with delivery ordering.

## Supported commands

Minipulsar currently handles a subset of the Pulsar commands:

- `CONNECT` / `CONNECTED`
- `PARTITIONED_METADATA` / `PARTITIONED_METADATA_RESPONSE` (always 0 partitions)
- `LOOKUP` / `LOOKUP_RESPONSE` (broker responds with its own service URL)
- `PRODUCER` / `PRODUCER_SUCCESS`
- `SEND` / `SEND_RECEIPT`
- `SUBSCRIBE` / `SUCCESS`
- `FLOW` / `MESSAGE`
- `ACK` (individual only)
- `PING` / `PONG`
- `CLOSE_PRODUCER` / `SUCCESS`
- `CLOSE_CONSUMER` / `SUCCESS`

Other command types are currently logged as unhandled.
