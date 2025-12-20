# minipulsar – Minimal Pulsar-kompatibler PoC-Broker

**Ziel:** Ein bewusst minimalistischer Apache-Pulsar-kompatibler Broker in Go
für lokale Tests, Experimente und Protokoll-Inspection – kein Ersatz für einen
echten Pulsar-Cluster.

## Features (bewusst stark reduziert)

- Pulsar-Binärprotokoll über TCP (Standard-Port `:6650`)
- Unterstützte Commands:
  - `CONNECT` / `CONNECTED`
  - `PARTITIONED_METADATA` / `PARTITIONED_METADATA_RESPONSE` (immer 0 Partitionen)
  - `LOOKUP` / `LOOKUP_RESPONSE` (CommandLookupTopic, redirectet auf sich selbst)
  - `PRODUCER` / `PRODUCER_SUCCESS`
  - `SEND` / `SEND_RECEIPT`
  - `SUBSCRIBE` / `SUCCESS`
  - `FLOW` / `MESSAGE`
  - `ACK` (nur Logging)
  - `PING` / `PONG`
- Persistenz:
  - SQLite-Log pro Topic in Tabelle `messages`
  - Sehr einfache Offsets pro Consumer (In-Memory, ab Start-ID 1)
- Kein:
  - Authentifizierung
  - TLS
  - Partitions (wir tun so, als wäre alles „non-partitioned“)
  - Schema-Registry
  - Transactions
  - Topic-Migration
  - DLQ, Retention, Policies etc.

## Aufbau

- `pb/PulsarApi.proto`
  - Vollständige Pulsar-Protokolldefinition (Apache 2.0, mit `go_package`-Option)
  - Wird per `protoc-gen-go` nach `pb/PulsarApi.pb.go` generiert
- `cmd/minipulsar/main.go`
  - Implementiert einen minimalen Broker mit:
    - TCP-Server
    - Protokoll-Decoder
    - SQLite-Persistenz
    - Einfachen Handlern für die wichtigsten Commands

## Voraussetzungen

- Go >= 1.21
- `protoc` + `protoc-gen-go` im `PATH`

Beispielinstallation für das Go-Plugin:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
export PATH="$PATH:$HOME/go/bin"
```

## Build

```bash
make generate   # erzeugt pb/PulsarApi.pb.go aus pb/PulsarApi.proto
make            # ruft generate + go build ./... auf
```

Der Broker liegt anschließend z. B. unter:

```bash
./cmd/minipulsar/minipulsar
```

## Start

```bash
./cmd/minipulsar/minipulsar -addr :6650 -db ./minipulsar.db
```

- `-addr` – Listen-Adresse für das Pulsar-Binärprotokoll
- `-db` – Pfad zur SQLite-Datei (wird bei Bedarf erzeugt)

## Kompatibilität

Getestet ist der Broker darauf ausgelegt, mit einem normalen Pulsar-Go-Client
zu sprechen. Typischer Ablauf:

1. Client: `PARTITIONED_METADATA` → Broker: `PARTITIONED_METADATA_RESPONSE` (0 Partitionen)
2. Client: `LOOKUP` (CommandLookupTopic) → Broker: `LOOKUP_RESPONSE` mit `Connect` auf `pulsar://localhost:6650`
3. Client: `CONNECT` → Broker: `CONNECTED`
4. Client: `PRODUCER` → Broker: `PRODUCER_SUCCESS`
5. Client: `SEND` → Broker:
   - speichert Message in SQLite
   - antwortet mit `SEND_RECEIPT`
6. Client: `SUBSCRIBE` → Broker: `SUCCESS`
7. Client: `FLOW` → Broker: liefert Messages als `MESSAGE`-Frames
8. Client: `ACK` → Broker: loggt, ändert aber keine Offsets
9. Client/Broker: `PING`/`PONG`

Für ernsthaften Einsatz brauchst du selbstverständlich einen echten Pulsar-Broker.
Dieses Projekt ist eine Lern- und Testplattform.
