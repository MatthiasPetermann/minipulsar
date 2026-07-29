---
title: "Observability and TUI"
weight: 33
---

## Logs and Prometheus

The logger supports text or JSON output. Use `-log-level` and `-log-format` for
machine-readable integration. The optional Prometheus endpoint is configured
with `-metrics-addr`, `-metrics-path`, `-metrics-interval`, and
`-metrics-top-topics`.

Metrics report connected producers/consumers, durable object totals, pending
messages, stored messages, top topic pressure, optional subscription backlog,
and collector health. Topic and subscription labels are bounded by the top-N
limit to avoid unbounded cardinality.

## Control deck

Run with `-tui` to open the Bubble Tea operations interface. It uses a
full-terminal canvas and four views:

| View | Keys | Purpose |
|---|---|---|
| Overview | `1` | Health, topology, memory, throughput, and pending trends. |
| Topics | `2`, `j`/`k` | Topic storage and pending-delivery pressure. |
| Backlog | `3`, `j`/`k` | Subscription backlog drill-down when policies expose it. |
| Logs | `4`, `j`/`k`, `pgup`/`pgdown` | Buffered structured log stream. |

Across views, `Tab` changes pages, `r` refreshes, `d` rotates global delay,
`Space` pauses ingress and delivery, `l` changes the log threshold, `f` toggles
log follow, `c` clears logs, and `q` exits the TUI. Pause and delay are intended
for diagnostics, not as a durable traffic-management system.
