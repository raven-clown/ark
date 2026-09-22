# ARK

Config-driven middleware that connects Kafka topics to plain HTTP apps:
consume → validate/route → callback → produce. Retries, dead-lettering,
a circuit breaker, and horizontal scaling within one process are built
in, so the app on the other end of the callback never touches a Kafka
client library. See [PLAN.md](PLAN.md) for the full design and roadmap.

## Status

Phase 1 (core engine) and Phase 2 (reliability) are done and verified
against a live Kafka. REST API, Prometheus metrics, and pause/resume
are partially built. Rule engine (fast-path/post-callback rules), MCP
server, and multi-node clustering are designed in PLAN.md but not yet
implemented.

## Features

- At-least-once delivery: offsets commit only after a successful
  produce, so a crash mid-message never loses it
- Retry with backoff on both the HTTP callback and the produce step
- Dead-letter and reject topics for message-specific failures; a
  destination-wide outage blocks in place instead of draining into DLQ
- Circuit breaker with an optional `target.health_check_url` to detect
  recovery without waiting for new traffic
- `workers: N` per pipeline — parallel consumer-group members in one
  process, no extra deploys needed
- `POST /api/v1/pipelines/{name}/pause` and `/resume`
- `GET /metrics` (Prometheus): throughput, callback latency, consumer
  lag, circuit breaker state, worker liveness, last-activity timestamp

## Repo layout

- `bridge-engine/` — the Go engine (consumer, rule engine, callback
  client, producer, REST API, MCP server)
- `bridge-ui/` — dashboard UI, deployed separately from the engine

## Running locally

```
docker compose up
```

Starts a single-node Kafka broker and the bridge engine wired to
`bridge-engine/config.example.yaml`. Kafka's own data directory is a
named volume by default; point it at another disk or mount with:

```
KAFKA_DATA_DIR=/mnt/other-disk/kafka docker compose up
```

To run the engine directly:

```
cd bridge-engine
go run ./cmd/bridge -config config.example.yaml
```

## API

- `GET /healthz` — liveness
- `GET /metrics` — Prometheus metrics
- `GET /api/v1/pipelines` — status for every pipeline worker
- `GET /api/v1/pipelines/{name}` — status for one pipeline's workers
- `POST /api/v1/pipelines/{name}/pause` / `/resume`
