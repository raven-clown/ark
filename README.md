# ARK

Config-driven middleware that connects Kafka topics to plain HTTP apps:
consume → validate/route → callback → produce, with an MCP interface so
agents can operate it. See [PLAN.md](PLAN.md) for the full design and
roadmap.

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
