---
name: run-local-kafka
description: Bring up a local Kafka broker plus ARK for real end-to-end verification, not just unit tests. Use before claiming any Kafka-facing change (consumer, producer, cluster, hot-reload) actually works.
---

# Verifying against real Kafka

ARK's own rule: verify against real infrastructure, not just `go test`.
`docker-compose.yml` at the repo root defines `kafka` (apache/kafka:3.8.0,
KRaft mode, single node, port 9092), `bridge` (builds from
`./bridge-engine`, mounts `bridge-engine/config.demo.yaml` or `$ARK_CONFIG`,
API on 8080) and `demo-echo` (a callback target on 8081 that answers 400
for `"invalid": true` and 500 for `"fail": true`).

On Windows/Git Bash, prefix every docker command with `MSYS_NO_PATHCONV=1`.
On this cloud container, no prefix is needed.

Bring up just Kafka to drive the bridge manually:
```
docker compose up -d kafka
docker compose logs -f kafka   # wait for the healthcheck to pass
```

Bring up the whole stack:
```
docker compose up --build
```

Useful checks once Kafka is up (from inside the kafka container, or with a
local kafka CLI pointed at localhost:9092):
```
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic> --from-beginning
```

For crash-recovery or cluster tests: kill a bridge/kafka container with
`docker compose stop <service>` or `docker kill <container>`, confirm the
remaining nodes react (rebalance, placement reassignment, resumed
consumption), then bring it back with `docker compose start <service>` and
confirm it rejoins correctly.

Tear down: `docker compose down -v` (drops the `kafka-data` volume too;
only do this between unrelated test runs, not mid-test).
