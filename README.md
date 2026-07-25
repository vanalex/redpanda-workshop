# Redpanda Workshop

A hands-on, progressive workshop for learning [Redpanda](https://redpanda.com) — a
Kafka-API-compatible streaming platform. It starts with a local multi-broker cluster and
the basics of the Kafka protocol, moves through failover and Rust clients, and ends with
schema management, high availability on Kubernetes, and Redpanda Connect pipelines
(including a streaming lakehouse into Apache Iceberg).

Each module lives in its own folder and can mostly be run independently, but the modules
below build on one another and share the same local cluster unless noted otherwise.

## Prerequisites

- [Docker](https://www.docker.com/) and Docker Compose
- [rpk](https://docs.redpanda.com/current/get-started/rpk-install/) (ships inside every
  broker container too, via `docker compose exec redpanda rpk ...`)
- [Rust](https://www.rust-lang.org/tools/install) (stable toolchain) for the producer/consumer modules
- [`protoc`](https://github.com/protocolbuffers/protobuf) (`brew install protobuf` on macOS), only for `04b-working-with-protobuf`
- [k3d](https://k3d.io) and [Helm](https://helm.sh) v3+, only for the Kubernetes HA module
- `jq` and `curl`, handy for the HTTP Proxy / Schema Registry / Admin API examples

## Modules

| # | Module | What it covers |
|---|--------|-----------------|
| 01 | [`01-environment`](01-environment) | Spins up the shared local 3-broker Redpanda cluster, Redpanda Console, Pandaproxy, and Schema Registry via Docker Compose. Start here — most other modules depend on this. |
| 02 | [`02-working-with-redpanda-broker`](02-working-with-redpanda-broker) | Core ways to talk to a broker: the `rpk` CLI (topics, producing/consuming, consumer groups), the Kafka wire protocol, the Pandaproxy HTTP REST interface, the Schema Registry, and the Admin API. |
| 03 | [`03-scalability-and-failover`](03-scalability-and-failover) | Topic partitioning and replication, consumer groups distributing load across instances, and live broker failover testing (stop/restart a broker and watch leader re-election). |
| 04 | [`04-producer-consumer`](04-producer-consumer/producer-consumer) | A Rust producer/consumer pair built with `rdkafka`, configured via environment variables — the baseline client implementation the rest of the Rust modules build on. |
| 04a | [`04a-working-with-avro`](04a-working-with-avro/working-with-avro) | Extends module 04 with Avro: a `Notification` record schema (`schema.avsc`), Rust structs serialized/deserialized with `apache-avro`, and a producer/consumer loop that alternates producing and consuming message by message. |
| 04b | [`04b-working-with-protobuf`](04b-working-with-protobuf/working-with-protobuf) | The same `Notification` message as module 04a, this time defined in Protobuf (`schema.proto`) and compiled to a Rust struct at build time with `prost-build`, with a matching produce/consume loop using `prost` for encoding/decoding. |
| 04c | [`04c-working-with-jsonschema-and-axum`](04c-working-with-jsonschema-and-axum/working-with-jsonschema-and-axum) | Same `Notification` message again, this time as JSON Schema (`schema.json`), fronted by an [axum](https://github.com/tokio-rs/axum) HTTP API instead of a CLI loop: `POST /notifications` validates and auto-registers the schema with Redpanda's built-in Schema Registry before producing, and `GET /notifications/stream` pushes consumed messages live over SSE. Includes a `Makefile` and a `test_1000.sh` load-test script. |
| — | [`high-available-cluster`](high-available-cluster) | Deploys a highly available 3-broker Redpanda cluster on Kubernetes (via k3d + Helm) and walks through Raft-based replication, PodDisruptionBudgets, maintenance-mode node drains, quorum loss, and permanent broker decommissioning. Self-contained — doesn't depend on `01-environment`. |
| — | [`redpanda-connect/iceberg`](redpanda-connect/iceberg) | A self-contained local lakehouse stack: Redpanda → Redpanda Connect → Apache Iceberg (REST catalog + MinIO) → Spark/Jupyter for querying. Demonstrates schema evolution landing straight into Iceberg tables. Ships its own Docker Compose stack. |
| — | [`redpanda-connect/redact-pii-routing`](redpanda-connect/redact-pii-routing) | A Redpanda Connect pipeline that redacts PII (email/phone/IP) from a raw event stream and routes messages to different topics by event type, with load-testing and PII-leak verification scripts. Runs against the cluster from `01-environment`. |
| — | [`redpanda-connect`](redpanda-connect) | Two smaller standalone Redpanda Connect pipelines: [`events-to-redpanda.yaml`](redpanda-connect/events-to-redpanda.yaml) (HTTP endpoint that ingests JSON events into a topic) and [`generate-events.yml`](redpanda-connect/generate-events.yml) (synthetic event traffic generator). |

## Suggested path

1. **`01-environment`** — bring up the shared cluster.
2. **`02-working-with-redpanda-broker`** — learn the tools you'll use everywhere else.
3. **`03-scalability-and-failover`** — see replication and failover in action.
4. **`04-producer-consumer`** → **`04a-working-with-avro`** → **`04b-working-with-protobuf`** → **`04c-working-with-jsonschema-and-axum`** — move from CLI tools to real client code, add schema-based messages (Avro, then Protobuf), then front it with an HTTP API and a live Schema Registry (JSON Schema).
5. **`redpanda-connect/redact-pii-routing`** and **`redpanda-connect/iceberg`** — connect the cluster to real pipelines (routing/redaction, and a lakehouse sink).
6. **`high-available-cluster`** — go deeper on availability, this time on Kubernetes.

## Quick start

```bash
cd 01-environment
docker compose up -d
docker compose ps
```

Redpanda Console is then available at http://localhost:8080. See each module's own
README (or the commands above) for what to do next.

## Cleanup

```bash
cd 01-environment
docker compose down -v
```

Repeat for any module that started its own stack (e.g. `redpanda-connect/iceberg`), and
tear down the Kubernetes cluster from `high-available-cluster` with `k3d cluster delete rp-cluster`.