# 02 — Working with the Redpanda Broker

This module covers the core ways to interact with the Redpanda cluster started in `01-environment`: the `rpk` CLI, the Kafka wire protocol, the HTTP Proxy (Pandaproxy), and the Schema Registry.

## Prerequisites

- The 3-node cluster from `01-environment` is running:
  ```bash
  cd ../01-environment
  docker compose up -d
  ```
- `rpk` is available. It ships inside each broker container, or install it locally:
  ```bash
  brew install redpanda-data/tap/redpanda
  ```

## rpk profile (optional but recommended)

Create a profile so you don't need to pass broker addresses on every command:
```bash
rpk profile create workshop \
  -X brokers=localhost:19092,localhost:29092,localhost:39092 \
  -X admin.hosts=localhost:19644,localhost:29644,localhost:39644 \
  -X schema_registry.hosts=localhost:18081,localhost:28081,localhost:38081
rpk profile use workshop
```

Or pass broker addresses inline on any command with `-X brokers=<addr>`.

## Broker addresses

| Broker      | Kafka (external) | Pandaproxy        | Schema Registry   | Admin API         |
|-------------|-----------------|-------------------|-------------------|-------------------|
| redpanda-0  | localhost:19092  | localhost:18082    | localhost:18081    | localhost:19644   |
| redpanda-1  | localhost:29092  | localhost:28082    | localhost:28081    | localhost:29644   |
| redpanda-2  | localhost:39092  | localhost:38082    | localhost:38081    | localhost:39644   |

Redpanda Console: http://localhost:8080

## rpk — cluster operations

Check cluster health:
```bash
rpk cluster health -X admin.hosts=localhost:19644
```

List brokers:
```bash
rpk cluster info -X brokers=localhost:19092
```

## Topics

Create a topic with 3 partitions and replication factor 3:
```bash
rpk topic create orders \
  --partitions 3 \
  --replicas 3 \
  -X brokers=localhost:19092
```

List topics:
```bash
rpk topic list -X brokers=localhost:19092
```

Describe a topic (partitions, leaders, replicas):
```bash
rpk topic describe orders -X brokers=localhost:19092
```

Delete a topic:
```bash
rpk topic delete orders -X brokers=localhost:19092
```

## Producing messages

Produce a single message (key + value):
```bash
echo '{"orderId":"1","item":"widget"}' | \
  rpk topic produce orders \
  --key order-1 \
  -X brokers=localhost:19092
```

Produce multiple messages from a file (one JSON object per line):
```bash
rpk topic produce orders \
  -X brokers=localhost:19092 < messages.jsonl
```

## Consuming messages

Consume from the beginning:
```bash
rpk topic consume orders \
  -o start \
  -X brokers=localhost:19092
```

Consume as a named consumer group:
```bash
rpk topic consume orders \
  -g my-consumer-group \
  -X brokers=localhost:19092
```

Print raw values only (pipe-friendly):
```bash
rpk topic consume orders \
  -o start \
  -f '%v\n' \
  -X brokers=localhost:19092
```

## Consumer groups

List groups:
```bash
rpk group list -X brokers=localhost:19092
```

Describe a group (lag per partition):
```bash
rpk group describe my-consumer-group -X brokers=localhost:19092
```

## HTTP Proxy (Pandaproxy)

Pandaproxy exposes a REST interface on port 18082 (redpanda-0 external).

List topics:
```bash
curl -s http://localhost:18082/topics | jq
```

Produce a message:
```bash
curl -s -X POST http://localhost:18082/topics/orders \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -d '{"records":[{"key":"order-2","value":{"orderId":"2","item":"gadget"}}]}' | jq
```

Create a consumer instance:
```bash
curl -s -X POST http://localhost:18082/consumers/http-group \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"name":"consumer-1","format":"json","auto.offset.reset":"earliest"}' | jq
```

Subscribe to a topic:
```bash
curl -s -X POST \
  http://localhost:18082/consumers/http-group/instances/consumer-1/subscription \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"topics":["orders"]}' | jq
```

Consume records:
```bash
curl -s \
  http://localhost:18082/consumers/http-group/instances/consumer-1/records \
  -H "Accept: application/vnd.kafka.json.v2+json" | jq
```

Delete the consumer instance when done:
```bash
curl -s -X DELETE \
  http://localhost:18082/consumers/http-group/instances/consumer-1
```

## Schema Registry

Register an Avro schema:
```bash
curl -s -X POST http://localhost:18081/subjects/orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"item\",\"type\":\"string\"}]}"
  }' | jq
```

List subjects:
```bash
curl -s http://localhost:18081/subjects | jq
```

Fetch a schema by subject and version:
```bash
curl -s http://localhost:18081/subjects/orders-value/versions/latest | jq
```

## Admin API

Cluster configuration:
```bash
curl -s http://localhost:19644/v1/cluster_config | jq
```

Node configuration:
```bash
curl -s http://localhost:19644/v1/node_config | jq
```

Broker health:
```bash
curl -s http://localhost:19644/v1/health_monitor/overview | jq
```

## Cleanup

Remove all containers, networks, and volumes:
```bash
cd ../01-environment
docker compose down -v
```