# 03 — Scalability and Failover

## Prerequisites

The 3-node cluster from `01-environment` is running:
```bash
cd ../01-environment
docker compose up -d
```

---

## Topic Management

### Delete existing topics
```bash
rpk topic delete demo.products demo.purchases demo.inventories \
  -X brokers=localhost:19092
```

### Create topics (replication factor 3, 6 partitions)
```bash
rpk topic create demo.products \
  --partitions 6 \
  --replicas 3 \
  -X brokers=localhost:19092

rpk topic create demo.purchases \
  --partitions 6 \
  --replicas 3 \
  -X brokers=localhost:19092

rpk topic create demo.inventories \
  --partitions 6 \
  --replicas 3 \
  -X brokers=localhost:19092
```

### Describe a topic (partitions, leaders, in-sync replicas)
```bash
rpk topic describe demo.purchases -X brokers=localhost:19092
```

---

## Producer

Produce messages to `demo.purchases`:
```bash
rpk topic produce demo.purchases \
  -X brokers=localhost:19092
```

Type `key:value` lines and press Enter. `Ctrl+C` to stop.

---

## Consumer

### Consume without a group (all consumers receive all messages)
```bash
rpk topic consume demo.purchases \
  -o end \
  -f 'Part-%p => %k:%v\n' \
  -X brokers=localhost:19092
```

### Consume with a consumer group (partitions distributed across consumers)

Open multiple terminals and run the same command — Redpanda will assign partitions across them:
```bash
rpk topic consume demo.purchases \
  -g purchases-group \
  -f 'Part-%p => %k:%v\n' \
  -X brokers=localhost:19092
```

---

## Broker Failover Testing

### Check cluster health before stopping a broker
```bash
rpk cluster health -X admin.hosts=localhost:19644,localhost:29644,localhost:39644
```

### Stop a broker
```bash
docker stop redpanda-1
```

### Verify the cluster re-elected partition leaders
```bash
rpk topic describe demo.purchases -X brokers=localhost:19092
rpk cluster health -X admin.hosts=localhost:19644,localhost:39644
```

Produce and consume messages while `redpanda-1` is down — the cluster remains available because topics have a replication factor of 3.

### Restart the broker
```bash
docker start redpanda-1
```

### Verify it rejoins and syncs
```bash
rpk cluster health -X admin.hosts=localhost:19644,localhost:29644,localhost:39644
rpk topic describe demo.purchases -X brokers=localhost:19092
```

---

## Broker addresses reference

| Broker     | Kafka (external) | Admin API         |
|------------|-----------------|-------------------|
| redpanda-0 | localhost:19092  | localhost:19644   |
| redpanda-1 | localhost:29092  | localhost:29644   |
| redpanda-2 | localhost:39092  | localhost:39644   |

Redpanda Console: http://localhost:8080