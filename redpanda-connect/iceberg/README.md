# Redpanda Connect → Apache Iceberg — Local Lakehouse

A self-contained local stack: Redpanda (streaming) → Redpanda Connect (pipeline) →
Apache Iceberg on MinIO (storage) → Spark (querying).

```
[Redpanda topic: orders] --> [Redpanda Connect] --> [Iceberg REST catalog + MinIO] --> [Spark SQL / Jupyter]
```

## Files

- `docker-compose.yml` — the full stack (Redpanda, Console, Redpanda Connect, Iceberg REST catalog, MinIO, Spark).
- `connect.yaml` — the Redpanda Connect pipeline definition (consume from `orders`, write to Iceberg).

## 1. Start the stack

```bash
docker compose up -d
```

Give it ~30-60 seconds. Check everything is healthy:

```bash
docker compose ps
docker compose logs -f redpanda-connect
```

You should see Redpanda Connect log that its `iceberg` output is active. If it errors on startup, it's almost
always because `rest` (the Iceberg REST catalog) or `minio` wasn't ready yet — Connect will retry automatically.

## 2. Create the topic and send test messages

Redpanda auto-creates topics on first produce, but creating it explicitly avoids surprises:

```bash
docker compose exec redpanda rpk topic create orders
```

Produce a few test JSON messages:

```bash
docker compose exec -T redpanda rpk topic produce orders <<'EOF'
{"order_id": 1, "customer": "alex", "amount": 42.50, "currency": "EUR"}
{"order_id": 2, "customer": "sam", "amount": 17.00, "currency": "EUR"}
{"order_id": 3, "customer": "alex", "amount": 99.99, "currency": "USD"}
EOF
```

Watch the Connect logs — you should see it pick up and process these within a few seconds
(the pipeline batches every 10 messages or 5 seconds, whichever comes first).

## 3. Verify the data landed in Iceberg

The bundled Spark container is pre-wired to a catalog named `demo`, pointing at the same
REST catalog (`rest:8181`) and MinIO warehouse (`s3://warehouse/`) that Redpanda Connect writes to.

Open a Spark SQL shell:

```bash
docker compose exec spark-iceberg spark-sql
```

Then:

```sql
SELECT * FROM demo.streaming.orders;
```

You should see your test rows, including the `ingested_at` and `source_topic` fields
that the pipeline added.

Alternatively, use the Jupyter notebook environment at http://localhost:8888 (no
password by default) and run the same query from a PySpark cell:

```python
spark.sql("SELECT * FROM demo.streaming.orders").show()
```

## 4. Watch it evolve

Produce a message with a new field:

```bash
docker compose exec -T redpanda rpk topic produce orders <<'EOF'
{"order_id": 4, "customer": "jo", "amount": 10.00, "currency": "USD", "promo_code": "WELCOME10"}
EOF
```

Re-run the Spark query — the table should now include a `promo_code` column (NULL for
older rows), thanks to `schema_evolution.enabled: true` in `connect.yaml`.

## Useful UIs

- Redpanda Console: http://localhost:8085 (browse topics, messages, consumer groups)
- MinIO Console: http://localhost:9001 (login: `admin` / `password`) — browse the raw
  Parquet/metadata files Iceberg writes under `warehouse/streaming/orders/`
- Spark UI: http://localhost:8080
- Jupyter: http://localhost:8888

## Cleaning up

```bash
docker compose down -v
```

The `-v` also removes the Redpanda and MinIO volumes, so you start fresh next time.

## Known limitation

The Iceberg output in Redpanda Connect currently supports high-speed **append-only**
ingestion — upserts/deletes aren't supported yet. This example is a good fit for event
logs and immutable records; for row-level updates you'd currently need Iceberg Topics
(Redpanda's in-broker integration) plus a Spark merge job instead.

## Next steps to try

- Swap the `redpanda` input for a CDC input (e.g. `postgres_cdc`) to stream real database
  changes into Iceberg instead of synthetic test messages.
- Add a `switch` output or dynamic `table: '${! meta("event_type") }'` to route different
  message types into separate Iceberg tables from a single topic.
- Point ClickHouse at the same Iceberg tables to query them alongside your existing
  ClickHouse setup.
