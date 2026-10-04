# Redpanda Connect — Postgres CDC

Stream row changes from a Postgres table into a Redpanda topic using Redpanda Connect's
`postgres_cdc` input (logical replication), ready to be consumed by a Rust/rdkafka client.

```
PostgreSQL
     │
     │ WAL / logical replication
     ▼
┌──────────────────┐
│ Redpanda Connect │
│   postgres_cdc   │
└────────┬─────────┘
         │
         ▼
     Redpanda
         │
         │ postgres.orders
         ▼
     Rust/rdkafka
```

Unlike the other modules, this one is self-contained: `docker-compose.yml` starts its own
single-node Redpanda, so the cluster from [`01-environment`](../01-environment) is not needed.

## Files

- `docker-compose.yml` — Postgres 17 (with `wal_level=logical`), a single Redpanda broker,
  Redpanda Console and Redpanda Connect.
- `postgres/init.sql` — creates the `orders` table in the `shop` database and seeds three rows.
- `redpanda-connect/postgres-cdc.yaml` — the pipeline: `postgres_cdc` on `public.orders` →
  `redpanda` output to topic `postgres.orders`, keyed by `id`.
- `run_demo.sh` — runs the whole walkthrough below in one go.

## Services and ports

| Service | Container | Host port |
|---|---|---|
| Postgres | `workshop-postgres` | `5432` (user/password `postgres`, db `shop`) |
| Redpanda Kafka API | `workshop-redpanda` | `19092` |
| Redpanda Admin API | `workshop-redpanda` | `9644` (`dev-container` default) |
| Schema Registry | `workshop-redpanda` | `18081` |
| Redpanda Console | `workshop-redpanda-console` | `8080` |
| Redpanda Connect HTTP | `workshop-redpanda-connect` | `4195` |

## Prerequisite: Enterprise license

`postgres_cdc` is an Enterprise connector. Without a license, Redpanda Connect exits with
`this feature requires a valid Redpanda Enterprise Edition license`.

Get a free 30-day trial key from <https://redpanda.com/try-enterprise>, or generate one with `rpk`:

```bash
docker run --rm -it -v "$PWD":/out --entrypoint rpk \
  docker.redpanda.com/redpandadata/redpanda:v26.2.3 \
  generate license --name <first> --last-name <last> \
  --email <you@company.com> --company <company> \
  --path /out/redpanda.license
```

Then mount it into the `redpanda-connect` service:

```yaml
    volumes:
      - ./redpanda-connect/postgres-cdc.yaml:/connect.yaml:ro
      - ./redpanda.license:/etc/redpanda/redpanda.license:ro
```

(or set `REDPANDA_LICENSE: ${REDPANDA_LICENSE}` and put the key in a `.env` file).
Don't commit `redpanda.license` or `.env`.

## Quick run

```bash
./run_demo.sh
```

It starts the stack, waits for Redpanda, consumes `postgres.orders` in the background,
inserts a new order and exits once all four change events have arrived (or after 60s).

## Step by step

### 1. Start everything

```bash
docker compose up -d
```

### 2. Check it

```bash
docker compose ps
```

`postgres` should be `healthy`, and `redpanda`, `console` and `redpanda-connect` `Up`.
You can also check the broker with:

```bash
docker compose exec redpanda rpk cluster health
```

### 3. Consume the CDC topic

```bash
docker compose exec redpanda \
  rpk topic consume postgres.orders
```

Because `stream_snapshot` is enabled, you should initially see the three seeded orders.
Leave this running.

### 4. Generate a CDC event

In a second terminal:

```bash
docker compose exec postgres \
  psql -U postgres -d shop \
  -c "INSERT INTO orders (customer_id, amount) VALUES (1004, 299.99);"
```

The new order shows up immediately in the consumer from step 3. Try an `UPDATE` or
`DELETE` on `orders` as well.

### 5. Browse in Redpanda Console

Open <http://localhost:8080> and look at the `postgres.orders` topic.
This is the same broker + Console arrangement as Redpanda's official Docker quickstart.

## Troubleshooting

- **`redpanda` exits with `unrecognised option '--admin-addr=...'`** — `rpk redpanda start`
  has no `--admin-addr` flag, so don't add one to the `redpanda` command. In `dev-container`
  mode the Admin API already listens on `0.0.0.0:9644`. To change it, use
  `--set redpanda.admin=...` instead.
- **`not a directory: Are you trying to mount a directory onto a file`** — the bind-mounted
  file didn't exist when the container was first created, so Docker created an empty directory
  in its place. Delete the directory, create the real file, and recreate the containers
  (`docker compose down && docker compose up -d`).
- **`orders` table is missing** — `init.sql` only runs on an empty data volume. Reset with
  `docker compose down -v && docker compose up -d`.

## Clean up

```bash
docker compose down -v
```
