# Redpanda Connect Shipment Tracker

You run a logistics operation. Parcels move through a lifecycle —
`label_created → picked_up → in_transit → out_for_delivery → delivered` (with the
occasional `exception`) — and customers want to know the moment their package
moves. You'd rather not have them refreshing a tracking page every ten minutes.

This project builds a real-time shipment-tracking and notification pipeline using
Redpanda and Redpanda Connect. The domain choice makes two things sharper: PII scrubbing is a genuine requirement
(you're handling names, emails, and home addresses), and "notify only on a real
status transition" is a correctness concern, not a nicety — you don't want to
ping someone every time a row is touched.

## Design

Orders live in a transactional Postgres database on the operations side. The goal
is to move that data into Redpanda, where we can process and enrich it in near
real-time. For a row like:

| shipment_id | customer_id | customer_name | customer_address | package_details | shipment_status |
|---|---|---|---|---|---|
| 3 | 3 | Amir Haddad | 78 Elm Rd, Portland, OR | 3x hardcover books | in_transit |

we want to:

- **Scrub** the sensitive customer fields (name, email, address).
- **Detect** whether this shipment's status actually changed since we last saw it.
- **Notify** the customer only on a real transition (optionally with an AI-written message).

An example notification record we might produce:

```json
{
  "shipment_id": 3,
  "customer_id": 3,
  "carrier": "DHL",
  "tracking_number": "JD0002123456789",
  "origin": "Los Angeles, CA",
  "destination": "Portland, OR",
  "package_details": "3x hardcover books",
  "shipment_status": "out_for_delivery",
  "previous_status": "in_transit"
}
```

## Stack

- **Postgres** — the transactional source of truth (`shipments` table).
- **Redpanda** — the streaming platform.
- **Redpanda Connect** — the integration/processing layer (defined in `connect.yaml`).

Everything is pre-wired in `docker-compose.yml`.

## Starting the services

```bash
docker-compose up -d
```

Redpanda Console will be at http://localhost:8080. Most of the work happens on the
command line, though. Set an alias so `rpk` runs inside the cluster container:

```bash
alias rpk="docker exec -ti redpanda-1 rpk"
```

## Topic setup

Create a topic to hold shipment events. Partition count is the classic beginner
sticking point — powers of two (4, 8, 16, …) are a sensible default because they
line up nicely with core counts and load balancing. For a modest operation, 4 is
plenty:

```bash
rpk topic create shipments -p 4
```

## Capturing shipments

Shipments are written to Postgres by the operations backend. You can see the
seeded rows:

```bash
docker exec -ti postgres psql -U root -c "select shipment_id, shipment_status from shipments"
```

We need this data in Redpanda, *and* we need to process it on the way in, so we
use Redpanda Connect to pull directly from Postgres rather than producing from the
app. The input is already defined in `connect.yaml`:

```yaml
input:
  sql_select:
    driver: postgres
    dsn: postgres://root:secret@postgres:5432/root?sslmode=disable
    table: shipments
    columns: [ '*' ]
```

> **Note on `sql_select`:** it reads the table and exits — it's a batch snapshot,
> not change data capture. That's fine for learning the processing model. For
> continuous CDC in production, reach for a streaming input (e.g. `pg_stream`,
> which reads the Postgres write-ahead log) so you capture every `UPDATE` as it
> happens.

Run the pipeline to see records flow:

```bash
rpk connect run /etc/redpanda/connect.yaml
```

Then confirm the topic is being hydrated, via the CLI or
[Console](http://localhost:8080/topics/shipments):

```bash
rpk topic consume shipments -f '%v' -n 1 -o -1 | jq '.'
```

Records are keyed by `shipment_id` so all events for a given parcel stay ordered
on the same partition.

## What the pipeline does

The processing lives between `input` and `output` in `connect.yaml`. Four steps:

**1. Scrub PII (stateless).** A `bloblang` processor builds a fresh document and
copies only the non-sensitive fields, dropping `customer_name`, `customer_email`,
and `customer_address` entirely. Operating on one record at a time with no memory
of others makes this a *stateless* operation — the simplest and most common kind.

**2. Look up the previous status (stateful).** To know whether a status *changed*,
the pipeline has to remember what it saw before. A `cache` resource keyed by
`shipment_id` provides that memory. A `branch` isolates the lookup so it doesn't
clobber the main message, and a `try`/`catch` maps a cache miss to `"none"`.

**3. Persist the current status.** Right after the lookup, a `cache set` writes the
current status back, so the next event for this shipment has something to compare
against.

**4. Notify on change only.** A final `branch` fires only when
`previous_status != shipment_status`, using `deleted()` to skip the branch on a
no-op while letting the message flow through untouched. Here it just logs; in
production you'd swap the `log` for an `http` POST to a notification service.

Re-run and you'll see notifications like:

```
INFO Shipment 3 for customer 3 moved in_transit -> out_for_delivery
```

## Testing

Redpanda Connect supports declarative unit tests right alongside the config, which
makes for a fast iteration loop. The `tests` block in `connect.yaml` feeds three
events for the same shipment (`picked_up`, `picked_up`, `in_transit`) and asserts
that PII is gone and `previous_status` is tracked correctly across them:

```bash
rpk connect test /etc/redpanda/connect.yaml
```

You should see:

```
Test '/etc/redpanda/connect.yaml' succeeded
```

This is the payoff of the shipment framing — the middle event (`picked_up` again)
must **not** be treated as a change, and the test locks that behavior in.

## Bonus: AI-personalized notifications

`connect-ai.yaml` adds an `openai_chat_completion` processor that writes a short,
friendly message on each real transition. The API key is injected from the
environment rather than hardcoded:

```yaml
api_key: "${OPENAI_API_KEY}"
```

Run it with the key passed through to the container:

```bash
export OPENAI_API_KEY=sk-...
docker exec -e OPENAI_API_KEY=$OPENAI_API_KEY -ti redpanda-1 \
  rpk connect run /etc/redpanda/connect-ai.yaml
```

Consume the topic and you'll see a `message` field with something like:

```json
{
  "shipment_id": 3,
  "customer_id": 3,
  "shipment_status": "out_for_delivery",
  "previous_status": "in_transit",
  "message": "Good news — your 3 hardcover books are out for delivery and should land on your doorstep today!"
}
```

## A note on the in-memory cache

The `memory` cache resource is per-process and resets when the pipeline restarts,
so `previous_status` starts empty on a fresh run. That's perfect for a tutorial
and for the unit tests (which run in a single process), but for anything real,
point the cache resource at Redis, Memcached, or DynamoDB so state survives
restarts and can be shared across pipeline instances.

## Files

- `docker-compose.yml` — Redpanda + Console + Postgres.
- `files/postgres/init.sql` — schema and seed shipments.
- `connect.yaml` — the full pipeline (input, scrub, stateful transition detection, notify, output) plus unit tests.
- `connect-ai.yaml` — the bonus variant with OpenAI personalization.

## Where to take it next

- Replace `sql_select` with `pg_stream` for true CDC off the WAL.
- Swap the in-memory cache for Redis to make state durable.
- Turn the notify `log` into an `http` POST to a real notification service.
- Add an `exception` fan-out: route `exception` statuses to a separate topic for the support team.
