# Redpanda Connect — PII Redaction + Event Routing

A single Redpanda Connect pipeline that consumes a raw event stream, strips PII, drops
noise, and fans messages out to different topics based on their type.

```
[events.raw] --> [Redpanda Connect: redact + route] --> [events.orders]
                                                      --> [events.users]
                                                      --> [events.dlq]
                                                      --> [events.other]
                       (heartbeats are dropped, not routed anywhere)
```

This assumes the 3-broker Redpanda cluster from [`01-environment`](../../01-environment)
is already running, with the Kafka API reachable at `localhost:19092`.

## Files

- `router.yml` — the pipeline: consume `events.raw`, filter/redact/enrich, switch-route to
  one of four output topics.
- `run_pipeline.sh` — creates the topics and starts the pipeline.
- `test.sh` — produces one sample record and consumes the routed result, for a quick smoke test.
- `generate_events.py` — generates a large batch of synthetic NDJSON events (default 1000)
  covering every routing branch, plus a `.counts.json` sidecar with the expected per-topic counts.
- `verify_load.sh` — loads a generated batch into `events.raw` and verifies actual routing
  counts match expected, and that no unredacted PII leaked into any output topic.

## What the pipeline does

`router.yml` consumes `events.raw` and runs each message through:

1. **Filter** — messages with `event_type: "heartbeat"` are dropped (`deleted()`). Anything
   that isn't valid JSON is caught and tagged `error: "malformed"` instead of crashing the pipeline.
2. **Redact** — `user.email` is masked to its first character + `***@domain`, `user.phone` is
   replaced with `"REDACTED"`, and `user.ip` is deleted outright.
3. **Enrich + route** — a `route` metadata field is set from `event_type`, and a
   `processed_at` timestamp is added.

The output `switch` then sends each message to:

| Condition | Topic |
|---|---|
| `event_type` is `order_created` or `order_updated` | `events.orders` |
| `event_type` is `user_signup` | `events.users` |
| tagged `error: "malformed"` | `events.dlq` |
| anything else | `events.other` |
| `event_type` is `heartbeat` | *(dropped, no topic)* |

## 1. Start the pipeline

```bash
./run_pipeline.sh
```

This creates the five topics (`events.raw`, `events.orders`, `events.users`, `events.dlq`,
`events.other`) and runs `rpk connect run router.yml` in the foreground. Leave
it running in its own terminal — it logs each active input/output on startup and then
processes silently, message by message.

## 2. Smoke-test with one message

In a second terminal:

```bash
./test.sh
```

This produces one `order_created` event with a fake email/phone/IP to `events.raw`, then
consumes the latest message off `events.orders` so you can see the redacted result immediately.

## 3. Monitor the pipeline live

Tail any/all output topics as messages land:

```bash
rpk topic consume events.orders events.users events.other events.dlq \
  -X brokers=127.0.0.1:19092 -f '%t: %v\n'
```

Or browse topics visually in Redpanda Console at http://localhost:8080.

## 4. Load-test with 1000 events

Generate a realistic batch (weighted mix of order/signup/heartbeat/malformed/other events,
with some records missing email/phone/ip on purpose):

```bash
python3 generate_events.py -n 1000 -o events_1000.ndjson
```

Then ingest it and verify the pipeline routed and redacted everything correctly:

```bash
./verify_load.sh events_1000.ndjson
```

`verify_load.sh` records each output topic's starting offset, produces the whole file into
`events.raw`, waits for the pipeline to drain it, then checks:

- **Routing counts** — actual messages landed per topic vs. the expected counts written by
  `generate_events.py` into `events_1000.counts.json`.
- **PII leakage** — greps the newly-produced records in every output topic for unredacted
  phone numbers, IPs, full emails, or leaked `heartbeat` events.

It prints `RESULT: PASS` (exit 0) or `RESULT: FAIL` (exit 1) with a mismatch/leak breakdown.

## Notes on broker address

`rpk` defaults to `127.0.0.1:9092`, but this workshop's cluster (see
[`01-environment/docker-compose.yml`](../../01-environment/docker-compose.yml)) exposes its
external Kafka API on **`19092`** (redpanda-0). Every `rpk` command here passes
`-X brokers=127.0.0.1:19092` explicitly, and `router.yml`'s `seed_brokers` are set to
`localhost:19092` — if you point this at a different cluster, update both.

## Next steps to try

- Add a `log` processor before and after the redaction mapping step in `router.yml` to see
  raw vs. redacted payloads directly in the pipeline's stdout.
- Route `events.dlq` messages to a separate alerting pipeline instead of just storing them.
- Extend the redaction mapping to cover nested PII (e.g. billing addresses) using
  `walk`/recursive Bloblang mappings instead of fixed field paths.