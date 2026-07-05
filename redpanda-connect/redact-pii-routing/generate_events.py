#!/usr/bin/env python3
"""Generate NDJSON test events for the redact-pii-routing pipeline.

Produces a mix of event types so every branch in router.yml gets exercised:
order_created/order_updated -> events.orders, user_signup -> events.users,
heartbeat -> dropped, malformed JSON -> events.dlq, anything else -> events.other.
"""
import argparse
import json
import random

FIRST_NAMES = ["alex", "sam", "jamie", "riley", "morgan", "casey", "drew", "taylor"]
DOMAINS = ["example.com", "mail.com", "test.org", "corp.io"]

EVENT_WEIGHTS = {
    "order_created": 30,
    "order_updated": 10,
    "user_signup": 20,
    "heartbeat": 10,
    "other_event": 25,
    "__malformed__": 5,
}


def rand_email(rng):
    return f"{rng.choice(FIRST_NAMES)}{rng.randint(1, 999)}@{rng.choice(DOMAINS)}"


def rand_phone(rng):
    return f"555-{rng.randint(1000, 9999)}"


def rand_ip(rng):
    return f"10.0.{rng.randint(0, 255)}.{rng.randint(0, 255)}"


def build_user(rng, include_email=True, include_phone=True, include_ip=True):
    user = {}
    if include_email:
        user["email"] = rand_email(rng)
    if include_phone:
        user["phone"] = rand_phone(rng)
    if include_ip:
        user["ip"] = rand_ip(rng)
    return user


def build_record(rng, event_type, idx):
    record = {
        "event_type": event_type,
        "id": idx,
        "user": build_user(
            rng,
            include_email=rng.random() > 0.1,
            include_phone=rng.random() > 0.2,
            include_ip=rng.random() > 0.3,
        ),
    }
    if event_type in ("order_created", "order_updated"):
        record["amount"] = round(rng.uniform(5, 500), 2)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-n", "--count", type=int, default=1000)
    parser.add_argument("-o", "--output", default="events_1000.ndjson")
    parser.add_argument("--seed", type=int, default=42, help="fixed seed for reproducible output")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    types, weights = zip(*EVENT_WEIGHTS.items())

    counts = {k: 0 for k in EVENT_WEIGHTS}
    with open(args.output, "w") as f:
        for idx in range(args.count):
            event_type = rng.choices(types, weights=weights, k=1)[0]
            counts[event_type] += 1
            if event_type == "__malformed__":
                # truncated JSON on purpose (single line, no closing braces) to hit
                # the catch: block in router.yml
                f.write('{"event_type":"order_created", "user": {"email": "broken@example.com"\n')
                continue
            record = build_record(rng, event_type, idx)
            f.write(json.dumps(record) + "\n")

    expected = {
        "events.orders": counts["order_created"] + counts["order_updated"],
        "events.users": counts["user_signup"],
        "events.other": counts["other_event"],
        "events.dlq": counts["__malformed__"],
        "dropped_heartbeat": counts["heartbeat"],
    }
    counts_path = args.output.rsplit(".", 1)[0] + ".counts.json"
    with open(counts_path, "w") as f:
        json.dump(expected, f, indent=2)

    print(f"Wrote {args.count} lines to {args.output}")
    for k, v in counts.items():
        print(f"  {k}: {v}")
    print(f"Expected routing counts written to {counts_path}: {expected}")


if __name__ == "__main__":
    main()