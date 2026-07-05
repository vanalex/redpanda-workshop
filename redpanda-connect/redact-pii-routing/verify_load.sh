#!/usr/bin/env bash
# Load events_1000.ndjson into events.raw and verify the router.yml pipeline
# routed + redacted everything correctly.
#
# Requires: rpk, jq, and the pipeline (rpk connect run router.yml) already running.
set -euo pipefail

BROKERS="127.0.0.1:19092"
INPUT="${1:-events_1000.ndjson}"
COUNTS_FILE="${INPUT%.ndjson}.counts.json"
OUTPUT_TOPICS=(events.orders events.users events.other events.dlq)

if [[ ! -f "$INPUT" ]]; then
  echo "Input file not found: $INPUT (run generate_events.py first)" >&2
  exit 1
fi
if [[ ! -f "$COUNTS_FILE" ]]; then
  echo "Expected-counts file not found: $COUNTS_FILE" >&2
  exit 1
fi

start_offset_for() {
  local topic="$1"
  for i in "${!OUTPUT_TOPICS[@]}"; do
    if [[ "${OUTPUT_TOPICS[$i]}" == "$topic" ]]; then
      echo "${START_OFFSETS[$i]}"
      return
    fi
  done
}

echo "== Recording starting offsets =="
START_OFFSETS=()
for t in "${OUTPUT_TOPICS[@]}"; do
  hw=$(rpk topic describe -p "$t" -X brokers="$BROKERS" | awk 'NR==2{print $NF}')
  START_OFFSETS+=("$hw")
  echo "  $t starts at offset $hw"
done

echo "== Producing $(wc -l < "$INPUT" | tr -d ' ') records to events.raw =="
rpk topic produce events.raw -X brokers="$BROKERS" < "$INPUT" > /dev/null

echo "== Waiting for the pipeline to drain events.raw =="
expected_total=$(jq '[.["events.orders"], .["events.users"], .["events.other"], .["events.dlq"]] | add' "$COUNTS_FILE")
for _ in $(seq 1 30); do
  total=0
  for t in "${OUTPUT_TOPICS[@]}"; do
    hw=$(rpk topic describe -p "$t" -X brokers="$BROKERS" | awk 'NR==2{print $NF}')
    total=$(( total + hw - $(start_offset_for "$t") ))
  done
  if [[ "$total" -ge "$expected_total" ]]; then
    break
  fi
  sleep 1
done

echo
echo "== Routing counts (actual vs expected) =="
pass=true
for t in "${OUTPUT_TOPICS[@]}"; do
  start=$(start_offset_for "$t")
  hw=$(rpk topic describe -p "$t" -X brokers="$BROKERS" | awk 'NR==2{print $NF}')
  actual=$(( hw - start ))
  expected=$(jq -r ".\"$t\"" "$COUNTS_FILE")
  status="OK"
  if [[ "$actual" != "$expected" ]]; then
    status="MISMATCH"
    pass=false
  fi
  printf "  %-16s actual=%-5s expected=%-5s %s\n" "$t" "$actual" "$expected" "$status"
done

echo
echo "== PII leakage check on newly produced records =="
leak_found=false
for t in "${OUTPUT_TOPICS[@]}"; do
  start=$(start_offset_for "$t")
  hw=$(rpk topic describe -p "$t" -X brokers="$BROKERS" | awk 'NR==2{print $NF}')
  if [[ "$hw" -le "$start" ]]; then
    continue
  fi
  records=$(rpk topic consume "$t" -X brokers="$BROKERS" -o "${start}:${hw}" -f '%v\n' 2>/dev/null)

  if echo "$records" | grep -qE '"phone":"555-[0-9]{4}"'; then
    echo "  LEAK: unredacted phone found in $t"
    leak_found=true
  fi
  if echo "$records" | grep -qE '"ip":"10\.0\.'; then
    echo "  LEAK: unredacted ip found in $t"
    leak_found=true
  fi
  if echo "$records" | grep -qE '"email":"[a-zA-Z0-9]{2,}@'; then
    echo "  LEAK: unredacted email found in $t"
    leak_found=true
  fi
  if echo "$records" | grep -q '"event_type":"heartbeat"'; then
    echo "  LEAK: heartbeat event leaked into $t (should have been dropped)"
    leak_found=true
  fi
done
if [[ "$leak_found" == "false" ]]; then
  echo "  none found"
fi

echo
if [[ "$pass" == "true" && "$leak_found" == "false" ]]; then
  echo "RESULT: PASS"
else
  echo "RESULT: FAIL"
  exit 1
fi