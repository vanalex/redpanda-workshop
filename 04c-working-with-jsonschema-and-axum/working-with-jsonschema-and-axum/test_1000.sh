#!/usr/bin/env bash
# Produce N notifications via the HTTP API (default 1000) and verify they're
# all observed on the SSE consume stream (GET /notifications/stream).
#
# Requires: the service running (`cargo run` / `make run`) and curl.
set -euo pipefail

HOST="${HOST:-http://localhost:3000}"
COUNT="${1:-1000}"
STREAM_OUT="$(mktemp)"
STREAM_PID=""

cleanup() {
  if [[ -n "$STREAM_PID" ]]; then
    kill "$STREAM_PID" 2>/dev/null || true
    wait "$STREAM_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "== Checking service is up at $HOST =="
curl -sf "$HOST/" > /dev/null || { echo "Service not reachable at $HOST" >&2; exit 1; }

echo "== Starting SSE consumer, writing to $STREAM_OUT =="
curl -sN "$HOST/notifications/stream" > "$STREAM_OUT" &
STREAM_PID=$!
sleep 1 # give the SSE subscription time to register before we start producing

echo "== Producing $COUNT notifications =="
start_ts=$(date +%s)
ok=0
failed=0
for i in $(seq 1 "$COUNT"); do
  status=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$HOST/notifications" \
    -H "Content-Type: application/json" \
    -d "{\"id\": $i, \"message\": \"load-test-$i\"}")
  if [[ "$status" == "200" ]]; then
    ok=$((ok + 1))
  else
    failed=$((failed + 1))
  fi
  if (( i % 100 == 0 )); then
    echo "  produced $i/$COUNT"
  fi
done
end_ts=$(date +%s)
elapsed=$((end_ts - start_ts))

echo
echo "== Produce results =="
echo "  ok=$ok failed=$failed elapsed=${elapsed}s"

echo
echo "== Waiting for the SSE stream to catch up =="
sleep 3

kill "$STREAM_PID" 2>/dev/null || true
wait "$STREAM_PID" 2>/dev/null || true
STREAM_PID=""

received=$(grep -c '^data: ' "$STREAM_OUT" || true)

echo
echo "== Results =="
echo "  produced (200 OK): $ok"
echo "  received on stream: $received"

if [[ "$ok" -eq "$received" ]]; then
  echo "  RESULT: PASS (no messages lost on the SSE stream)"
else
  echo "  RESULT: MISMATCH ($((ok - received)) message(s) missing from the stream)"
fi

echo
echo "Raw SSE output kept at: $STREAM_OUT"
