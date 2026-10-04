#!/usr/bin/env bash
# End-to-end CDC demo: Postgres -> Redpanda Connect (postgres_cdc) -> Redpanda topic postgres.orders
set -euo pipefail

cd "$(dirname "$0")"

TOPIC=postgres.orders
TIMEOUT=60

# 1. start everything
echo "==> Starting the stack"
docker compose up -d

# 2. check it
echo "==> Container status"
docker compose ps

echo "==> Waiting for Redpanda to be healthy"
for _ in $(seq "$TIMEOUT"); do
  if docker compose exec -T redpanda rpk cluster health 2>/dev/null | grep -q "Healthy:.*true"; then
    break
  fi
  sleep 1
done

if [ "$(docker compose ps -q --status running redpanda-connect)" = "" ]; then
  echo "!! redpanda-connect is not running. Last log lines:"
  docker compose logs --tail 10 redpanda-connect
  exit 1
fi

# 3. consume the CDC topic in the background: 3 snapshot rows + 1 new insert
echo "==> Consuming $TOPIC (expecting 3 snapshot orders + 1 new insert)"
docker compose exec -T redpanda rpk topic consume "$TOPIC" --offset start -n 4 &
CONSUMER_PID=$!

sleep 3

# 4. generate a CDC event
echo "==> Inserting a new order"
docker compose exec -T postgres \
  psql -U postgres -d shop \
  -c "INSERT INTO orders (customer_id, amount) VALUES (1004, 299.99);"

# wait for the consumer, but don't hang forever
for _ in $(seq "$TIMEOUT"); do
  kill -0 "$CONSUMER_PID" 2>/dev/null || break
  sleep 1
done

if kill -0 "$CONSUMER_PID" 2>/dev/null; then
  kill "$CONSUMER_PID"
  echo "!! Did not receive 4 messages within ${TIMEOUT}s. Check: docker compose logs redpanda-connect"
  exit 1
fi

echo "==> Done. Browse the topic in Redpanda Console: http://localhost:8080"
