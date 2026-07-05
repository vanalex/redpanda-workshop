echo '{"event_type":"order_created","user":{"email":"alex@example.com","phone":"555-1234","ip":"10.0.0.1"},"amount":42}' \
  | rpk topic produce events.raw -X brokers=127.0.0.1:19092

rpk topic consume events.orders -n 1 -o -1 -X brokers=127.0.0.1:19092

