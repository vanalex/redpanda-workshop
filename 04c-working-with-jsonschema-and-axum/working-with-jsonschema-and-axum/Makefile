BROKER_CONTAINER := redpanda-0
BROKER := localhost:9092
TOPIC := workshop-jsonschema-topic
SCHEMA_REGISTRY_URL := http://localhost:18081
HTTP_PORT := 3000

.PHONY: build run topic subjects schema produce stream load-test-1000 clean

build:
	cargo build

run:
	cargo run

## Create the Kafka topic (auto.topic.create.enable is off on this cluster).
topic:
	docker exec $(BROKER_CONTAINER) rpk topic create $(TOPIC) --brokers $(BROKER)

## List subjects registered in the schema registry.
subjects:
	curl -s $(SCHEMA_REGISTRY_URL)/subjects

## Show the latest registered schema for this topic's value subject.
schema:
	curl -s $(SCHEMA_REGISTRY_URL)/subjects/$(TOPIC)-value/versions/latest

## Produce one sample notification via the HTTP API.
produce:
	curl -s -i -X POST http://localhost:$(HTTP_PORT)/notifications \
		-H "Content-Type: application/json" \
		-d '{"id": 1, "message": "hello from makefile"}'

## Stream consumed notifications over SSE (Ctrl-C to stop).
stream:
	curl -sN http://localhost:$(HTTP_PORT)/notifications/stream

## Produce 1000 notifications and verify none are lost on the SSE stream.
load-test-1000:
	./test_1000.sh 1000

clean:
	cargo clean
