# create topics
rpk topic create events.raw events.orders events.users events.dlq events.other -X brokers=127.0.0.1:19092

# run the pipeline (binary or docker)
rpk connect run router.yml