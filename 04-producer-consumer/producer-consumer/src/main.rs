use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

async fn produce(brokers: &str, topic: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create producer");

    let futures = (0..5).map(|i| {
        let producer = producer.clone();
        let topic = topic.to_owned();
        tokio::spawn(async move {
            let key = format!("key-{i}");
            let payload = format!("message-{i}");
            let record = FutureRecord::to(&topic).key(&key).payload(&payload);
            match producer.send(record, Duration::from_secs(5)).await {
                Ok(delivery) => {
                    println!(
                        "Delivered message {i} to partition {} at offset {}",
                        delivery.partition, delivery.offset
                    );
                }
                Err((err, _)) => {
                    eprintln!("Failed to deliver message {i}: {err}");
                }
            }
        })
    });

    for f in futures {
        f.await.expect("Task panicked");
    }
}

async fn consume(brokers: &str, group_id: &str, topic: &str) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Failed to create consumer");

    consumer.subscribe(&[topic]).expect("Failed to subscribe");

    println!("Consuming from topic '{topic}' as group '{group_id}'");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let key = msg.key().and_then(|k| std::str::from_utf8(k).ok()).unwrap_or("<none>");
                let payload = msg.payload().and_then(|p| std::str::from_utf8(p).ok()).unwrap_or("<none>");
                println!(
                    "Received: partition={} offset={} key={key} payload={payload}",
                    msg.partition(),
                    msg.offset()
                );
                consumer.commit_message(&msg, CommitMode::Async).expect("Commit failed");
            }
            Err(err) => {
                eprintln!("Consumer error: {err}");
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let brokers = "localhost:19092,localhost:29092,localhost:39092";
    let topic = "workshop-topic";
    let group_id = "workshop-group";

    println!("Producing messages to topic '{topic}' on {brokers}");
    produce(brokers, topic).await;

    println!("Starting consumer...");
    consume(brokers, group_id, topic).await;
}