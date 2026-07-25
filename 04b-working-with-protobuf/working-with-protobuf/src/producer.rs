use crate::model::Notification;
use crate::settings::Config;
use anyhow::Result;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub fn build_producer(cfg: &Config) -> Result<FutureProducer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("message.timeout.ms", &cfg.message_timeout_ms.to_string())
        .create()?)
}

pub async fn produce_one(producer: &FutureProducer, topic: &str, i: usize) -> Result<()> {
    let notification = Notification {
        id: Some(i as i64),
        message: Some(format!("notification-{i}")),
    };

    let key = format!("key-{i}");
    let payload = notification.encode_to_vec();

    let record = FutureRecord::to(topic).key(&key).payload(&payload);
    match producer.send(record, Duration::from_secs(5)).await {
        Ok(delivery) => println!(
            "Delivered message {i} to partition {} at offset {}",
            delivery.partition, delivery.offset
        ),
        Err((err, _)) => eprintln!("Failed to deliver message {i}: {err}"),
    }

    Ok(())
}