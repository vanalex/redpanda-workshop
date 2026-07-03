use crate::settings::Config;
use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub async fn produce(cfg: &Config) -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("message.timeout.ms", &cfg.message_timeout_ms.to_string())
        .create()?;

    let handles: Vec<_> = (0..cfg.message_count)
        .map(|i| {
            let producer = producer.clone();
            let topic = cfg.topic.clone();
            tokio::spawn(async move {
                let key = format!("key-{i}");
                let payload = format!("message-{i}");
                let record = FutureRecord::to(&topic).key(&key).payload(&payload);
                match producer.send(record, Duration::from_secs(5)).await {
                    Ok(delivery) => println!(
                        "Delivered message {i} to partition {} at offset {}",
                        delivery.partition, delivery.offset
                    ),
                    Err((err, _)) => eprintln!("Failed to deliver message {i}: {err}"),
                }
            })
        })
        .collect();

    for handle in handles {
        handle.await?;
    }

    Ok(())
}