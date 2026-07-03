use crate::settings::Config;
use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;

pub async fn consume(cfg: &Config) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", &cfg.group_id)
        .set("auto.offset.reset", &cfg.auto_offset_reset)
        .set("enable.auto.commit", "false")
        .create()?;

    consumer.subscribe(&[cfg.topic.as_str()])?;

    println!("Consuming from topic '{}' as group '{}'", cfg.topic, cfg.group_id);

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
                consumer.commit_message(&msg, CommitMode::Async)?;
            }
            Err(err) => eprintln!("Consumer error: {err}"),
        }
    }
}