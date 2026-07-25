use crate::model::Notification;
use crate::settings::Config;
use anyhow::Result;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message as _;

pub fn build_consumer(cfg: &Config) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", &cfg.group_id)
        .set("auto.offset.reset", &cfg.auto_offset_reset)
        .set("enable.auto.commit", "false")
        .create()?;

    consumer.subscribe(&[cfg.topic.as_str()])?;

    Ok(consumer)
}

pub async fn consume_one(consumer: &StreamConsumer) -> Result<()> {
    match consumer.recv().await {
        Ok(msg) => {
            let key = msg.key().and_then(|k| std::str::from_utf8(k).ok()).unwrap_or("<none>");

            if let Some(payload) = msg.payload() {
                let notification = Notification::decode(payload)?;
                println!(
                    "Received: partition={} offset={} key={key} notification={notification:?}",
                    msg.partition(),
                    msg.offset()
                );
            } else {
                println!("Received: partition={} offset={} key={key} <empty payload>", msg.partition(), msg.offset());
            }

            consumer.commit_message(&msg, CommitMode::Async)?;
        }
        Err(err) => eprintln!("Consumer error: {err}"),
    }

    Ok(())
}