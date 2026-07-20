use crate::model::Notification;
use crate::settings::Config;
use anyhow::Result;
use apache_avro::{from_avro_datum, from_value, Schema};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;

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

pub async fn consume_one(consumer: &StreamConsumer, schema: &Schema) -> Result<()> {
    match consumer.recv().await {
        Ok(msg) => {
            let key = msg.key().and_then(|k| std::str::from_utf8(k).ok()).unwrap_or("<none>");

            if let Some(mut payload) = msg.payload() {
                let value = from_avro_datum(schema, &mut payload, None)?;
                let notification: Notification = from_value(&value)?;
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