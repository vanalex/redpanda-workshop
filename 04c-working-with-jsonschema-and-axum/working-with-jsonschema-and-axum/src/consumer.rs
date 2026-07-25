use crate::model::Notification;
use crate::settings::Config;
use anyhow::{anyhow, Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use schema_registry_converter::async_impl::easy_json::EasyJsonDecoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;

pub struct JsonConsumer {
    consumer: StreamConsumer,
    decoder: EasyJsonDecoder,
}

impl JsonConsumer {
    pub fn new(cfg: &Config) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &cfg.brokers)
            .set("group.id", &cfg.group_id)
            .set("auto.offset.reset", &cfg.auto_offset_reset)
            .set("enable.auto.commit", "false")
            .create()
            .context("failed to create Kafka consumer")?;

        consumer.subscribe(&[cfg.topic.as_str()])?;

        let sr_settings = SrSettings::new_builder(cfg.schema_registry_url.clone())
            .build()
            .map_err(|e| anyhow!("failed to build schema registry settings: {e}"))?;

        Ok(Self {
            consumer,
            decoder: EasyJsonDecoder::new(sr_settings),
        })
    }

    // Reads the Confluent/Redpanda wire-format header off the payload, fetches the
    // referenced schema from the registry (cached after first use) to decode it.
    pub async fn recv(&self) -> Result<Notification> {
        let msg = self.consumer.recv().await.context("consumer error")?;

        let decoded = self
            .decoder
            .decode(msg.payload())
            .await
            .map_err(|e| anyhow!("schema registry decode failed: {e}"))?
            .ok_or_else(|| anyhow!("received empty payload"))?;

        self.consumer.commit_message(&msg, CommitMode::Async)?;

        Ok(serde_json::from_value(decoded.value)?)
    }
}
