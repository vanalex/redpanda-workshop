use crate::model::SCHEMA_STR;
use crate::settings::Config;
use anyhow::{anyhow, Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use schema_registry_converter::async_impl::easy_json::EasyJsonEncoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::{SchemaType, SubjectNameStrategy, SuppliedSchema};
use std::fmt;
use std::time::Duration;

#[derive(Debug)]
pub enum SendError {
    SchemaValidation(String),
    Delivery(String),
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::SchemaValidation(msg) => write!(f, "schema validation failed: {msg}"),
            SendError::Delivery(msg) => write!(f, "failed to deliver message: {msg}"),
        }
    }
}

impl std::error::Error for SendError {}

pub struct JsonProducer {
    producer: FutureProducer,
    encoder: EasyJsonEncoder,
    topic: String,
}

impl JsonProducer {
    pub fn new(cfg: &Config) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &cfg.brokers)
            .set("message.timeout.ms", &cfg.message_timeout_ms.to_string())
            .create()
            .context("failed to create Kafka producer")?;

        let sr_settings = SrSettings::new_builder(cfg.schema_registry_url.clone())
            .build()
            .map_err(|e| anyhow!("failed to build schema registry settings: {e}"))?;

        Ok(Self {
            producer,
            encoder: EasyJsonEncoder::new(sr_settings),
            topic: cfg.topic.clone(),
        })
    }

    // Registers `schema.json` for the "{topic}-value" subject on first call (or
    // reuses the existing registration), then validates the raw client payload
    // against it before encoding to the Confluent/Redpanda wire format (magic byte
    // + schema id). `value` is the untouched request body — not re-serialized from
    // a Rust struct — so a payload that violates the schema is actually rejected
    // here instead of being silently reshaped by serde first.
    pub async fn send(&self, value: &serde_json::Value, key: &str) -> Result<(i32, i64), SendError> {
        let strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
            self.topic.clone(),
            false,
            SuppliedSchema {
                name: Some("Notification".to_string()),
                schema_type: SchemaType::Json,
                schema: SCHEMA_STR.to_string(),
                references: vec![],
                properties: None,
                tags: None,
            },
        );

        let payload = self
            .encoder
            .encode(value, strategy)
            .await
            .map_err(|e| SendError::SchemaValidation(e.to_string()))?;

        let record = FutureRecord::to(&self.topic).key(key).payload(&payload);
        self.producer
            .send(record, Duration::from_secs(5))
            .await
            .map(|delivery| (delivery.partition, delivery.offset))
            .map_err(|(err, _)| SendError::Delivery(err.to_string()))
    }
}
