use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    pub schema_registry_url: String,
    pub message_timeout_ms: u64,
    pub auto_offset_reset: String,
    pub http_port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            brokers: env::var("KAFKA_BROKERS")
                .unwrap_or_else(|_| "localhost:9092,localhost:29092,localhost:39092".to_string()),
            topic: env::var("KAFKA_TOPIC")
                .unwrap_or_else(|_| "workshop-jsonschema-topic".to_string()),
            group_id: env::var("KAFKA_GROUP_ID")
                .unwrap_or_else(|_| "workshop-jsonschema-group".to_string()),
            schema_registry_url: env::var("SCHEMA_REGISTRY_URL")
                .unwrap_or_else(|_| "http://localhost:18081".to_string()),
            message_timeout_ms: env::var("MESSAGE_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5000),
            auto_offset_reset: env::var("AUTO_OFFSET_RESET")
                .unwrap_or_else(|_| "earliest".to_string()),
            http_port: env::var("HTTP_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3000),
        }
    }
}
