use std::env;

#[derive(Debug)]
pub struct Config {
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    pub message_count: usize,
    pub message_timeout_ms: u64,
    pub auto_offset_reset: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            brokers: env::var("KAFKA_BROKERS")
                .unwrap_or_else(|_| "localhost:19092,localhost:29092,localhost:39092".to_string()),
            topic: env::var("KAFKA_TOPIC")
                .unwrap_or_else(|_| "workshop-topic".to_string()),
            group_id: env::var("KAFKA_GROUP_ID")
                .unwrap_or_else(|_| "workshop-group".to_string()),
            message_count: env::var("MESSAGE_COUNT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5),
            message_timeout_ms: env::var("MESSAGE_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5000),
            auto_offset_reset: env::var("AUTO_OFFSET_RESET")
                .unwrap_or_else(|_| "earliest".to_string()),
        }
    }
}