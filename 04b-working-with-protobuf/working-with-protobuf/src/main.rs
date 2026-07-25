mod consumer;
mod model;
mod producer;
mod settings;

use anyhow::Result;
use settings::Config;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cfg = Config::default();

    println!("Config: {cfg:?}");

    let kafka_producer = producer::build_producer(&cfg)?;
    let kafka_consumer = consumer::build_consumer(&cfg)?;

    println!(
        "Alternating produce/consume of {} Protobuf-encoded messages on topic '{}' ({})",
        cfg.message_count, cfg.topic, cfg.brokers
    );

    for i in 0..cfg.message_count {
        producer::produce_one(&kafka_producer, &cfg.topic, i).await?;
        consumer::consume_one(&kafka_consumer).await?;
    }

    Ok(())
}