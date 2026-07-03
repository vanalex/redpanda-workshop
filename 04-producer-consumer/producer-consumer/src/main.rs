mod consumer;
mod producer;
mod settings;

use anyhow::Result;
use settings::Config;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cfg = Config::default();

    println!("Config: {cfg:?}");
    println!("Producing {} messages to topic '{}' on {}", cfg.message_count, cfg.topic, cfg.brokers);
    producer::produce(&cfg).await?;

    println!("Starting consumer...");
    consumer::consume(&cfg).await?;

    Ok(())
}