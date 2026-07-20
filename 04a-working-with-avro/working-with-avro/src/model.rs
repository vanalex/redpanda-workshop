use anyhow::Result;
use apache_avro::Schema;
use serde::{Deserialize, Serialize};

pub const SCHEMA_STR: &str = include_str!("../schema.avsc");

#[derive(Debug, Serialize, Deserialize)]
pub struct Notification {
    pub id: Option<i64>,
    pub message: Option<String>,
}

pub fn load_schema() -> Result<Schema> {
    Ok(Schema::parse_str(SCHEMA_STR)?)
}