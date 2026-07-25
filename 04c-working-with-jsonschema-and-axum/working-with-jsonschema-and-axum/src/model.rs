use serde::{Deserialize, Serialize};

pub const SCHEMA_STR: &str = include_str!("../schema.json");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub id: Option<i64>,
    pub message: Option<String>,
}
