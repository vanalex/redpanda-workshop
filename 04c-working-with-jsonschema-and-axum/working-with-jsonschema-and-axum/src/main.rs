mod consumer;
mod model;
mod producer;
mod settings;

use anyhow::Result;
use axum::{
    extract::State,
    http::StatusCode,
    response::sse::{Event, Sse},
    routing::{get, post},
    Json, Router,
};
use consumer::JsonConsumer;
use model::Notification;
use producer::{JsonProducer, SendError};
use settings::Config;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};

#[derive(Clone)]
struct AppState {
    producer: Arc<JsonProducer>,
    tx: broadcast::Sender<Notification>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();
    let cfg = Config::default();

    let producer = Arc::new(JsonProducer::new(&cfg)?);
    let consumer = JsonConsumer::new(&cfg)?;
    let (tx, _rx) = broadcast::channel::<Notification>(100);

    let consumer_tx = tx.clone();
    tokio::spawn(async move {
        loop {
            match consumer.recv().await {
                Ok(notification) => {
                    // Err just means no SSE clients are currently subscribed.
                    let _ = consumer_tx.send(notification);
                }
                Err(err) => eprintln!("consumer error: {err}"),
            }
        }
    });

    let state = AppState { producer, tx };

    let app = Router::new()
        .route("/", get(health))
        .route("/notifications", post(create_notification))
        .route("/notifications/stream", get(stream_notifications))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", cfg.http_port)).await?;
    println!("Listening on http://0.0.0.0:{}", cfg.http_port);
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health() -> &'static str {
    "OK"
}

async fn create_notification(
    State(state): State<AppState>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let key = payload
        .get("id")
        .and_then(|v| v.as_i64())
        .map(|id| id.to_string())
        .unwrap_or_else(|| "unkeyed".to_string());

    state
        .producer
        .send(&payload, &key)
        .await
        .map(|(partition, offset)| {
            Json(serde_json::json!({ "partition": partition, "offset": offset }))
        })
        .map_err(|err| match err {
            SendError::SchemaValidation(_) => (StatusCode::BAD_REQUEST, err.to_string()),
            SendError::Delivery(_) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        })
}

async fn stream_notifications(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.tx.subscribe();
    let stream = BroadcastStream::new(rx)
        .filter_map(|item| item.ok())
        .map(|notification| Ok(Event::default().json_data(notification).unwrap()));

    Sse::new(stream)
}
