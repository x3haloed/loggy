use actix_web::{web, App, HttpServer, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use serde_json::json;
use libsql::{Builder, Connection};
use clap::Parser;
use hostname::get as get_hostname;
use std::process;
use tracing::{info, error};
use tracing_subscriber;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Config {
    #[arg(long, default_value = "8080")]
    port: u16,
    #[arg(long, default_value = ":memory:")]
    db_url: String,
}

async fn init_db(db_url: &str) -> Connection {
    let db = Builder::new_local(db_url).build().await.unwrap();
    let conn = db.connect().unwrap();
    conn.execute_batch(r#"
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            level TEXT NOT NULL,
            service TEXT NOT NULL,
            body JSON NOT NULL
        );
    "#).await.unwrap();
    conn.execute_batch(r#"
        CREATE VIRTUAL TABLE IF NOT EXISTS logs_fts
        USING fts5(message, content='logs', content_rowid='id');
    "#).await.unwrap();
    conn.execute_batch(r#"
        CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(ts);
        CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level);
        CREATE INDEX IF NOT EXISTS idx_logs_service ON logs(service);
    "#).await.unwrap();
    conn
}

struct AppState {
    conn: Connection,
}

async fn ingest_ndjson(
    body: String,
    data: web::Data<AppState>,
) -> impl Responder {
    let hostname = get_hostname().unwrap_or_default().to_string_lossy().to_string();
    let pid = process::id();
    let mut tx = data.conn.transaction().await.unwrap();
    let mut count = 0;
    for (i, line) in body.lines().enumerate() {
        if line.trim().is_empty() { continue; }
        let mut v = match serde_json::from_str::<serde_json::Value>(line) {
            Ok(serde_json::Value::Object(map)) => map,
            _ => {
                error!("Invalid JSON at line {}", i+1);
                return HttpResponse::BadRequest().json(json!({"error": format!("Invalid JSON at line {}", i+1)}));
            }
        };
        let ts = match v.remove("timestamp").and_then(|v| v.as_str().map(|s| s.to_string())) {
            Some(val) => val,
            None => return HttpResponse::BadRequest().json(json!({"error": format!("Missing or invalid timestamp at line {}", i+1)})),
        };
        let level = match v.remove("level").and_then(|v| v.as_str().map(|s| s.to_string())) {
            Some(val) => val,
            None => return HttpResponse::BadRequest().json(json!({"error": format!("Missing or invalid level at line {}", i+1)})),
        };
        let service = match v.remove("service").and_then(|v| v.as_str().map(|s| s.to_string())) {
            Some(val) => val,
            None => return HttpResponse::BadRequest().json(json!({"error": format!("Missing or invalid service at line {}", i+1)})),
        };
        let message = match v.remove("message") {
            Some(val) => val,
            None => return HttpResponse::BadRequest().json(json!({"error": format!("Missing message at line {}", i+1)})),
        };
        // Enrich metadata
        v.insert("host_name".to_string(), serde_json::Value::String(hostname.clone()));
        v.insert("process_id".to_string(), serde_json::Value::Number(pid.into()));
        v.insert("message".to_string(), message.clone());
        let body_str = serde_json::Value::Object(v).to_string();
        if let Err(e) = tx.execute("INSERT INTO logs (ts, level, service, body) VALUES (?1, ?2, ?3, ?4)",
                                   (ts, level, service, body_str)
        ).await {
            error!("DB insert error: {:?}", e);
            return HttpResponse::InternalServerError().json(json!({"error": "Database insert error"}));
        }
        count += 1;
    }
    tx.commit().await.unwrap();
    info!("Ingested {} records", count);
    HttpResponse::Ok().json(json!({"ingested": count}))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let config = Config::parse();
    tracing_subscriber::fmt::init();
    let conn = init_db(&config.db_url).await;
    let state = web::Data::new(AppState { conn });
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/logs/ndjson", web::post().to(ingest_ndjson))
    })
    .bind(("0.0.0.0", config.port))?
    .run()
    .await
}
