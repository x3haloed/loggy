use actix_web::{web, App, HttpServer, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use libsql::{Builder, Connection};
use clap::Parser;
use hostname::get as get_hostname;
use std::process;
use tracing::{info, error};
use tracing_subscriber;
use chrono::{DateTime, NaiveDateTime, Utc};

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

// --- OTLP Ingestion types and handler ---
#[derive(Deserialize)]
struct ExportLogsServiceRequest {
    #[serde(rename = "resourceLogs")]
    resource_logs: Vec<ResourceLogs>,
}

#[derive(Deserialize)]
struct ResourceLogs {
    #[serde(rename = "instrumentationLibraryLogs")]
    instrumentation_library_logs: Vec<InstrumentationLibraryLogs>,
}

#[derive(Deserialize)]
struct InstrumentationLibraryLogs {
    logs: Vec<LogRecord>,
}

#[derive(Deserialize)]
struct LogRecord {
    #[serde(rename = "timeUnixNano")]
    time_unix_nano: String,
    #[serde(rename = "severityText")]
    severity_text: String,
    #[serde(rename = "body")]
    body: AnyValue,
    #[serde(default, rename = "attributes")]
    attributes: Vec<KeyValue>,
}

#[derive(Deserialize)]
struct KeyValue {
    key: String,
    value: AnyValue,
}

#[derive(Deserialize)]
struct AnyValue {
    #[serde(rename = "stringValue")]
    string_value: Option<String>,
    #[serde(rename = "boolValue")]
    bool_value: Option<bool>,
    #[serde(rename = "intValue")]
    int_value: Option<i64>,
    #[serde(rename = "doubleValue")]
    double_value: Option<f64>,
}

async fn ingest_otlp(
    req: web::Json<ExportLogsServiceRequest>,
    data: web::Data<AppState>,
) -> impl Responder {
    let hostname = get_hostname().unwrap_or_default().to_string_lossy().to_string();
    let pid = process::id();
    let mut tx = data.conn.transaction().await.unwrap();
    let mut count = 0;
    for resource in &req.resource_logs {
        for ils in &resource.instrumentation_library_logs {
            for log in &ils.logs {
                let ts_nano: u128 = log.time_unix_nano.parse().unwrap_or_default();
                let secs = (ts_nano / 1_000_000_000) as i64;
                let nsecs = (ts_nano % 1_000_000_000) as u32;
                let naive = NaiveDateTime::from_timestamp_opt(secs, nsecs)
                    .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0));
                let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);
                let ts = datetime.to_rfc3339();
                let level = log.severity_text.clone();
                let mut service = "unknown".to_string();
                for kv in &log.attributes {
                    if kv.key == "service.name" {
                        if let Some(ref s) = kv.value.string_value {
                            service = s.clone();
                        }
                    }
                }
                let mut map = serde_json::Map::new();
                for kv in &log.attributes {
                    let val = if let Some(ref s) = kv.value.string_value {
                        Value::String(s.clone())
                    } else if let Some(b) = kv.value.bool_value {
                        Value::Bool(b)
                    } else if let Some(i) = kv.value.int_value {
                        Value::Number(i.into())
                    } else if let Some(d) = kv.value.double_value {
                        serde_json::Number::from_f64(d).map(Value::Number).unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    };
                    map.insert(kv.key.clone(), val);
                }
                if let Some(ref msg) = log.body.string_value {
                    map.insert("message".to_string(), Value::String(msg.clone()));
                }
                map.insert("host_name".to_string(), Value::String(hostname.clone()));
                map.insert("process_id".to_string(), Value::Number(pid.into()));
                let body_str = Value::Object(map).to_string();
                if let Err(e) = tx.execute(
                    "INSERT INTO logs (ts, level, service, body) VALUES (?1, ?2, ?3, ?4)",
                    (ts, level, service.clone(), body_str),
                ).await {
                    error!("OTLP DB insert error: {:?}", e);
                    return HttpResponse::InternalServerError().json(json!({"error": "Database insert error"}));
                }
                count += 1;
            }
        }
    }
    tx.commit().await.unwrap();
    info!("OTLP Ingested {} records", count);
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
            .route("/v1/logs", web::post().to(ingest_otlp))
    })
    .bind(("0.0.0.0", config.port))?
    .run()
    .await
}
