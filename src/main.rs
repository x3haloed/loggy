use actix_web::{web, App, HttpServer, HttpResponse, Responder};
use serde::Deserialize;
use serde_json::{json, Value};
use libsql::{Builder, Connection};
use clap::Parser;
use hostname::get as get_hostname;
use std::process;
use tracing::{info, error};
use tracing_subscriber;
use chrono::{DateTime, NaiveDateTime, Utc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::fs;
use tokio_stream::StreamExt;
use libsql::Row;
use serde::Serialize;

// Metrics counters
static LOGS_INGESTED: AtomicU64 = AtomicU64::new(0);
static INGESTION_FAILURES: AtomicU64 = AtomicU64::new(0);

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
    db_url: String,
}

async fn ingest_ndjson(
    body: String,
    data: web::Data<AppState>,
) -> impl Responder {
    let hostname = get_hostname().unwrap_or_default().to_string_lossy().to_string();
    let pid = process::id();
    let tx = data.conn.transaction().await.unwrap();
    let mut count = 0;
    for (i, line) in body.lines().enumerate() {
        if line.trim().is_empty() { continue; }
        let mut v = match serde_json::from_str::<serde_json::Value>(line) {
            Ok(serde_json::Value::Object(map)) => map,
            _ => {
                error!("Invalid JSON at line {}", i+1);
                INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst);
                return HttpResponse::BadRequest().json(json!({"error": format!("Invalid JSON at line {}", i+1)}));
            }
        };
        let ts = match v.remove("timestamp").and_then(|v| v.as_str().map(|s| s.to_string())) {
            Some(val) => val,
            None => { INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst); return HttpResponse::BadRequest().json(json!({"error": format!("Missing or invalid timestamp at line {}", i+1)})); }
        };
        let level = match v.remove("level").and_then(|v| v.as_str().map(|s| s.to_string())) {
            Some(val) => val,
            None => { INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst); return HttpResponse::BadRequest().json(json!({"error": format!("Missing or invalid level at line {}", i+1)})); }
        };
        let service = match v.remove("service").and_then(|v| v.as_str().map(|s| s.to_string())) {
            Some(val) => val,
            None => { INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst); return HttpResponse::BadRequest().json(json!({"error": format!("Missing or invalid service at line {}", i+1)})); }
        };
        let message = match v.remove("message") {
            Some(val) => val,
            None => { INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst); return HttpResponse::BadRequest().json(json!({"error": format!("Missing message at line {}", i+1)})); }
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
            INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst);
            return HttpResponse::InternalServerError().json(json!({"error": "Database insert error"}));
        }
        count += 1;
    }
    tx.commit().await.unwrap();
    LOGS_INGESTED.fetch_add(count, Ordering::SeqCst);
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
    let tx = data.conn.transaction().await.unwrap();
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
                    INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst);
                    return HttpResponse::InternalServerError().json(json!({"error": "Database insert error"}));
                }
                count += 1;
            }
        }
    }
    tx.commit().await.unwrap();
    LOGS_INGESTED.fetch_add(count, Ordering::SeqCst);
    info!("OTLP Ingested {} records", count);
    HttpResponse::Ok().json(json!({"ingested": count}))
}

/// Health check endpoint
async fn health(data: web::Data<AppState>) -> impl Responder {
    if data.conn.execute("SELECT 1", ()).await.is_ok() {
        HttpResponse::Ok().body("OK")
    } else {
        HttpResponse::InternalServerError().body("DB unreachable")
    }
}

/// Prometheus-style metrics endpoint
async fn metrics(data: web::Data<AppState>) -> impl Responder {
    let ingested = LOGS_INGESTED.load(Ordering::SeqCst);
    let failed = INGESTION_FAILURES.load(Ordering::SeqCst);
    let db_size = if data.db_url == ":memory:" {
        0
    } else {
        fs::metadata(&data.db_url).map(|m| m.len()).unwrap_or(0)
    };
    let body = format!(
        "\
# HELP loggy_ingested_total Total number of log records ingested\n\
# TYPE loggy_ingested_total counter\n\
loggy_ingested_total {ingested}\n\
# HELP loggy_failed_ingestions_total Total number of failed ingestion requests\n\
# TYPE loggy_failed_ingestions_total counter\n\
loggy_failed_ingestions_total {failed}\n\
# HELP loggy_db_size_bytes Current size of the DB file in bytes\n\
# TYPE loggy_db_size_bytes gauge\n\
loggy_db_size_bytes {db_size}\n",
        ingested = ingested,
        failed = failed,
        db_size = db_size,
    );
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4")
        .body(body)
}

#[derive(Deserialize)]
struct CallToolRequest {
    name: String,
    arguments: Option<Value>,
}

/// MCP endpoint: list available tools
async fn list_tools() -> impl Responder {
    let tools = vec![
        json!({
            "name": "list_services",
            "description": "List distinct service names",
            "inputSchema": { "type": "object", "properties": {}, "required": [] },
            "annotations": {"title": "List Services", "readOnlyHint": true, "openWorldHint": false}
        }),
        json!({
            "name": "search_logs",
            "description": "Search logs by full-text message",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "q": {"type": "string"},
                    "limit": {"type": "integer"},
                    "offset": {"type": "integer"}
                },
                "required": ["q"]
            },
            "annotations": {"title": "Search Logs", "readOnlyHint": true, "openWorldHint": false}
        }),
        json!({
            "name": "get_log",
            "description": "Retrieve a single log by ID",
            "inputSchema": {"type": "object", "properties": {"id": {"type": "integer"}}, "required": ["id"]},
            "annotations": {"title": "Get Log", "readOnlyHint": true, "openWorldHint": false}
        })
    ];
    HttpResponse::Ok().json(json!({"tools": tools}))
}

/// MCP endpoint: call a tool by name
async fn call_tool(
    req: web::Json<CallToolRequest>,
    data: web::Data<AppState>,
) -> impl Responder {
    let name = req.name.as_str();
    match name {
        "list_services" => {
            // build JSON array of services
            let sql = "SELECT json_group_array(service) FROM (SELECT DISTINCT service FROM logs)";
            let mut rows = data.conn.query(sql, libsql::params![]).await.unwrap();
            if let Some(row) = rows.next().await.unwrap() {
                let arr: String = row.get(0).unwrap();
                let text = arr;
                HttpResponse::Ok().json(json!({"content": [{"type": "text", "text": text}]}))
            } else {
                HttpResponse::Ok().json(json!({"content": [{"type": "text", "text": "[]"}]}))
            }
        }
        "search_logs" => {
            let args = if let Some(a) = &req.arguments {
                a
            } else {
                return HttpResponse::BadRequest().json(json!({"isError": true, "content": [{"type": "text", "text": "Missing arguments for search_logs"}]}));
            };
            let q = args.get("q").and_then(Value::as_str).unwrap_or("");
            let limit = args.get("limit").and_then(Value::as_u64).unwrap_or(10);
            let offset = args.get("offset").and_then(Value::as_u64).unwrap_or(0);
            let pattern = format!("%{}%", q);
            // search JSON message field
            let sql = "SELECT json_group_array(json_object('id', id, 'ts', ts, 'level', level, 'service', service, 'body', body)) FROM logs WHERE json_extract(body, '$.message') LIKE ?1 LIMIT ?2 OFFSET ?3";
            let mut rows = data.conn.query(sql, libsql::params![pattern, limit as i64, offset as i64]).await.unwrap();
            if let Some(row) = rows.next().await.unwrap() {
                let arr: String = row.get(0).unwrap();
                let text = arr;
                HttpResponse::Ok().json(json!({"content": [{"type": "text", "text": text}]}))
            } else {
                HttpResponse::Ok().json(json!({"content": [{"type": "text", "text": "[]"}]}))
            }
        }
        "get_log" => {
            let args = if let Some(a) = &req.arguments {
                a
            } else {
                return HttpResponse::BadRequest().json(json!({"isError": true, "content": [{"type": "text", "text": "Missing arguments for get_log"}]}));
            };
            let id = args.get("id").and_then(Value::as_u64);
            if let Some(id) = id {
                let sql = "SELECT json_object('id', id, 'ts', ts, 'level', level, 'service', service, 'body', body) FROM logs WHERE id = ?1";
                let mut rows = data.conn.query(sql, libsql::params![id as i64]).await.unwrap();
                if let Some(row) = rows.next().await.unwrap() {
                    let obj: String = row.get(0).unwrap();
                    let text = obj;
                    HttpResponse::Ok().json(json!({"content": [{"type": "text", "text": text}]}))
                } else {
                    HttpResponse::Ok().json(json!({"content": [{"type": "text", "text": "{}"}]}))
                }
            } else {
                HttpResponse::BadRequest().json(json!({"isError": true, "content": [{"type": "text", "text": "Missing 'id' argument"}]}))
            }
        }
        _ => HttpResponse::BadRequest().json(json!({"isError": true, "content": [{"type": "text", "text": format!("Tool not found: {}", name)}]})),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let config = Config::parse();
    tracing_subscriber::fmt::init();
    let conn = init_db(&config.db_url).await;
    let state = web::Data::new(AppState { conn, db_url: config.db_url.clone() });
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/logs/ndjson", web::post().to(ingest_ndjson))
            .route("/v1/logs", web::post().to(ingest_otlp))
            .route("/tools/list", web::post().to(list_tools))
            .route("/tools/call", web::post().to(call_tool))
            .route("/healthz", web::get().to(health))
            .route("/metrics", web::get().to(metrics))
    })
    .bind(("0.0.0.0", config.port))?
    .run()
    .await
}
