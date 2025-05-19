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
use bytes::Bytes;
use actix_web::HttpRequest;
use tokio::sync::broadcast::{channel, Sender};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::IntervalStream;
use std::time::Duration;

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
    broadcaster: Sender<String>,
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
struct JsonRpcRequest {
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    params: Option<Value>,
}

/// SSE endpoint: establish connection and receive server messages
async fn mcp_sse(data: web::Data<AppState>, req: HttpRequest) -> impl Responder {
    // Log SSE connection attempt
    let conn_info = req.connection_info();
    let remote = conn_info.realip_remote_addr().unwrap_or("<unknown>");
    let origin_opt = req.headers().get("Origin").and_then(|h| h.to_str().ok());
    info!("MCP[SSE] connection attempt from {} with Origin {:?}", remote, origin_opt);
    
    // Check if this is a localhost connection - allow without Origin in dev
    let is_localhost = remote.starts_with("127.0.0.1") || 
                       remote.starts_with("::1") || 
                       remote.starts_with("localhost");
                       
    // Either require Origin header or allow if it's a localhost connection
    if req.headers().get("Origin").is_none() && !is_localhost {
        error!("MCP[SSE] connection rejected: missing Origin header from {}", remote);
        return HttpResponse::Forbidden().body("Missing Origin");
    }
    info!("MCP[SSE] connection established from {}", remote);
    let mut rx = data.broadcaster.subscribe();
    let init = tokio_stream::iter(vec![
        Ok::<Bytes, actix_web::Error>(Bytes::from_static(b"event: endpoint\ndata: /mcp/sse\n\n")),
    ]);
    // Keep-alive comments to satisfy SSE client timeouts
    let keep_alive = IntervalStream::new(tokio::time::interval(Duration::from_secs(15)))
        .map(|_| Ok::<Bytes, actix_web::Error>(Bytes::from_static(b": keep-alive\n\n")));
    let broadcast = BroadcastStream::new(rx).filter_map(|res| {
        match res {
            Ok(msg) => {
                let s = format!("event: message\ndata: {}\n\n", msg);
                Some(Ok::<Bytes, actix_web::Error>(Bytes::from(s)))
            }
            Err(_) => None,
        }
    });
    // Merge initial event, broadcast, and keep-alive streams
    let stream = init.merge(broadcast).merge(keep_alive);
    HttpResponse::Ok()
        .append_header(("Content-Type", "text/event-stream"))
        .append_header(("Cache-Control", "no-cache"))
        .streaming(stream)
}

/// POST endpoint: receive client messages
async fn mcp_post(body: String, data: web::Data<AppState>) -> impl Responder {
    // Log incoming JSON-RPC POST
    info!("MCP[POST] raw body: {}", body);
    let req: JsonRpcRequest = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => { error!("MCP[POST] JSON parse error: {} -- body: {}", e, body); return HttpResponse::BadRequest().body("Invalid JSON-RPC"); }
    };
    // Log parsed JSON-RPC request
    info!("MCP[POST] JSON-RPC request: method={}, id={:?}, params={:?}", req.method, req.id, req.params);
    if req.method == "notifications/initialized" && req.id.is_none() {
        // notification; nothing to do
        return HttpResponse::Ok().finish();
    }
    if let Some(id) = req.id.clone() {
        let mut response = json!({"jsonrpc": "2.0", "id": id.clone()});
        match req.method.as_str() {
            "initialize" => {
                response["result"] = json!({
                    "protocolVersion": "2025-03-26",
                    "capabilities": {"tools": {"listChanged": false}, "logging": {}, "resources": {}, "prompts": {}},
                    "serverInfo": {"name": "loggy", "version": env!("CARGO_PKG_VERSION")},
                    "instructions": ""
                });
            }
            "tools/list" => {
                let tools = vec![
                    json!({
                        "name": "list_services",
                        "description": "List all service names that have logged entries",
                        "inputSchema": { "type": "object", "properties": {}, "required": [] },
                        "annotations": {"title": "List Services", "readOnlyHint": true, "openWorldHint": false}
                    }),
                    json!({
                        "name": "search_logs",
                        "description": "Search logs by full-text message content",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "q": {"type": "string", "description": "Search query text"},
                                "limit": {"type": "integer", "description": "Maximum results to return"},
                                "offset": {"type": "integer", "description": "Number of results to skip"}
                            },
                            "required": ["q"]
                        },
                        "annotations": {"title": "Search Logs", "readOnlyHint": true, "openWorldHint": false}
                    }),
                    json!({
                        "name": "get_log",
                        "description": "Retrieve a single log entry by ID",
                        "inputSchema": {
                            "type": "object", 
                            "properties": {"id": {"type": "integer", "description": "Log entry ID"}}, 
                            "required": ["id"]
                        },
                        "annotations": {"title": "Get Log", "readOnlyHint": true, "openWorldHint": false}
                    })
                ];
                response["result"] = json!({"tools": tools});
            }
            "tools/call" => {
                if let Some(params) = &req.params {
                    // Extract tool name and arguments
                    let tool_name = params.get("name").and_then(Value::as_str);
                    let args = params.get("arguments");
                    
                    if let Some(name) = tool_name {
                        match name {
                            "list_services" => {
                                // Fetch distinct service names from the database
                                match list_distinct_services(&data.conn).await {
                                    Ok(services) => {
                                        let text = format!("Available services: {}", services.join(", "));
                                        response["result"] = json!({"content": [{"type": "text", "text": text}]});
                                    },
                                    Err(e) => {
                                        error!("Error listing services: {:?}", e);
                                        response["error"] = json!({"code": -32603, "message": "Database error"});
                                    }
                                }
                            },
                            "search_logs" => {
                                if let Some(args_obj) = args {
                                    let q = args_obj.get("q").and_then(Value::as_str).unwrap_or("");
                                    let limit = args_obj.get("limit").and_then(Value::as_u64).unwrap_or(10);
                                    let offset = args_obj.get("offset").and_then(Value::as_u64).unwrap_or(0);
                                    
                                    match search_logs(&data.conn, q, limit as i64, offset as i64).await {
                                        Ok(results) => {
                                            response["result"] = json!({"content": [{"type": "text", "text": results}]});
                                        },
                                        Err(e) => {
                                            error!("Error searching logs: {:?}", e);
                                            response["error"] = json!({"code": -32603, "message": "Database error"});
                                        }
                                    }
                                } else {
                                    response["error"] = json!({"code": -32602, "message": "Missing search query"});
                                }
                            },
                            "get_log" => {
                                if let Some(args_obj) = args {
                                    if let Some(id) = args_obj.get("id").and_then(Value::as_u64) {
                                        match get_log_by_id(&data.conn, id as i64).await {
                                            Ok(log_entry) => {
                                                response["result"] = json!({"content": [{"type": "text", "text": log_entry}]});
                                            },
                                            Err(e) => {
                                                error!("Error getting log: {:?}", e);
                                                response["error"] = json!({"code": -32603, "message": "Database error"});
                                            }
                                        }
                                    } else {
                                        response["error"] = json!({"code": -32602, "message": "Missing or invalid 'id' parameter"});
                                    }
                                } else {
                                    response["error"] = json!({"code": -32602, "message": "Missing arguments"});
                                }
                            },
                            _ => {
                                response["error"] = json!({"code": -32601, "message": format!("Tool not found: {}", name)});
                            }
                        }
                    } else {
                        response["error"] = json!({"code": -32602, "message": "Missing tool name"});
                    }
                } else {
                    response["error"] = json!({"code": -32602, "message": "Missing params"});
                }
            }
            _ => {
                response["error"] = json!({"code": -32601, "message": "Method not found"});
            }
        }
        // Respond synchronously with JSON-RPC response
        return HttpResponse::Ok().json(response);
    }
    HttpResponse::Ok().finish()
}

// Helper functions for tool implementation
async fn list_distinct_services(conn: &Connection) -> Result<Vec<String>, libsql::Error> {
    let mut rows = conn.query("SELECT DISTINCT service FROM logs ORDER BY service", ()).await?;
    let mut services = Vec::new();
    
    while let Some(row) = rows.next().await? {
        if let Ok(service) = row.get::<String>(0) {
            services.push(service);
        }
    }
    
    Ok(services)
}

async fn search_logs(conn: &Connection, query: &str, limit: i64, offset: i64) -> Result<String, libsql::Error> {
    let pattern = format!("%{}%", query);
    let sql = "SELECT json_group_array(json_object('id', id, 'ts', ts, 'level', level, 'service', service, 'body', body)) 
               FROM logs 
               WHERE json_extract(body, '$.message') LIKE ?1 
               LIMIT ?2 OFFSET ?3";
    
    let mut rows = conn.query(sql, libsql::params![pattern, limit, offset]).await?;
    
    if let Some(row) = rows.next().await? {
        let json_text: String = row.get(0)?;
        Ok(json_text)
    } else {
        Ok("[]".to_string())
    }
}

async fn get_log_by_id(conn: &Connection, id: i64) -> Result<String, libsql::Error> {
    let sql = "SELECT json_object('id', id, 'ts', ts, 'level', level, 'service', service, 'body', body) 
               FROM logs 
               WHERE id = ?1";
    
    let mut rows = conn.query(sql, libsql::params![id]).await?;
    
    if let Some(row) = rows.next().await? {
        let json_text: String = row.get(0)?;
        Ok(json_text)
    } else {
        Ok("{}".to_string())
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let config = Config::parse();
    tracing_subscriber::fmt::init();
    let conn = init_db(&config.db_url).await;
    let (broadcaster_tx, _) = channel(100);
    let state = web::Data::new(AppState { conn, db_url: config.db_url.clone(), broadcaster: broadcaster_tx.clone() });
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/logs/ndjson", web::post().to(ingest_ndjson))
            .route("/v1/logs", web::post().to(ingest_otlp))
            .route("/mcp/sse", web::get().to(mcp_sse))
            .route("/mcp/sse", web::post().to(mcp_post))
            .route("/healthz", web::get().to(health))
            .route("/metrics", web::get().to(metrics))
    })
    .bind(("0.0.0.0", config.port))?
    .run()
    .await
}
