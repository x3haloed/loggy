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
use tokio::sync::Mutex;
use std::collections::HashSet;
use lazy_static::lazy_static;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::IntervalStream;
use std::time::Duration;
use prost::Message;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest as ProtoExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue as ProtoAnyValue, KeyValue as ProtoKeyValue};
use opentelemetry_proto::tonic::common::v1::any_value::Value as ProtoAnyValueKind;

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
    port: u16,
    known_services: Mutex<HashSet<String>>,
    mcp_log_level: Mutex<String>,
}

lazy_static! {
    static ref VALID_LOG_LEVELS: HashSet<String> = {
        vec!["debug", "info", "notice", "warning", "error", "critical", "alert", "emergency"]
            .into_iter().map(String::from).collect()
    };
}

const INVALID_PARAMS: i32 = -32602;

async fn ingest_ndjson(
    body: String,
    data: web::Data<AppState>,
) -> impl Responder {
    let hostname = get_hostname().unwrap_or_default().to_string_lossy().to_string();
    let pid = process::id();
    let tx = data.conn.transaction().await.unwrap();
    let mut count = 0;
    let mut new_service_detected_in_batch = false; // Flag for new service detection
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
        let service_name = service.clone(); // Clone for use after v.remove("service") potentially
        let message = match v.remove("message") {
            Some(val) => val,
            None => { INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst); return HttpResponse::BadRequest().json(json!({"error": format!("Missing message at line {}", i+1)})); }
        };

        // Check if service_name is new
        {
            let mut known_services_guard = data.known_services.lock().await;
            if known_services_guard.insert(service_name.clone()) {
                new_service_detected_in_batch = true;
                info!("New service '{}' detected during NDJSON ingestion", service_name);
            }
        } // Lock released

        // Enrich metadata
        v.insert("host_name".to_string(), serde_json::Value::String(hostname.clone()));
        v.insert("process_id".to_string(), serde_json::Value::Number(pid.into()));
        v.insert("message".to_string(), message.clone());
        let body_str = serde_json::Value::Object(v).to_string();
        if let Err(e) = tx.execute("INSERT INTO logs (ts, level, service, body) VALUES (?1, ?2, ?3, ?4)",
                                   (ts, level, service_name, body_str) // Use service_name
        ).await {
            error!("DB insert error: {:?}", e);
            INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst);
            return HttpResponse::InternalServerError().json(json!({"error": "Database insert error"}));
        }
        count += 1;
    }
    tx.commit().await.unwrap();
    LOGS_INGESTED.fetch_add(count, Ordering::SeqCst);
    info!("Ingested {} records via NDJSON", count);

    // Send notification if new services were added
    if new_service_detected_in_batch {
        let notification = json!({
            "jsonrpc": "2.0",
            "method": "notifications/resources/list_changed",
            "params": {}
        });
        if let Err(e) = data.broadcaster.send(notification.to_string()) {
            error!("Failed to send resources/list_changed notification (NDJSON): {}", e);
        } else {
            info!("Sent notifications/resources/list_changed notification (NDJSON)");
        }
    }

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
    req: HttpRequest,
    body: Bytes,
    data: web::Data<AppState>,
) -> impl Responder {
    let hostname = get_hostname().unwrap_or_default().to_string_lossy().to_string();
    let pid = process::id();
    let tx = data.conn.transaction().await.unwrap();
    let mut count = 0;
    let mut new_service_detected_in_batch = false; // Flag for new service detection
    // dispatch based on Content-Type
    let ct = req.headers().get("content-type").and_then(|h| h.to_str().ok()).unwrap_or("");
    if ct.starts_with("application/x-protobuf") {
        // Protobuf payload
        let proto_req = match ProtoExportLogsServiceRequest::decode(&*body) {
            Ok(r) => r,
            Err(e) => {
                error!("Protobuf decode error ingest_otlp: {:?}", e);
                INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst);
                return HttpResponse::BadRequest().json(json!({"error": "Protobuf decode error"}));
            }
        };
        for resource in proto_req.resource_logs {
            for scope in resource.scope_logs {
                for log in scope.log_records {
                    let ts_nano: u128 = log.time_unix_nano as u128;
                    let secs = (ts_nano / 1_000_000_000) as i64;
                    let nsecs = (ts_nano % 1_000_000_000) as u32;
                    let naive = NaiveDateTime::from_timestamp_opt(secs, nsecs)
                        .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0));
                    let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);
                    let ts = datetime.to_rfc3339();
                    let level = log.severity_text.clone();
                    let mut service_name = "unknown".to_string();
                    for kv in &log.attributes {
                        if kv.key == "service.name" {
                            if let Some(ProtoAnyValue { value: Some(ProtoAnyValueKind::StringValue(s)) }) = &kv.value {
                                service_name = s.clone();
                            }
                        }
                    }

                    // Check if service_name is new
                    {
                        let mut known_services_guard = data.known_services.lock().await;
                        if known_services_guard.insert(service_name.clone()) {
                            new_service_detected_in_batch = true;
                            info!("New service '{}' detected during OTLP ingestion", service_name);
                        }
                    } // Lock released

                    let mut map = serde_json::Map::new();
                    for kv in &log.attributes {
                        let val = match &kv.value {
                            Some(ProtoAnyValue { value: Some(ProtoAnyValueKind::StringValue(s)) }) => Value::String(s.clone()),
                            Some(ProtoAnyValue { value: Some(ProtoAnyValueKind::BoolValue(b)) }) => Value::Bool(*b),
                            Some(ProtoAnyValue { value: Some(ProtoAnyValueKind::IntValue(i)) }) => Value::Number((*i).into()),
                            Some(ProtoAnyValue { value: Some(ProtoAnyValueKind::DoubleValue(d)) }) => serde_json::Number::from_f64(*d).map(Value::Number).unwrap_or(Value::Null),
                            _ => Value::Null,
                        };
                        map.insert(kv.key.clone(), val);
                    }
                    if let Some(ProtoAnyValue { value: Some(ProtoAnyValueKind::StringValue(msg)) }) = &log.body {
                        map.insert("message".to_string(), Value::String(msg.clone()));
                    }
                    map.insert("host_name".to_string(), Value::String(hostname.clone()));
                    map.insert("process_id".to_string(), Value::Number(pid.into()));
                    let body_str = Value::Object(map).to_string();
                    if let Err(e) = tx.execute(
                        "INSERT INTO logs (ts, level, service, body) VALUES (?1, ?2, ?3, ?4)",
                        (ts, level.clone(), service_name.clone(), body_str), // Use service_name
                    ).await {
                        error!("OTLP DB insert error: {:?}", e);
                        INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst);
                        return HttpResponse::InternalServerError().json(json!({"error": "Database insert error"}));
                    }
                    count += 1;
                }
            }
        }
    } else {
        error!("OTLP parse error: invalid protobuf payload");
        INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst);
        return HttpResponse::BadRequest().json(json!({"error": "Invalid OTLP protobuf payload"}));
    }
    tx.commit().await.unwrap();
    LOGS_INGESTED.fetch_add(count, Ordering::SeqCst);
    info!("OTLP Ingested {} records", count);

    // Send notification if new services were added
    if new_service_detected_in_batch {
        let notification = json!({
            "jsonrpc": "2.0",
            "method": "notifications/resources/list_changed",
            "params": {}
        });
        if let Err(e) = data.broadcaster.send(notification.to_string()) {
            error!("Failed to send resources/list_changed notification (OTLP): {}", e);
        } else {
            info!("Sent notifications/resources/list_changed notification (OTLP)");
        }
    }

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

    const MCP_PAGE_SIZE: usize = 10;

    // Handle Notifications (req.id is None)
    if req.id.is_none() {
        match req.method.as_str() {
            "notifications/initialized" => {
                info!("Received notifications/initialized");
                // Potentially clear any pending state related to a previous session if needed.
            }
            "notifications/cancelled" => {
                let request_id_val = req.params.as_ref().and_then(|p| p.get("requestId")).cloned(); // Cloned to own Value
                let reason_val = req.params.as_ref()
                    .and_then(|p| p.get("reason"))
                    .and_then(Value::as_str)
                    .map(String::from);
                info!("Received notifications/cancelled for requestId: {:?}, reason: {:?}", request_id_val, reason_val);
                // Here, you might add logic to mark the corresponding request_id_val as cancelled
                // if you are tracking cancellable operations.
            }
            // MCP specifies these are server-to-client, so server receiving them is an error.
            "notifications/resources/list_changed" | 
            "notifications/tools/list_changed" | 
            "notifications/prompts/list_changed" | 
            "notifications/message" | 
            "notifications/roots/list_changed" | 
            "notifications/resources/updated" => {
                 error!("Server received unexpected client-bound notification: {}", req.method);
            }
            _ => {
                warn!("Received unhandled notification: {}", req.method);
            }
        }
        return HttpResponse::Ok().finish(); // All notifications get an empty 200 OK.
    }

    // Handle Requests (req.id is Some)
    // req.id is definitely Some here, so we can unwrap it.
    let id = req.id.expect("req.id should be Some for requests, this is a bug in logic flow.");
    let mut response = json!({"jsonrpc": "2.0", "id": id.clone()}); // id.clone() because it's used in response

    match req.method.as_str() {
        "initialize" => {
                response["result"] = json!({
                    "protocolVersion": "2025-03-26",
                    "capabilities": {
                        "tools": {"listChanged": false},
                        "logging": {},
                        "resources": {"subscribe": true, "listChanged": true},
                        "prompts": {"listChanged": false}, // Assuming prompts are static for now
                        "completions": {} // Add completions capability
                    },
                    "serverInfo": {"name": "loggy", "version": env!("CARGO_PKG_VERSION")},
                    "instructions": ""
                });
            }
            "tools/list" => {
                // Define all tools data (as it was before pagination)
                let all_tools_data = vec![
                    json!({
                        "name": "list_services",
                        "description": "List all service names that have logged entries",
                        "inputSchema": { "type": "object", "properties": {}, "required": [] },
                        "annotations": {
                            "title": "List Services",
                            "readOnlyHint": true,
                            "destructiveHint": false,
                            "idempotentHint": true,
                            "openWorldHint": false
                        }
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
                        "annotations": {
                            "title": "Search Logs",
                            "readOnlyHint": true,
                            "destructiveHint": false,
                            "idempotentHint": true,
                            "openWorldHint": false
                        }
                    }),
                    json!({
                        "name": "get_log",
                        "description": "Retrieve a single log entry by ID",
                        "inputSchema": {
                            "type": "object", 
                            "properties": {"id": {"type": "integer", "description": "Log entry ID"}}, 
                            "required": ["id"]
                        },
                        "annotations": {
                            "title": "Get Log",
                            "readOnlyHint": true,
                            "destructiveHint": false,
                            "idempotentHint": true,
                            "openWorldHint": false
                        }
                    }),
                    json!({
                        "name": "tail_logs",
                        "description": "Stream the last N lines (optionally filtered by service & level)",
                        "inputSchema": {"type": "object", "properties": {"service": {"type": "string"}, "level": {"type": "string"}, "lines": {"type": "integer"}}, "required": []},
                        "annotations": {
                            "title": "Tail Logs",
                            "readOnlyHint": true,
                            "destructiveHint": false,
                            "idempotentHint": false,
                            "openWorldHint": false
                        }
                    }),
                    json!({
                        "name": "search_logs_around",
                        "description": "Fetch log entries before & after a given ID",
                        "inputSchema": {"type": "object", "properties": {"id": {"type": "integer"}, "before": {"type": "integer"}, "after": {"type": "integer"}}, "required": ["id"]},
                        "annotations": {
                            "title": "Search Logs Around",
                            "readOnlyHint": true,
                            "destructiveHint": false,
                            "idempotentHint": true,
                            "openWorldHint": false
                        }
                    }),
                    json!({
                        "name": "summarize_logs",
                        "description": "Natural-language summary of logs over a time window",
                        "inputSchema": {"type": "object", "properties": {"service": {"type": "string"}, "level": {"type": "string"}, "minutes": {"type": "integer"}}, "required": ["minutes"]},
                        "annotations": {
                            "title": "Summarize Logs",
                            "readOnlyHint": true,
                            "destructiveHint": false,
                            "idempotentHint": true,
                            "openWorldHint": false
                        }
                    }),
                    json!({
                        "name": "get_metrics",
                        "description": "Fetch current ingestion and DB metrics",
                        "inputSchema": {"type": "object", "properties": {}, "required": []},
                        "annotations": {
                            "title": "Get Metrics",
                            "readOnlyHint": true,
                            "destructiveHint": false,
                            "idempotentHint": true,
                            "openWorldHint": false
                        }
                    }),
                ];

                let current_offset: usize = req.params.as_ref()
                    .and_then(|p| p.get("cursor"))
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);

                let start_index = current_offset;
                let end_index = std::cmp::min(start_index + MCP_PAGE_SIZE, all_tools_data.len());
                
                let page_tools = if start_index < end_index {
                    all_tools_data[start_index..end_index].to_vec()
                } else {
                    vec![]
                };

                response["result"] = json!({"tools": page_tools});

                if end_index < all_tools_data.len() {
                    response["result"]["nextCursor"] = json!(end_index.to_string());
                }
            }
            "tools/call" => {
                if let Some(params_val) = &req.params { // Renamed to avoid conflict with shadowing 'params' if any
                    // Extract tool name and arguments
                    let tool_name = params_val.get("name").and_then(Value::as_str);
                    let args = params_val.get("arguments");
                    
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
                                    // Check for required 'q' parameter
                                    if let Some(q_val) = args_obj.get("q") {
                                        if let Some(q_str) = q_val.as_str() {
                                            if q_str.is_empty() {
                                                response["result"] = json!({
                                                    "content": [{"type": "text", "text": "Error: Missing or empty 'q' parameter for tool search_logs"}],
                                                    "isError": true
                                                });
                                            } else {
                                                let limit = args_obj.get("limit").and_then(Value::as_u64).unwrap_or(10);
                                                let offset = args_obj.get("offset").and_then(Value::as_u64).unwrap_or(0);
                                                match search_logs(&data.conn, q_str, limit as i64, offset as i64).await {
                                                    Ok(results) => {
                                                        response["result"] = json!({"content": [{"type": "text", "text": results}]});
                                                    },
                                                    Err(e) => {
                                                        error!("Error searching logs: {:?}", e);
                                                        response["error"] = json!({"code": -32603, "message": "Database error"});
                                                    }
                                                }
                                            }
                                        } else {
                                             response["result"] = json!({
                                                "content": [{"type": "text", "text": "Error: Invalid 'q' parameter, must be a string for tool search_logs"}],
                                                "isError": true
                                            });
                                        }
                                    } else {
                                        response["result"] = json!({
                                            "content": [{"type": "text", "text": "Error: Missing 'q' parameter for tool search_logs"}],
                                            "isError": true
                                        });
                                    }
                                } else {
                                    response["result"] = json!({
                                        "content": [{"type": "text", "text": "Error: Missing arguments for tool search_logs"}],
                                        "isError": true
                                    });
                                }
                            },
                            "get_log" => {
                                if let Some(args_obj) = args {
                                    if let Some(id_val) = args_obj.get("id") {
                                        if let Some(id_u64) = id_val.as_u64() {
                                            match get_log_by_id(&data.conn, id_u64 as i64).await {
                                                Ok(log_entry) => {
                                                    response["result"] = json!({"content": [{"type": "text", "text": log_entry}]});
                                                },
                                                Err(e) => {
                                                    error!("Error getting log: {:?}", e);
                                                    response["error"] = json!({"code": -32603, "message": "Database error"});
                                                }
                                            }
                                        } else {
                                            response["result"] = json!({
                                                "content": [{"type": "text", "text": "Error: Invalid 'id' parameter, must be an integer for tool get_log"}],
                                                "isError": true
                                            });
                                        }
                                    } else {
                                        response["result"] = json!({
                                            "content": [{"type": "text", "text": "Error: Missing 'id' parameter for tool get_log"}],
                                            "isError": true
                                        });
                                    }
                                } else {
                                    response["result"] = json!({
                                        "content": [{"type": "text", "text": "Error: Missing arguments for tool get_log"}],
                                        "isError": true
                                    });
                                }
                            },
                            "tail_logs" => {
                                if let Some(args_obj) = args { // args can be an empty object {}
                                    let lines_n = args_obj.get("lines").and_then(Value::as_u64).unwrap_or(10) as i64;
                                    let service_opt = args_obj.get("service").and_then(Value::as_str);
                                    let level_opt = args_obj.get("level").and_then(Value::as_str);
                                    match tail_logs(&data.conn, service_opt, level_opt, lines_n).await {
                                        Ok(text) => {
                                            response["result"] = json!({"content": [{"type": "text", "text": text}]});
                                        },
                                        Err(e) => {
                                            error!("Error tailing logs: {:?}", e);
                                            response["error"] = json!({"code": -32603, "message": "Database error"});
                                        }
                                    }
                                } else {
                                    // This case should ideally not be hit if inputSchema defines properties as optional.
                                    // However, if `arguments` itself is missing, it's an issue.
                                    response["result"] = json!({
                                        "content": [{"type": "text", "text": "Error: Missing arguments for tool tail_logs"}],
                                        "isError": true
                                    });
                                }
                            },
                            "search_logs_around" => {
                                if let Some(args_obj) = args {
                                    if let Some(id_val) = args_obj.get("id") {
                                        if let Some(id_u64) = id_val.as_u64() {
                                            let before_n = args_obj.get("before").and_then(Value::as_u64).unwrap_or(0) as i64;
                                            let after_n = args_obj.get("after").and_then(Value::as_u64).unwrap_or(0) as i64;
                                            match search_logs_around(&data.conn, id_u64 as i64, before_n, after_n).await {
                                                Ok(text) => {
                                                    response["result"] = json!({"content": [{"type": "text", "text": text}]});
                                                },
                                                Err(e) => {
                                                    error!("Error fetching logs around: {:?}", e);
                                                    response["error"] = json!({"code": -32603, "message": "Database error"});
                                                }
                                            }
                                        } else {
                                            response["result"] = json!({
                                                "content": [{"type": "text", "text": "Error: Invalid 'id' parameter, must be an integer for tool search_logs_around"}],
                                                "isError": true
                                            });
                                        }
                                    } else {
                                        response["result"] = json!({
                                            "content": [{"type": "text", "text": "Error: Missing 'id' parameter for tool search_logs_around"}],
                                            "isError": true
                                        });
                                    }
                                } else {
                                    response["result"] = json!({
                                        "content": [{"type": "text", "text": "Error: Missing arguments for tool search_logs_around"}],
                                        "isError": true
                                    });
                                }
                            },
                            "summarize_logs" => {
                                if let Some(args_obj) = args {
                                    if let Some(minutes_val) = args_obj.get("minutes") {
                                        if let Some(minutes_u64) = minutes_val.as_u64(){
                                            let service_opt = args_obj.get("service").and_then(Value::as_str);
                                            let level_opt = args_obj.get("level").and_then(Value::as_str);
                                            match summarize_logs(&data.conn, service_opt, level_opt, minutes_u64 as i64).await {
                                                Ok(text) => {
                                                    response["result"] = json!({"content": [{"type": "text", "text": text}]});
                                                },
                                                Err(e) => {
                                                    error!("Error summarizing logs: {:?}", e);
                                                    response["error"] = json!({"code": -32603, "message": "Database error"});
                                                }
                                            }
                                        } else {
                                            response["result"] = json!({
                                                "content": [{"type": "text", "text": "Error: Invalid 'minutes' parameter, must be an integer for tool summarize_logs"}],
                                                "isError": true
                                            });
                                        }
                                    } else {
                                         response["result"] = json!({
                                            "content": [{"type": "text", "text": "Error: Missing 'minutes' parameter for tool summarize_logs"}],
                                            "isError": true
                                        });
                                    }
                                } else {
                                    response["result"] = json!({
                                        "content": [{"type": "text", "text": "Error: Missing arguments for tool summarize_logs"}],
                                        "isError": true
                                    });
                                }
                            },
                            "get_metrics" => {
                                // This tool takes no arguments, so no specific argument error handling here.
                                // The check for `args` not being None (if it were strictly enforced) would be outside.
                                match get_metrics_text(&data.conn, &data.db_url).await {
                                    Ok(text) => {
                                        response["result"] = json!({"content": [{"type": "text", "text": text}]});
                                    },
                                    Err(e) => {
                                        error!("Error getting metrics: {:?}", e);
                                        response["error"] = json!({"code": -32603, "message": "Database error"});
                                    }
                                }
                            },
                            _ => {
                                response["result"] = json!({
                                    "content": [{"type": "text", "text": format!("Error: Tool not found: {}", name)}],
                                    "isError": true
                                });
                            }
                        }
                    } else {
                        response["result"] = json!({
                            "content": [{"type": "text", "text": "Error: Missing tool name"}],
                            "isError": true
                        });
                    }
                } else {
                    response["result"] = json!({
                        "content": [{"type": "text", "text": "Error: Missing params for tools/call"}],
                        "isError": true
                    });
                }
            }
            "resources/list" => {
                let current_offset: usize = req.params.as_ref()
                    .and_then(|p| p.get("cursor"))
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);

                let mut resources_on_page = Vec::new();
                let mut final_next_cursor: Option<usize> = None;

                let static_resources_defs = [
                    ("file:///loggy/logs/all.log", "all.log", "All logs", "text/plain"),
                    ("file:///loggy/config.toml", "config.toml", "Loggy configuration", "text/plain"),
                    ("file:///loggy/metrics.txt", "metrics.txt", "Metrics snapshot", "text/plain")
                ];
                let num_static_resources = static_resources_defs.len();

                let mut effective_idx = current_offset;
                while resources_on_page.len() < MCP_PAGE_SIZE {
                    if effective_idx < num_static_resources {
                        let (uri, name, description, mime_type) = static_resources_defs[effective_idx];
                        let size = match name {
                            "all.log" => fetch_all_logs_text(&data.conn).await.unwrap_or_default().len(),
                            "config.toml" => format!("port = {}\ndb_url = \"{}\"\n", data.port, data.db_url).len(),
                            "metrics.txt" => {
                                let ing = LOGS_INGESTED.load(Ordering::SeqCst);
                                let f = INGESTION_FAILURES.load(Ordering::SeqCst);
                                let db_sz = if data.db_url == ":memory:" { 0 } else { fs::metadata(&data.db_url).map(|m| m.len()).unwrap_or(0) };
                                format!("# HELP loggy_ingested_total...\nloggy_ingested_total {ingested}\n# HELP loggy_failed_ingestions_total...\nloggy_failed_ingestions_total {failed}\n# HELP loggy_db_size_bytes...\nloggy_db_size_bytes {db_size}\n", ingested=ing, failed=f, db_size=db_sz).len()
                            },
                            _ => 0, 
                        };
                        resources_on_page.push(json!({
                            "uri": uri,
                            "name": name,
                            "description": description,
                            "mimeType": mime_type,
                            "size": size
                        }));
                    } else {
                        // Dynamic resource part
                        let db_offset = effective_idx - num_static_resources;
                        let services = list_distinct_services(&data.conn, 1, db_offset as i64).await.unwrap_or_default();
                        if let Some(service_name) = services.get(0) {
                            let service_logs_content = fetch_service_logs_text(&data.conn, service_name).await.unwrap_or_default();
                            resources_on_page.push(json!({
                                "uri": format!("file:///loggy/logs/{}.log", service_name),
                                "name": format!("{}.log", service_name),
                                "description": format!("Log snapshot for {}", service_name),
                                "mimeType": "text/plain",
                                "size": service_logs_content.len()
                            }));
                        } else {
                            break; 
                        }
                    }
                    effective_idx += 1;
                }

                // Check if there's a next item to determine nextCursor
                if effective_idx < num_static_resources { 
                    final_next_cursor = Some(effective_idx);
                } else { 
                    let db_offset_check = effective_idx - num_static_resources;
                    let services_check = list_distinct_services(&data.conn, 1, db_offset_check as i64).await.unwrap_or_default();
                    if !services_check.is_empty() {
                        final_next_cursor = Some(effective_idx);
                    }
                }

                response["result"] = json!({"resources": resources_on_page});
                if let Some(cursor_val) = final_next_cursor {
                    response["result"]["nextCursor"] = json!(cursor_val.to_string());
                }
            }
            "resources/read" => {
                if let Some(p) = &req.params {
                    if let Some(uri) = p.get("uri").and_then(Value::as_str) {
                        let mut content = json!({"uri": uri, "mimeType": "text/plain", "text": ""});
                        if uri.ends_with("all.log") {
                            if let Ok(text) = fetch_all_logs_text(&data.conn).await {
                                content["text"] = json!(text);
                            }
                        } else if uri.contains("/logs/") && uri.ends_with(".log") {
                            let svc = uri.trim_start_matches("file:///loggy/logs/").trim_end_matches(".log");
                            if let Ok(text) = fetch_service_logs_text(&data.conn, svc).await {
                                content["text"] = json!(text);
                            }
                        } else if uri.ends_with("config.toml") {
                            let toml = format!("port = {}\ndb_url = \"{}\"\n", data.port, data.db_url);
                            content["text"] = json!(toml);
                        } else if uri.ends_with("metrics.txt") {
                            let ing = LOGS_INGESTED.load(Ordering::SeqCst);
                            let f = INGESTION_FAILURES.load(Ordering::SeqCst);
                            let db_sz = if data.db_url == ":memory:" { 0 } else { fs::metadata(&data.db_url).map(|m| m.len()).unwrap_or(0) };
                            let body = format!("# HELP loggy_ingested_total...\nloggy_ingested_total {ingested}\n# HELP loggy_failed_ingestions_total...\nloggy_failed_ingestions_total {failed}\n# HELP loggy_db_size_bytes...\nloggy_db_size_bytes {db_size}\n", ingested=ing, failed=f, db_size=db_sz);
                            content["text"] = json!(body);
                        }
                        response["result"] = json!({"contents": [content]});
                    } else {
                        response["error"] = json!({"code": -32602, "message": "Missing uri param"});
                    }
                } else {
                    response["error"] = json!({"code": -32602, "message": "Missing params"});
                }
            }
            "resources/templates/list" => {
                // Define all templates data (as it was before pagination)
                let all_templates_data = vec![
                    json!({
                        "uriTemplate": "logquery:///{service}/{level}/{startTs}/{endTs}",
                        "name": "Time Range Logs",
                        "description": "Select logs for a given service & level between two ISO-8601 timestamps",
                        "mimeType": "application/json",
                        "annotations": {
                            "audience": ["assistant"],
                            "priority": 0.5
                        }
                    }),
                    json!({
                        "uriTemplate": "logentry:///{id}?before={n}&after={m}",
                        "name": "Entry Drilldown",
                        "description": "Fetch N entries before and M entries after a log ID",
                        "mimeType": "application/json",
                        "annotations": {
                            "audience": ["assistant"],
                            "priority": 0.5
                        }
                    }),
                    json!({
                        "uriTemplate": "file:///project/src/{path}",
                        "name": "Source Code Files",
                        "description": "Fetch source code snippets by path",
                        "mimeType": "text/x-rust",
                        "annotations": {
                            "audience": ["assistant"],
                            "priority": 0.5
                        }
                    }),
                ];

                let current_offset: usize = req.params.as_ref()
                    .and_then(|p| p.get("cursor"))
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);

                let start_index = current_offset;
                let end_index = std::cmp::min(start_index + MCP_PAGE_SIZE, all_templates_data.len());

                let page_templates = if start_index < end_index {
                    all_templates_data[start_index..end_index].to_vec()
                } else {
                    vec![]
                };
                
                response["result"] = json!({"resourceTemplates": page_templates});

                if end_index < all_templates_data.len() {
                    response["result"]["nextCursor"] = json!(end_index.to_string());
                }
            }
            "logging/setLevel" => {
                match req.params.as_ref().and_then(|p| p.get("level")).and_then(Value::as_str) {
                    Some(level_str) => {
                        if VALID_LOG_LEVELS.contains(level_str) {
                            let mut app_level = data.mcp_log_level.lock().await;
                            *app_level = level_str.to_string();
                            info!("MCP log level set to: {}", level_str);
                            response["result"] = json!({});
                        } else {
                            response["error"] = json!({
                                "code": INVALID_PARAMS,
                                "message": format!("Invalid 'level' value: {}. Must be one of {:?}", level_str, *VALID_LOG_LEVELS)
                            });
                        }
                    }
                    None => {
                        response["error"] = json!({
                            "code": INVALID_PARAMS,
                            "message": "Missing or invalid 'level' parameter. It must be a string."
                        });
                    }
                }
            }
            "completion/complete" => {
                match req.params.as_ref()
                    .and_then(|p| p.get("argument"))
                    .and_then(|a| a.get("value"))
                    .and_then(Value::as_str)
                {
                    Some(partial_value_str) => {
                        let partial_value_lower = partial_value_str.to_lowercase();

                        // Static list of tool names for this basic implementation
                        let tool_names = vec![
                            "list_services", "search_logs", "get_log", 
                            "tail_logs", "search_logs_around", "summarize_logs", 
                            "get_metrics"
                        ];

                        let suggested_values: Vec<String> = tool_names
                            .into_iter()
                            .filter(|name| name.to_lowercase().starts_with(&partial_value_lower))
                            .map(String::from) 
                            .collect();

                        let original_suggestion_count = suggested_values.len();
                        let limited_values: Vec<String> = suggested_values.into_iter().take(100).collect();
                        let has_more = original_suggestion_count > limited_values.len();

                        response["result"] = json!({
                            "completion": {
                                "values": limited_values,
                                "total": original_suggestion_count,
                                "hasMore": has_more
                            }
                        });
                    }
                    None => {
                        response["error"] = json!({
                            "code": INVALID_PARAMS,
                            "message": "Missing or invalid 'argument.value' parameter for completion/complete. It must be a string."
                        });
                    }
                }
            }
            "resources/subscribe" => {
                if let Some(p) = &req.params {
                    if let Some(uri) = p.get("uri").and_then(Value::as_str) {
                        response["result"] = json!({});
                        let notif = json!({"jsonrpc": "2.0", "method": "notifications/resources/updated", "params": {"uri": uri}});
                        let _ = data.broadcaster.send(notif.to_string());
                    } else {
                        response["error"] = json!({"code": -32602, "message": "Missing uri param"});
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
    // Fallback if somehow a request (id=Some) does not match any method,
    // though the `_` case in the match should ideally handle "Method not found".
    // This line might be unreachable if the match covers all possibilities or has a proper default.
    warn!("Request with id {:?} did not match any known method and wasn't handled by default.", id);
    HttpResponse::InternalServerError().body("Unhandled request method despite having an ID.")
}

// Helper functions for tool implementation
async fn list_distinct_services(conn: &Connection, limit: i64, offset: i64) -> Result<Vec<String>, libsql::Error> {
    let mut rows = conn.query("SELECT DISTINCT service FROM logs ORDER BY service LIMIT ? OFFSET ?", libsql::params![limit, offset]).await?;
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

// Helper to read all logs as plain text
async fn fetch_all_logs_text(conn: &Connection) -> Result<String, libsql::Error> {
    let mut rows = conn.query(
        "SELECT ts || ' [' || level || '] ' || json_extract(body, '$.message') as line FROM logs ORDER BY id", 
        ()
    ).await?;
    let mut buf = String::new();
    while let Some(row) = rows.next().await? {
        let line: String = row.get(0)?;
        buf.push_str(&line);
        buf.push('\n');
    }
    Ok(buf)
}

// Helper to read logs for one service as plain text
async fn fetch_service_logs_text(conn: &Connection, service: &str) -> Result<String, libsql::Error> {
    let mut rows = conn.query(
        "SELECT ts || ' [' || level || '] ' || json_extract(body, '$.message') as line FROM logs WHERE service = ?1 ORDER BY id",
        libsql::params![service]
    ).await?;
    let mut buf = String::new();
    while let Some(row) = rows.next().await? {
        let line: String = row.get(0)?;
        buf.push_str(&line);
        buf.push('\n');
    }
    Ok(buf)
}

// Add helper function for tail_logs
async fn tail_logs(conn: &Connection, _service: Option<&str>, _level: Option<&str>, lines: i64) -> Result<String, libsql::Error> {
    let all_text = fetch_all_logs_text(conn).await?;
    let lines_vec: Vec<&str> = all_text.lines().collect();
    let start = if (lines as usize) < lines_vec.len() { lines_vec.len() - lines as usize } else { 0 };
    let selected = &lines_vec[start..];
    Ok(selected.join("\n"))
}

// Add helper functions for the remaining tools
async fn search_logs_around(conn: &Connection, id: i64, before: i64, after: i64) -> Result<String, libsql::Error> {
    // Fetch entries before the ID
    let sql_before = format!(
        "SELECT json_object('id', id, 'ts', ts, 'level', level, 'service', service, 'body', body) FROM logs WHERE id < {} ORDER BY id DESC LIMIT {}",
        id, before
    );
    let mut rows_before = conn.query(&sql_before, ()).await?;
    let mut entries = Vec::new();
    while let Some(row) = rows_before.next().await? {
        let json: String = row.get(0)?;
        entries.push(json);
    }
    entries.reverse();
    // Center entry
    if let Ok(center) = get_log_by_id(conn, id).await {
        if center != "{}" {
            entries.push(center);
        }
    }
    // Entries after the ID
    let sql_after = format!(
        "SELECT json_object('id', id, 'ts', ts, 'level', level, 'service', service, 'body', body) FROM logs WHERE id > {} ORDER BY id ASC LIMIT {}",
        id, after
    );
    let mut rows_after = conn.query(&sql_after, ()).await?;
    while let Some(row) = rows_after.next().await? {
        let json: String = row.get(0)?;
        entries.push(json);
    }
    Ok(format!("[{}]", entries.join(",")))
}

async fn summarize_logs(conn: &Connection, service: Option<&str>, level: Option<&str>, minutes: i64) -> Result<String, libsql::Error> {
    let since = (Utc::now() - chrono::Duration::minutes(minutes)).to_rfc3339();
    // Total count
    let sql_total = format!("SELECT COUNT(*) FROM logs WHERE ts >= '{}'", since);
    let mut total_rows = conn.query(&sql_total, ()).await?;
    let total = total_rows.next().await?.map(|r| r.get::<i64>(0).unwrap_or(0)).unwrap_or(0);
    // Breakdown by level
    let sql_breakdown = if let Some(svc) = service {
        if let Some(lvl) = level {
            format!(
                "SELECT level, COUNT(*) FROM logs WHERE ts >= '{}' AND service = '{}' AND level = '{}' GROUP BY level",
                since, svc, lvl
            )
        } else {
            format!(
                "SELECT level, COUNT(*) FROM logs WHERE ts >= '{}' AND service = '{}' GROUP BY level",
                since, svc
            )
        }
    } else if let Some(lvl) = level {
        format!(
            "SELECT level, COUNT(*) FROM logs WHERE ts >= '{}' AND level = '{}' GROUP BY level",
            since, lvl
        )
    } else {
        format!(
            "SELECT level, COUNT(*) FROM logs WHERE ts >= '{}' GROUP BY level",
            since
        )
    };
    let mut breakdown_rows = conn.query(&sql_breakdown, ()).await?;
    let mut parts = Vec::new();
    while let Some(row) = breakdown_rows.next().await? {
        let lvl: String = row.get(0)?;
        let cnt: i64 = row.get(1)?;
        parts.push(format!("{}: {}", lvl, cnt));
    }
    let breakdown = if parts.is_empty() { "None".to_string() } else { parts.join(", ") };
    Ok(format!("Logs in past {} minutes: {}\nBy level: {}", minutes, total, breakdown))
}

// Add helper function for get_metrics_text
async fn get_metrics_text(conn: &Connection, db_url: &str) -> Result<String, libsql::Error> {
    let ingested = LOGS_INGESTED.load(Ordering::SeqCst);
    let failed = INGESTION_FAILURES.load(Ordering::SeqCst);
    let db_size = if db_url == ":memory:" {
        0
    } else {
        fs::metadata(db_url).map(|m| m.len()).unwrap_or(0)
    };
    Ok(format!("Ingested: {}\nFailed: {}\nDB size: {} bytes", ingested, failed, db_size))
}

/// Admin UI: display log count, db size, health, and metrics
async fn admin_ui(data: web::Data<AppState>) -> impl Responder {
    // count logs
    let count: i64 = match data.conn.query("SELECT COUNT(*) FROM logs", ()).await {
        Ok(mut rows) => {
            if let Some(row) = rows.next().await.unwrap_or(None) {
                row.get(0).unwrap_or(0)
            } else { 0 }
        }
        Err(_) => 0,
    };
    // db size
    let db_size = if data.db_url == ":memory:" {
        0
    } else {
        fs::metadata(&data.db_url).map(|m| m.len()).unwrap_or(0)
    };
    // health status
    let health_status = if data.conn.execute("SELECT 1", ()).await.is_ok() { "OK" } else { "DB unreachable" };
    // metrics totals
    let ingested_total = LOGS_INGESTED.load(Ordering::SeqCst);
    let failed_total = INGESTION_FAILURES.load(Ordering::SeqCst);
    // render html
    let html = format!(r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Loggy Admin</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="container py-5">
    <h1>Loggy Admin Interface</h1>
    <p>Health: <strong>{health_status}</strong></p>
    <p>Number of logs: <strong>{count}</strong></p>
    <p>Metrics:</p>
    <ul>
      <li>Ingested total: {ingested_total}</li>
      <li>Failed ingestions: {failed_total}</li>
      <li>Database size: {db_size} bytes</li>
    </ul>
    <form method="post" action="/admin/clear">
      <button type="submit" class="btn btn-danger">Clear Logs</button>
    </form>
  </body>
</html>"#,
        health_status=health_status,
        count=count,
        ingested_total=ingested_total,
        failed_total=failed_total,
        db_size=db_size
    );
    HttpResponse::Ok().content_type("text/html; charset=utf-8").body(html)
}

/// Clear all logs and redirect to admin UI
async fn clear_logs(data: web::Data<AppState>) -> impl Responder {
    if let Err(e) = data.conn.execute_batch("DELETE FROM logs; DELETE FROM logs_fts;").await {
        error!("Error clearing logs: {:?}", e);
    }
    HttpResponse::SeeOther().append_header(("Location", "/")).finish()
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let config = Config::parse();
    tracing_subscriber::fmt::init();
    let conn = init_db(&config.db_url).await;
    let (broadcaster_tx, _) = channel(100);

    // Initialize known_services
    let mut initial_services = HashSet::new();
    match list_distinct_services(&conn, -1, 0).await { // Fetch all for initial population
        Ok(services) => {
            for service in services {
                initial_services.insert(service);
            }
            info!("Initialized known_services with {} existing services", initial_services.len());
        }
        Err(e) => {
            error!("Failed to query existing services for known_services: {:?}", e);
            // Continue with an empty set, notifications will be sent for all services initially.
        }
    }
    let known_services_mutex = Mutex::new(initial_services);

    let state = web::Data::new(AppState {
        conn,
        db_url: config.db_url.clone(),
        broadcaster: broadcaster_tx.clone(),
        port: config.port,
        known_services: known_services_mutex,
        mcp_log_level: Mutex::new("info".to_string()),
    });
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            // Admin interface
            .route("/", web::get().to(admin_ui))
            .route("/admin/clear", web::post().to(clear_logs))
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
