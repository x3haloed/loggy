pub mod types;
pub mod tools;
pub mod resources;

use actix_web::{web, HttpRequest, HttpResponse, Responder};
use tokio_stream::StreamExt;
use bytes::Bytes;
// Sender is not directly used here as it's part of AppState, which is used by data.broadcaster
// use tokio::sync::broadcast::{Sender}; 
use tokio_stream::wrappers::{BroadcastStream, IntervalStream};
use std::time::Duration;
use tracing::{info, error, warn};
use crate::AppState; // AppState is in crate root (main.rs)

use serde_json::{json, Value};
use crate::mcp::types::JsonRpcRequest;
use std::collections::HashSet; // For VALID_LOG_LEVELS
use lazy_static::lazy_static; // For VALID_LOG_LEVELS
use std::fs; // For metrics.txt and config.toml size/content in resources/read (within mcp_post body)
use std::sync::atomic::Ordering; // For LOGS_INGESTED etc. in resources/read (within mcp_post body)
use crate::{LOGS_INGESTED, INGESTION_FAILURES}; // Atomics from main.rs
use crate::mcp::types::{INVALID_PARAMS, METHOD_NOT_FOUND, INTERNAL_ERROR}; // Error codes
// The helper functions like list_distinct_services, search_logs etc. are now in crate::mcp::tools
use crate::mcp::resources::{fetch_all_logs_text, fetch_service_logs_text}; // Added for resource helpers
use crate::mcp::tools::list_distinct_services; // For list_distinct_services used in resources/list
// Note: fetch_all_logs_text and fetch_service_logs_text are already imported at the top level of this file.

// Moved from main.rs
pub const MCP_PAGE_SIZE: usize = 10; // Made public

lazy_static! {
    pub static ref VALID_LOG_LEVELS: HashSet<String> = { // Made lazy_static effectively public
        vec!["debug", "info", "notice", "warning", "error", "critical", "alert", "emergency"]
            .into_iter().map(String::from).collect()
    };
}

pub async fn handle_mcp_sse_get_request(data: web::Data<AppState>, req: HttpRequest) -> impl Responder {
    // Original mcp_sse logic
    let conn_info = req.connection_info();
    let remote = conn_info.realip_remote_addr().unwrap_or("<unknown>");
    let origin_opt = req.headers().get("Origin").and_then(|h| h.to_str().ok());
    info!("MCP[SSE] connection attempt from {} with Origin {:?}", remote, origin_opt);
    
    let is_localhost = remote.starts_with("127.0.0.1") || 
                       remote.starts_with("::1") || 
                       remote.starts_with("localhost");
                       
    if req.headers().get("Origin").is_none() && !is_localhost {
        error!("MCP[SSE] connection rejected: missing Origin header from {}", remote);
        return HttpResponse::Forbidden().body("Missing Origin");
    }
    info!("MCP[SSE] connection established from {}", remote);
    let mut rx = data.broadcaster.subscribe();
    let init = tokio_stream::iter(vec![
        Ok::<Bytes, actix_web::Error>(Bytes::from_static(b"event: endpoint\ndata: /mcp/sse\n\n")),
    ]);
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
    let stream = init.merge(broadcast).merge(keep_alive);
    HttpResponse::Ok()
        .append_header(("Content-Type", "text/event-stream"))
        .append_header(("Cache-Control", "no-cache"))
        .streaming(stream)
}

pub async fn handle_mcp_post_request(body: String, data: web::Data<AppState>) -> impl Responder {
    // Original mcp_post logic
    info!("MCP[POST] raw body: {}", body);
    let req: JsonRpcRequest = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => { error!("MCP[POST] JSON parse error: {} -- body: {}", e, body); return HttpResponse::BadRequest().body("Invalid JSON-RPC"); }
    };
    info!("MCP[POST] JSON-RPC request: method={}, id={:?}, params={:?}", req.method, req.id, req.params);

    if req.id.is_none() {
        match req.method.as_str() {
            "notifications/initialized" => {
                info!("Received notifications/initialized");
            }
            "notifications/cancelled" => {
                let request_id_val = req.params.as_ref().and_then(|p| p.get("requestId")).cloned();
                let reason_val = req.params.as_ref()
                    .and_then(|p| p.get("reason"))
                    .and_then(Value::as_str)
                    .map(String::from);
                info!("Received notifications/cancelled for requestId: {:?}, reason: {:?}", request_id_val, reason_val);
            }
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
        return HttpResponse::Ok().finish();
    }

    let id = req.id.expect("req.id should be Some for requests, this is a bug in logic flow.");
    let mut response = json!({"jsonrpc": "2.0", "id": id.clone()});

    match req.method.as_str() {
        "initialize" => {
            response["result"] = json!({
                "protocolVersion": "2025-03-26",
                "capabilities": {
                    "tools": {"listChanged": false},
                    "logging": {},
                    "resources": {"subscribe": true, "listChanged": true},
                    "prompts": {"listChanged": false},
                    "completions": {}
                },
                "serverInfo": {"name": "loggy", "version": env!("CARGO_PKG_VERSION")},
                "instructions": ""
            });
        }
        "tools/list" => {
                // TODO: This tools list definition should ideally live in mcp::tools or be generated from there.
                // For now, keeping definition here for pagination logic.
                let all_tools_data = vec![
                     json!({
                        "name": "list_services",
                        "description": "List all service names that have logged entries",
                        "inputSchema": { "type": "object", "properties": {}, "required": [] },
                        "annotations": { "title": "List Services", "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false }
                    }),
                    json!({
                        "name": "search_logs",
                        "description": "Search logs by full-text message content",
                        "inputSchema": { "type": "object", "properties": { "q": {"type": "string"}, "limit": {"type": "integer"}, "offset": {"type": "integer"}}, "required": ["q"] },
                        "annotations": { "title": "Search Logs", "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false }
                    }),
                    json!({
                        "name": "get_log",
                        "description": "Retrieve a single log entry by ID",
                        "inputSchema": { "type": "object", "properties": {"id": {"type": "integer"}}, "required": ["id"] },
                        "annotations": { "title": "Get Log", "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false }
                    }),
                    json!({
                        "name": "tail_logs",
                        "description": "Stream the last N lines (optionally filtered by service & level)",
                        "inputSchema": {"type": "object", "properties": {"service": {"type": "string"}, "level": {"type": "string"}, "lines": {"type": "integer"}}, "required": []},
                        "annotations": { "title": "Tail Logs", "readOnlyHint": true, "destructiveHint": false, "idempotentHint": false, "openWorldHint": false }
                    }),
                    json!({
                        "name": "search_logs_around",
                        "description": "Fetch log entries before & after a given ID",
                        "inputSchema": {"type": "object", "properties": {"id": {"type": "integer"}, "before": {"type": "integer"}, "after": {"type": "integer"}}, "required": ["id"]},
                        "annotations": { "title": "Search Logs Around", "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false }
                    }),
                    json!({
                        "name": "summarize_logs",
                        "description": "Natural-language summary of logs over a time window",
                        "inputSchema": {"type": "object", "properties": {"service": {"type": "string"}, "level": {"type": "string"}, "minutes": {"type": "integer"}}, "required": ["minutes"]},
                        "annotations": { "title": "Summarize Logs", "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false }
                    }),
                    json!({
                        "name": "get_metrics",
                        "description": "Fetch current ingestion and DB metrics",
                        "inputSchema": {"type": "object", "properties": {}, "required": []},
                        "annotations": { "title": "Get Metrics", "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false }
                    }),
                ];
            let current_offset: usize = req.params.as_ref()
                .and_then(|p| p.get("cursor")).and_then(Value::as_str)
                .and_then(|s| s.parse().ok()).unwrap_or(0);
            let start_index = current_offset;
            let end_index = std::cmp::min(start_index + MCP_PAGE_SIZE, all_tools_data.len());
            let page_tools = if start_index < end_index { all_tools_data[start_index..end_index].to_vec() } else { vec![] };
            response["result"] = json!({"tools": page_tools});
            if end_index < all_tools_data.len() {
                response["result"]["nextCursor"] = json!(end_index.to_string());
            }
        }
        "tools/call" => {
            if let Some(params_val) = &req.params {
                    let tool_name_opt = params_val.get("name").and_then(Value::as_str);
                    let args_opt = params_val.get("arguments");

                    if let Some(name) = tool_name_opt {
                        match name {
                            "list_services" => {
                                match crate::mcp::tools::list_distinct_services(&data.conn).await {
                                    Ok(services) => {
                                        let text = format!("Available services: {}", services.join(", "));
                                        response["result"] = json!({"content": [{"type": "text", "text": text}]});
                                    },
                                    Err(e) => {
                                        error!("Error listing services: {:?}", e);
                                        response["error"] = json!({"code": INTERNAL_ERROR, "message": "Database error listing services"});
                                    }
                                }
                            },
                            "search_logs" => {
                                if let Some(args_obj) = args_opt {
                                    if let Some(q_val) = args_obj.get("q") {
                                        if let Some(q_str) = q_val.as_str() {
                                            if q_str.is_empty() {
                                                response["result"] = json!({"content": [{"type": "text", "text": "Error: Missing or empty 'q' parameter for tool search_logs"}], "isError": true});
                                            } else {
                                                let limit = args_obj.get("limit").and_then(Value::as_u64).unwrap_or(10);
                                                let offset = args_obj.get("offset").and_then(Value::as_u64).unwrap_or(0);
                                                match crate::mcp::tools::search_logs(&data.conn, q_str, limit as i64, offset as i64).await {
                                                    Ok(results) => response["result"] = json!({"content": [{"type": "text", "text": results}]}),
                                                    Err(e) => {
                                                        error!("Error searching logs: {:?}", e);
                                                        response["error"] = json!({"code": INTERNAL_ERROR, "message": "Database error searching logs"});
                                                    }
                                                }
                                            }
                                        } else {
                                            response["result"] = json!({"content": [{"type": "text", "text": "Error: Invalid 'q' parameter, must be a string for tool search_logs"}], "isError": true});
                                        }
                                    } else {
                                         response["result"] = json!({"content": [{"type": "text", "text": "Error: Missing 'q' parameter for tool search_logs"}], "isError": true});
                                    }
                                } else {
                                    response["result"] = json!({"content": [{"type": "text", "text": "Error: Missing arguments for tool search_logs"}], "isError": true });
                                }
                            },
                             "get_log" => {
                                if let Some(args_obj) = args_opt {
                                    if let Some(id_val) = args_obj.get("id") {
                                        if let Some(id_u64) = id_val.as_u64() {
                                            match crate::mcp::tools::get_log_by_id(&data.conn, id_u64 as i64).await {
                                                Ok(log_entry) => response["result"] = json!({"content": [{"type": "text", "text": log_entry}]}),
                                                Err(e) => {
                                                    error!("Error getting log: {:?}", e);
                                                    response["error"] = json!({"code": INTERNAL_ERROR, "message": "Database error getting log"});
                                                }
                                            }
                                        } else {
                                            response["result"] = json!({"content": [{"type": "text", "text": "Error: Invalid 'id' parameter for get_log"}], "isError": true});
                                        }
                                    } else {
                                        response["result"] = json!({"content": [{"type": "text", "text": "Error: Missing 'id' parameter for get_log"}], "isError": true});
                                    }
                                } else {
                                    response["result"] = json!({"content": [{"type": "text", "text": "Error: Missing arguments for get_log"}], "isError": true});
                                }
                            },
                            "tail_logs" => {
                                if let Some(args_obj) = args_opt {
                                    let lines_n = args_obj.get("lines").and_then(Value::as_u64).unwrap_or(10) as i64;
                                    let service_opt = args_obj.get("service").and_then(Value::as_str);
                                    let level_opt = args_obj.get("level").and_then(Value::as_str);
                                    match crate::mcp::tools::tail_logs(&data.conn, service_opt, level_opt, lines_n).await {
                                        Ok(text) => response["result"] = json!({"content": [{"type": "text", "text": text}]}),
                                        Err(e) => {
                                            error!("Error tailing logs: {:?}", e);
                                            response["error"] = json!({"code": INTERNAL_ERROR, "message": "Database error tailing logs"});
                                        }
                                    }
                                } else { // Should not happen if arguments is an empty object by default from client
                                     response["result"] = json!({"content": [{"type": "text", "text": "Error: Missing arguments object for tail_logs"}], "isError": true});
                                }
                            },
                            "search_logs_around" => {
                                if let Some(args_obj) = args_opt {
                                    if let Some(id_val) = args_obj.get("id") {
                                        if let Some(id_u64) = id_val.as_u64() {
                                            let before_n = args_obj.get("before").and_then(Value::as_u64).unwrap_or(0) as i64;
                                            let after_n = args_obj.get("after").and_then(Value::as_u64).unwrap_or(0) as i64;
                                            match crate::mcp::tools::search_logs_around(&data.conn, id_u64 as i64, before_n, after_n).await {
                                                Ok(text) => response["result"] = json!({"content": [{"type": "text", "text": text}]}),
                                                Err(e) => {
                                                    error!("Error searching logs around: {:?}", e);
                                                    response["error"] = json!({"code": INTERNAL_ERROR, "message": "Database error searching logs around"});
                                                }
                                            }
                                        } else {
                                            response["result"] = json!({"content": [{"type": "text", "text": "Error: Invalid 'id' parameter for search_logs_around"}], "isError": true});
                                        }
                                    } else {
                                         response["result"] = json!({"content": [{"type": "text", "text": "Error: Missing 'id' parameter for search_logs_around"}], "isError": true});
                                    }
                                } else {
                                    response["result"] = json!({"content": [{"type": "text", "text": "Error: Missing arguments for search_logs_around"}], "isError": true});
                                }
                            },
                            "summarize_logs" => {
                                if let Some(args_obj) = args_opt {
                                    if let Some(minutes_val) = args_obj.get("minutes") {
                                        if let Some(minutes_u64) = minutes_val.as_u64() {
                                            let service_opt = args_obj.get("service").and_then(Value::as_str);
                                            let level_opt = args_obj.get("level").and_then(Value::as_str);
                                            match crate::mcp::tools::summarize_logs(&data.conn, service_opt, level_opt, minutes_u64 as i64).await {
                                                Ok(text) => response["result"] = json!({"content": [{"type": "text", "text": text}]}),
                                                Err(e) => {
                                                    error!("Error summarizing logs: {:?}", e);
                                                    response["error"] = json!({"code": INTERNAL_ERROR, "message": "Database error summarizing logs"});
                                                }
                                            }
                                        } else {
                                            response["result"] = json!({"content": [{"type": "text", "text": "Error: Invalid 'minutes' parameter for summarize_logs"}], "isError": true});
                                        }
                                    } else {
                                        response["result"] = json!({"content": [{"type": "text", "text": "Error: Missing 'minutes' parameter for summarize_logs"}], "isError": true});
                                    }
                                } else {
                                     response["result"] = json!({"content": [{"type": "text", "text": "Error: Missing arguments for summarize_logs"}], "isError": true});
                                }
                            },
                            "get_metrics" => {
                                match crate::mcp::tools::get_metrics_text(&data.conn, &data.db_url).await {
                                    Ok(text) => response["result"] = json!({"content": [{"type": "text", "text": text}]}),
                                    Err(e) => {
                                        error!("Error getting metrics: {:?}", e);
                                        response["error"] = json!({"code": INTERNAL_ERROR, "message": "Database error getting metrics"});
                                    }
                                }
                            },
                            _ => {
                                 response["result"] = json!({"content": [{"type": "text", "text": format!("Error: Tool not found: {}", name)}], "isError": true});
                            }
                        }
                } else {
                     response["result"] = json!({"content": [{"type": "text", "text": "Error: Missing tool name"}], "isError": true});
                }
            } else {
                response["result"] = json!({"content": [{"type": "text", "text": "Error: Missing params for tools/call"}], "isError": true});
            }
        }
        "resources/list" => {
                // This logic uses fetch_all_logs_text, fetch_service_logs_text (already imported)
                // and crate::mcp::tools::list_distinct_services (via `use crate::mcp::tools;`)
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
                if let Some(p_params) = &req.params { // Renamed to avoid conflict
                    if let Some(uri) = p_params.get("uri").and_then(Value::as_str) {
                        let mut content = json!({"uri": uri, "mimeType": "text/plain", "text": ""});
                        if uri.ends_with("all.log") {
                            if let Ok(text) = fetch_all_logs_text(&data.conn).await { // Corrected path
                                content["text"] = json!(text);
                            }
                        } else if uri.contains("/logs/") && uri.ends_with(".log") {
                            let svc = uri.trim_start_matches("file:///loggy/logs/").trim_end_matches(".log");
                            if let Ok(text) = fetch_service_logs_text(&data.conn, svc).await { // Corrected path
                                content["text"] = json!(text);
                            }
                        } else if uri.ends_with("config.toml") {
                            let toml_content = format!("port = {}\ndb_url = \"{}\"\n", data.port, data.db_url);
                            content["text"] = json!(toml_content);
                        } else if uri.ends_with("metrics.txt") {
                            let ing = LOGS_INGESTED.load(Ordering::SeqCst);
                            let f = INGESTION_FAILURES.load(Ordering::SeqCst);
                            let db_sz = if data.db_url == ":memory:" { 0 } else { fs::metadata(&data.db_url).map(|m| m.len()).unwrap_or(0) };
                            let metrics_body = format!("# HELP loggy_ingested_total...\nloggy_ingested_total {ingested}\n# HELP loggy_failed_ingestions_total...\nloggy_failed_ingestions_total {failed}\n# HELP loggy_db_size_bytes...\nloggy_db_size_bytes {db_size}\n", ingested=ing, failed=f, db_size=db_sz);
                            content["text"] = json!(metrics_body);
                        }
                        response["result"] = json!({"contents": [content]});
                    } else {
                        response["error"] = json!({"code": INVALID_PARAMS, "message": "Missing uri param for resources/read"});
                    }
                } else {
                    response["error"] = json!({"code": INVALID_PARAMS, "message": "Missing params for resources/read"});
                }
        }
        "resources/templates/list" => {
                // This logic was already paginated and doesn't depend on the moved helpers.
                // No change needed to the body of this handler.
                let all_templates_data = vec![
                    json!({
                        "uriTemplate": "logquery:///{service}/{level}/{startTs}/{endTs}",
                        "name": "Time Range Logs",
                        "description": "Select logs for a given service & level between two ISO-8601 timestamps",
                        "mimeType": "application/json",
                        "annotations": { "audience": ["assistant"], "priority": 0.5 }
                    }),
                    json!({
                        "uriTemplate": "logentry:///{id}?before={n}&after={m}",
                        "name": "Entry Drilldown",
                        "description": "Fetch N entries before and M entries after a log ID",
                        "mimeType": "application/json",
                        "annotations": { "audience": ["assistant"], "priority": 0.5 }
                    }),
                    json!({
                        "uriTemplate": "file:///project/src/{path}",
                        "name": "Source Code Files",
                        "description": "Fetch source code snippets by path",
                        "mimeType": "text/x-rust",
                        "annotations": { "audience": ["assistant"], "priority": 0.5 }
                    }),
                ];
                 let current_offset: usize = req.params.as_ref()
                    .and_then(|p| p.get("cursor"))
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                let start_index = current_offset;
                let end_index = std::cmp::min(start_index + MCP_PAGE_SIZE, all_templates_data.len());
                let page_templates = if start_index < end_index { all_templates_data[start_index..end_index].to_vec() } else { vec![] };
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
                        response["error"] = json!({"code": INVALID_PARAMS, "message": format!("Invalid 'level' value")});
                    }
                }
                None => {
                    response["error"] = json!({"code": INVALID_PARAMS, "message": "Missing 'level' parameter"});
                }
            }
        }
        "completion/complete" => {
            // This will be moved to mcp module, perhaps mcp::completions
            response["result"] = json!({"completion": {"values": [], "total": 0, "hasMore": false}}); // Placeholder
        }
        "resources/subscribe" => {
            if let Some(p_val) = &req.params {
                if let Some(uri_val) = p_val.get("uri").and_then(Value::as_str) {
                    response["result"] = json!({});
                    let notif = json!({"jsonrpc": "2.0", "method": "notifications/resources/updated", "params": {"uri": uri_val}});
                    let _ = data.broadcaster.send(notif.to_string());
                } else {
                    response["error"] = json!({"code": INVALID_PARAMS, "message": "Missing uri param for subscribe"});
                }
            } else {
                response["error"] = json!({"code": INVALID_PARAMS, "message": "Missing params for subscribe"});
            }
        }
        _ => {
            response["error"] = json!({"code": METHOD_NOT_FOUND, "message": "Method not found"});
        }
    }
    HttpResponse::Ok().json(response)
}
