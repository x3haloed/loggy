use actix_web::{web, HttpRequest, HttpResponse, Responder};
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use hostname::get as get_hostname;
use prost::Message;
use serde::Deserialize;
use serde_json::{json, Value, Map};
use std::process;
use std::sync::atomic::Ordering;
use tracing::{error, info}; // Removed warn as it's not used in the provided ingest functions

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest as ProtoExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue as ProtoAnyValue, KeyValue as ProtoKeyValue};
use opentelemetry_proto::tonic::common::v1::any_value::Value as ProtoAnyValueKind;

use crate::AppState;
use crate::{LOGS_INGESTED, INGESTION_FAILURES};

// --- OTLP Ingestion types ---
#[derive(Deserialize, Debug)]
pub struct ExportLogsServiceRequest {
    #[serde(rename = "resourceLogs")]
    pub resource_logs: Vec<ResourceLogs>,
}

#[derive(Deserialize, Debug)]
pub struct ResourceLogs {
    #[serde(rename = "instrumentationLibraryLogs")]
    pub instrumentation_library_logs: Vec<InstrumentationLibraryLogs>,
}

#[derive(Deserialize, Debug)]
pub struct InstrumentationLibraryLogs {
    pub logs: Vec<LogRecord>,
}

#[derive(Deserialize, Debug)]
pub struct LogRecord {
    #[serde(rename = "timeUnixNano")]
    pub time_unix_nano: String,
    #[serde(rename = "severityText")]
    pub severity_text: String,
    #[serde(rename = "body")]
    pub body: AnyValue,
    #[serde(default, rename = "attributes")]
    pub attributes: Vec<KeyValue>,
}

#[derive(Deserialize, Debug)]
pub struct KeyValue {
    pub key: String,
    pub value: AnyValue,
}

#[derive(Deserialize, Debug)]
pub struct AnyValue {
    #[serde(rename = "stringValue")]
    pub string_value: Option<String>,
    #[serde(rename = "boolValue")]
    pub bool_value: Option<bool>,
    #[serde(rename = "intValue")]
    pub int_value: Option<i64>,
    #[serde(rename = "doubleValue")]
    pub double_value: Option<f64>,
}


pub async fn ingest_ndjson(
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
        let ts = match v.remove("timestamp").and_then(|v_val| v_val.as_str().map(|s| s.to_string())) {
            Some(val) => val,
            None => { INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst); return HttpResponse::BadRequest().json(json!({"error": format!("Missing or invalid timestamp at line {}", i+1)})); }
        };
        let level = match v.remove("level").and_then(|v_val| v_val.as_str().map(|s| s.to_string())) {
            Some(val) => val,
            None => { INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst); return HttpResponse::BadRequest().json(json!({"error": format!("Missing or invalid level at line {}", i+1)})); }
        };
        let service = match v.remove("service").and_then(|v_val| v_val.as_str().map(|s| s.to_string())) {
            Some(val) => val,
            None => { INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst); return HttpResponse::BadRequest().json(json!({"error": format!("Missing or invalid service at line {}", i+1)})); }
        };
        let service_name = service.clone(); 
        let message = match v.remove("message") {
            Some(val) => val,
            None => { INGESTION_FAILURES.fetch_add(1, Ordering::SeqCst); return HttpResponse::BadRequest().json(json!({"error": format!("Missing message at line {}", i+1)})); }
        };

        {
            let mut known_services_guard = data.known_services.lock().await;
            if known_services_guard.insert(service_name.clone()) {
                new_service_detected_in_batch = true;
                info!("New service '{}' detected during NDJSON ingestion", service_name);
            }
        } 

        v.insert("host_name".to_string(), serde_json::Value::String(hostname.clone()));
        v.insert("process_id".to_string(), serde_json::Value::Number(pid.into()));
        v.insert("message".to_string(), message.clone());
        let body_str = serde_json::Value::Object(v).to_string();
        if let Err(e) = tx.execute("INSERT INTO logs (ts, level, service, body) VALUES (?1, ?2, ?3, ?4)",
                                   (ts, level, service_name, body_str) 
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


pub async fn ingest_otlp(
    req: HttpRequest,
    body: Bytes,
    data: web::Data<AppState>,
) -> impl Responder {
    let hostname = get_hostname().unwrap_or_default().to_string_lossy().to_string();
    let pid = process::id();
    let tx = data.conn.transaction().await.unwrap();
    let mut count = 0;
    let mut new_service_detected_in_batch = false; 
    let ct = req.headers().get("content-type").and_then(|h| h.to_str().ok()).unwrap_or("");
    if ct.starts_with("application/x-protobuf") {
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
                for log_record in scope.log_records { // Renamed log to log_record to avoid conflict
                    let ts_nano: u128 = log_record.time_unix_nano as u128;
                    let secs = (ts_nano / 1_000_000_000) as i64;
                    let nsecs = (ts_nano % 1_000_000_000) as u32;
                    let naive = NaiveDateTime::from_timestamp_opt(secs, nsecs)
                        .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0));
                    let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);
                    let ts = datetime.to_rfc3339();
                    let level = log_record.severity_text.clone();
                    let mut service_name = "unknown".to_string();
                    for kv in &log_record.attributes {
                        if kv.key == "service.name" {
                            if let Some(ProtoAnyValue { value: Some(ProtoAnyValueKind::StringValue(s)) }) = &kv.value {
                                service_name = s.clone();
                            }
                        }
                    }

                    {
                        let mut known_services_guard = data.known_services.lock().await;
                        if known_services_guard.insert(service_name.clone()) {
                            new_service_detected_in_batch = true;
                            info!("New service '{}' detected during OTLP ingestion", service_name);
                        }
                    } 

                    let mut map = Map::new(); // Using serde_json::Map directly
                    for kv in &log_record.attributes {
                        let val = match &kv.value {
                            Some(ProtoAnyValue { value: Some(ProtoAnyValueKind::StringValue(s)) }) => Value::String(s.clone()),
                            Some(ProtoAnyValue { value: Some(ProtoAnyValueKind::BoolValue(b)) }) => Value::Bool(*b),
                            Some(ProtoAnyValue { value: Some(ProtoAnyValueKind::IntValue(i)) }) => Value::Number((*i).into()),
                            Some(ProtoAnyValue { value: Some(ProtoAnyValueKind::DoubleValue(d)) }) => serde_json::Number::from_f64(*d).map(Value::Number).unwrap_or(Value::Null),
                            _ => Value::Null,
                        };
                        map.insert(kv.key.clone(), val);
                    }
                    if let Some(ProtoAnyValue { value: Some(ProtoAnyValueKind::StringValue(msg)) }) = &log_record.body {
                        map.insert("message".to_string(), Value::String(msg.clone()));
                    }
                    map.insert("host_name".to_string(), Value::String(hostname.clone()));
                    map.insert("process_id".to_string(), Value::Number(pid.into()));
                    let body_str = Value::Object(map).to_string();
                    if let Err(e) = tx.execute(
                        "INSERT INTO logs (ts, level, service, body) VALUES (?1, ?2, ?3, ?4)",
                        (ts, level.clone(), service_name.clone(), body_str), 
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
