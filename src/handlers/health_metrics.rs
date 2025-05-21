use actix_web::{web, HttpResponse, Responder};
use crate::AppState;
use crate::{LOGS_INGESTED, INGESTION_FAILURES}; // Atomics from main.rs
use std::fs;
use std::sync::atomic::Ordering;

/// Health check endpoint
pub async fn health(data: web::Data<AppState>) -> impl Responder {
    if data.conn.execute("SELECT 1", ()).await.is_ok() {
        HttpResponse::Ok().body("OK")
    } else {
        HttpResponse::InternalServerError().body("DB unreachable")
    }
}

/// Prometheus-style metrics endpoint
pub async fn metrics(data: web::Data<AppState>) -> impl Responder {
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
