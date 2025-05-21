use actix_web::{web, App, HttpServer};
use libsql::{Connection};
use clap::Parser;
use tracing::{info, error};
use tracing_subscriber;
use std::sync::atomic::{AtomicU64};
use tokio::sync::broadcast::{channel, Sender};
use tokio::sync::Mutex;
use std::collections::HashSet;

mod db;
mod handlers;
mod mcp;
use crate::db::init_db;
use crate::handlers::admin::{admin_ui, clear_logs};
use crate::handlers::ingestion::{ingest_ndjson, ingest_otlp};
use crate::handlers::health_metrics::{health, metrics};
use crate::mcp::{handle_mcp_sse_get_request, handle_mcp_post_request};
use crate::mcp::tools::list_distinct_services; // For known_services initialization

// Metrics counters
pub static LOGS_INGESTED: AtomicU64 = AtomicU64::new(0);
pub static INGESTION_FAILURES: AtomicU64 = AtomicU64::new(0); // Made public

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Config {
    #[arg(long, default_value = "8080")]
    port: u16,
    #[arg(long, default_value = ":memory:")]
    db_url: String,
}

// Removed init_db function from here

pub struct AppState { // Made AppState public
    conn: Connection,
    db_url: String,
    broadcaster: Sender<String>,
    port: u16,
    known_services: Mutex<HashSet<String>>,
    mcp_log_level: Mutex<String>,
}

// lazy_static! block for VALID_LOG_LEVELS removed
// const INVALID_PARAMS removed

// JsonRpcRequest struct removed
// mcp_sse function removed
// mcp_post function removed

// All helper functions (list_distinct_services, search_logs, get_log_by_id, 
// fetch_all_logs_text, fetch_service_logs_text, tail_logs, search_logs_around, 
// summarize_logs, get_metrics_text) are now removed from main.rs.
// They are expected to be in src/mcp/tools.rs or src/mcp/resources.rs.

// admin_ui and clear_logs functions removed from here

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
            .route("/mcp/sse", web::get().to(handle_mcp_sse_get_request))
            .route("/mcp/sse", web::post().to(handle_mcp_post_request))
            .route("/healthz", web::get().to(health))
            .route("/metrics", web::get().to(metrics))
    })
    .bind(("0.0.0.0", config.port))?
    .run()
    .await
}
