use actix_web::{web, HttpResponse, Responder};
use crate::AppState;
use crate::{LOGS_INGESTED, INGESTION_FAILURES};
use std::fs;
use std::sync::atomic::Ordering;
use tracing::error; // For logging errors in clear_logs
// Note: libsql::Error is not directly used here as errors are handled via unwrap_or or Result ignored.
// AppState.conn (libsql::Connection) methods are used directly.

/// Admin UI: display log count, db size, health, and metrics
pub async fn admin_ui(data: web::Data<AppState>) -> impl Responder {
    // count logs
    let count: i64 = match data.conn.query("SELECT COUNT(*) FROM logs", ()).await {
        Ok(mut rows) => {
            if let Some(row_result) = rows.next().await {
                match row_result {
                    Ok(row) => row.get(0).unwrap_or(0),
                    Err(_) => 0, // Error fetching row
                }
            } else { 0 } // No rows
        }
        Err(_) => 0, // Error executing query
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
pub async fn clear_logs(data: web::Data<AppState>) -> impl Responder {
    if let Err(e) = data.conn.execute_batch("DELETE FROM logs; DELETE FROM logs_fts;").await {
        error!("Error clearing logs: {:?}", e);
    }
    HttpResponse::SeeOther().append_header(("Location", "/")).finish()
}
