use libsql::{Connection, Error as LibsqlError, params};
use chrono::{Utc, Duration as ChronoDuration}; // Renamed to avoid conflict if std::time::Duration is also used
use crate::{LOGS_INGESTED, INGESTION_FAILURES}; // Atomics from main.rs
use std::fs;
use std::sync::atomic::Ordering;

use crate::mcp::resources::fetch_all_logs_text; // Using the moved function


pub async fn list_distinct_services(conn: &Connection, limit: i64, offset: i64) -> Result<Vec<String>, LibsqlError> {
    // Fetch distinct services, optionally paginated by limit/offset
    let base_sql = "SELECT DISTINCT service FROM logs ORDER BY service";
    let mut rows = if limit < 0 {
        conn.query(base_sql, ()).await?
    } else {
        let paged_sql = format!("{} LIMIT ? OFFSET ?", base_sql);
        conn.query(&paged_sql, params![limit, offset]).await?
    };
    let mut services = Vec::new();
    while let Some(row) = rows.next().await? {
        if let Ok(service) = row.get::<String>(0) {
            services.push(service);
        }
    }
    Ok(services)
}

pub async fn search_logs(conn: &Connection, query: &str, limit: i64, offset: i64) -> Result<String, LibsqlError> {
    let pattern = format!("%{}%", query);
    let sql = "SELECT json_group_array(json_object('id', id, 'ts', ts, 'level', level, 'service', service, 'body', body)) 
               FROM logs 
               WHERE json_extract(body, '$.message') LIKE ?1 
               LIMIT ?2 OFFSET ?3";
    
    let mut rows = conn.query(sql, params![pattern, limit, offset]).await?;
    
    match rows.next().await? {
        Some(row) => Ok(row.get(0).unwrap_or_else(|_| "[]".to_string())),
        None => Ok("[]".to_string()),
    }
}

pub async fn get_log_by_id(conn: &Connection, id: i64) -> Result<String, LibsqlError> {
    let sql = "SELECT json_object('id', id, 'ts', ts, 'level', level, 'service', service, 'body', body) 
               FROM logs 
               WHERE id = ?1";
    
    let mut rows = conn.query(sql, params![id]).await?;
    
    match rows.next().await? {
        Some(row) => Ok(row.get(0).unwrap_or_else(|_| "{}".to_string())),
        None => Ok("{}".to_string()), // Or perhaps an error like NotFound
    }
}

pub async fn tail_logs(conn: &Connection, _service: Option<&str>, _level: Option<&str>, lines: i64) -> Result<String, LibsqlError> {
    // TODO: Filter by _service and _level if provided. Currently ignored.
    let all_text = fetch_all_logs_text(conn).await?;
    let lines_vec: Vec<&str> = all_text.lines().collect();
    let start_idx = if (lines as usize) < lines_vec.len() { lines_vec.len() - (lines as usize) } else { 0 };
    let selected_lines = &lines_vec[start_idx..];
    Ok(selected_lines.join("\n"))
}

pub async fn search_logs_around(conn: &Connection, id: i64, before: i64, after: i64) -> Result<String, LibsqlError> {
    let mut entries = Vec::new();

    // Fetch entries before the ID
    if before > 0 {
        let sql_before = format!(
            "SELECT json_object('id', id, 'ts', ts, 'level', level, 'service', service, 'body', body) FROM logs WHERE id < {} ORDER BY id DESC LIMIT {}",
            id, before
        );
        let mut rows_before = conn.query(&sql_before, ()).await?;
        let mut before_entries = Vec::new();
        while let Some(row) = rows_before.next().await? {
            let json: String = row.get(0)?;
            before_entries.push(json);
        }
        before_entries.reverse(); // Correct order
        entries.extend(before_entries);
    }

    // Center entry
    if let Ok(center_json) = get_log_by_id(conn, id).await {
        if center_json != "{}" { // Assuming "{}" means not found from get_log_by_id
            entries.push(center_json);
        }
    }
    
    // Entries after the ID
    if after > 0 {
        let sql_after = format!(
            "SELECT json_object('id', id, 'ts', ts, 'level', level, 'service', service, 'body', body) FROM logs WHERE id > {} ORDER BY id ASC LIMIT {}",
            id, after
        );
        let mut rows_after = conn.query(&sql_after, ()).await?;
        while let Some(row) = rows_after.next().await? {
            let json: String = row.get(0)?;
            entries.push(json);
        }
    }
    Ok(format!("[{}]", entries.join(",")))
}

pub async fn summarize_logs(conn: &Connection, service: Option<&str>, level: Option<&str>, minutes: i64) -> Result<String, LibsqlError> {
    let since_str = (Utc::now() - ChronoDuration::minutes(minutes)).to_rfc3339();
    let mut conditions = vec!["ts >= ?".to_string()];
    let mut args = vec![since_str.clone()];
    if let Some(s) = service {
        conditions.push("service = ?".to_string());
        args.push(s.to_string());
    }
    if let Some(l) = level {
        conditions.push("level = ?".to_string());
        args.push(l.to_string());
    }
    let where_clause = if conditions.is_empty() {
        "".to_string()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };
    let sql_total = format!("SELECT COUNT(*) FROM logs {}", where_clause);
    // Execute COUNT query with the appropriate parameters
    let mut total_rows = if args.len() == 1 {
        conn.query(&sql_total, params![args[0].clone()]).await?
    } else if args.len() == 2 {
        conn.query(&sql_total, params![args[0].clone(), args[1].clone()]).await?
    } else if args.len() == 3 {
        conn.query(&sql_total, params![args[0].clone(), args[1].clone(), args[2].clone()]).await?
    } else {
        conn.query(&sql_total, ()).await?
    };
    let total = match total_rows.next().await? {
        Some(row) => row.get::<i64>(0).unwrap_or(0),
        None => 0,
    };
    let sql_breakdown = format!("SELECT level, COUNT(*) FROM logs {} GROUP BY level", where_clause);
    // Execute breakdown query with the same parameters
    let mut breakdown_rows = if args.len() == 1 {
        conn.query(&sql_breakdown, params![args[0].clone()]).await?
    } else if args.len() == 2 {
        conn.query(&sql_breakdown, params![args[0].clone(), args[1].clone()]).await?
    } else if args.len() == 3 {
        conn.query(&sql_breakdown, params![args[0].clone(), args[1].clone(), args[2].clone()]).await?
    } else {
        conn.query(&sql_breakdown, ()).await?
    };
    let mut breakdown_parts = Vec::new();
    while let Some(row) = breakdown_rows.next().await? {
        let lvl: String = row.get(0)?;
        let cnt: i64 = row.get(1)?;
        breakdown_parts.push(format!("{}: {}", lvl, cnt));
    }
    let breakdown_str = if breakdown_parts.is_empty() {
        "None".to_string()
    } else {
        breakdown_parts.join(", ")
    };
    Ok(format!(
        "Logs in past {} minutes: {}\nBy level: {}",
        minutes, total, breakdown_str
    ))
}

pub async fn get_metrics_text(_conn: &Connection, db_url: &str) -> Result<String, LibsqlError> {
    // _conn is not used, but kept for signature consistency if it might be used later.
    let ingested = LOGS_INGESTED.load(Ordering::SeqCst);
    let failed = INGESTION_FAILURES.load(Ordering::SeqCst);
    let db_size = if db_url == ":memory:" {
        0
    } else {
        fs::metadata(db_url).map(|m| m.len()).unwrap_or(0)
    };
    Ok(format!("Ingested: {}\nFailed: {}\nDB size: {} bytes", ingested, failed, db_size))
}
