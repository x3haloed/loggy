use libsql::{Connection, Error as LibsqlError, params}; // Removed Rows as it's not directly used by these functions

// Helper to read all logs as plain text
pub async fn fetch_all_logs_text(conn: &Connection) -> Result<String, LibsqlError> {
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
pub async fn fetch_service_logs_text(conn: &Connection, service: &str) -> Result<String, LibsqlError> {
    let mut rows = conn.query(
        "SELECT ts || ' [' || level || '] ' || json_extract(body, '$.message') as line FROM logs WHERE service = ?1 ORDER BY id",
        params![service]
    ).await?;
    let mut buf = String::new();
    while let Some(row) = rows.next().await? {
        let line: String = row.get(0)?;
        buf.push_str(&line);
        buf.push('\n');
    }
    Ok(buf)
}
