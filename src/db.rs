use libsql::{Builder, Connection};

// This file will contain database initialization and interaction logic.
pub async fn init_db(db_url: &str) -> Connection {
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
