# Loggy

**An MCP server for collecting, storing, and querying application logs**

Loggy is a self-contained Rust binary that accepts verbose application logs over HTTP (NDJSON & OTLP), persists them in an embeddable libSQL (SQLite) database, and exposes MCP-compatible tools plus health/metrics endpoints for LLM-powered debugging.

---

## Features

- **NDJSON ingestion**: HTTP POST `/logs/ndjson` (Content-Type: application/x-ndjson)
- **OTLP ingestion**: HTTP POST `/v1/logs` (OpenTelemetry JSON payloads)
- **MCP tools API**: `POST /tools/list` and `POST /tools/call` for LLMs to discover and surface logs
- **Health & Metrics**: `GET /healthz` for liveness, `GET /metrics` for Prometheus metrics
- **Full-text & structured search**: backed by FTS5 and secondary indices on timestamp, level, and service
- **Configurable storage**: in-memory (`:memory:`) or file-based database
- **Enrichment**: automatically captures host name and process ID on ingest
- **Cross-platform**: works on macOS, Linux, and Windows

---

## Getting Started

### Prerequisites

- Rust toolchain (1.70+)
- `cargo` build system

### Building

```bash
git clone https://github.com/your-org/loggy.git
cd loggy
cargo build --release
```

### Running

```bash
# In-memory database on port 8080
./target/release/loggy --port 8080 --db-url ":memory:"

# File-based database
./target/release/loggy --port 9090 --db-url "/path/to/loggy.db"
```

#### Command-line Flags

| Flag      | Description                              | Default    |
|-----------|------------------------------------------|------------|
| `--port`  | HTTP port to listen on                   | `8080`     |
| `--db-url`| libSQL database URL (e.g. file path or `:memory:`) | `:memory:` |

---

## HTTP Endpoints

### 1. Log Ingestion

- **NDJSON**: `POST /logs/ndjson`
  - Body: newline-delimited JSON objects
  - Success: `{ "ingested": <count> }`

- **OTLP**: `POST /v1/logs`
  - Body: OpenTelemetry JSON payload
  - Success: `{ "ingested": <count> }`

### 2. Health & Metrics

- **Health**: `GET /healthz` → `200 OK` or `500 DB unreachable`
- **Metrics**: `GET /metrics` → Prometheus text format

Example:
```bash
curl http://localhost:8080/metrics
```

### 3. MCP Tools Discovery & Invocation

- **List Tools**: `POST /tools/list`
  ```json
  { "tools": [ {"name":"list_services", ...}, {"name":"search_logs", ...}, {"name":"get_log", ...} ] }
  ```

- **Call Tool**: `POST /tools/call`
  ```bash
  curl http://localhost:8080/tools/call \
       -H 'Content-Type: application/json' \
       -d '{ "name":"search_logs", "arguments": {"q":"error", "limit":5 } }'
  ```
  ```json
  { "content": [ {"type":"text","text":"[{...log JSON...}]"} ] }
  ```

---

## Database Storage & Migrations

- Default schema initialized at startup with:
  - `logs(id INTEGER PRIMARY KEY, ts TEXT, level TEXT, service TEXT, body JSON)`
  - FTS5 virtual table on `message`
  - Indices on `ts`, `level`, and `service`
- Configure migrations in `src/migrations` if using `libsql-migrate`

---

## Contributing

1. Fork the repo & create a feature branch
2. Run `cargo fmt`, `cargo clippy`, and `cargo test`
3. Open a pull request

---

## License

MIT