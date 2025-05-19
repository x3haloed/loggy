# loggy

`loggy` is a Rust-based MCP (Model Context Protocol) server for ingesting, storing, and querying application logs locally via an embedded libSQL database.

Features
--------
- HTTP ingestion of newline-delimited JSON (NDJSON) (`/logs/ndjson`)
- OpenTelemetry Logs (OTLP) ingestion over HTTP (`/v1/logs`)
- JSON-RPC 2.0 over SSE (`/mcp/sse`) for tool discovery and invocation
- HTTP RPC endpoints for quick tool calls (`/tools/list`, `/tools/call`)
- Prometheus-style health and metrics endpoints (`/healthz`, `/metrics`)
- Full-text search with FTS5, plus structured filters by timestamp, level, service
- Metadata enrichment: host name, process ID, plus arbitrary JSON attributes

Getting Started
---------------

### Prerequisites

- Rust (1.70+)
- (no external DB; uses embedded SQLite via libSQL)

### Building

```bash
git clone https://github.com/yourorg/loggy.git
cd loggy
cargo build --release
```

### Running

```bash
# in-memory mode (ephemeral)
cargo run --release -- --port 8080 --db-url ":memory:"

# file-backed mode (persistent)
cargo run --release -- --port 8080 --db-url "./loggy.db"
```

- `--port`  — HTTP listening port (default `8080`)
- `--db-url`— libSQL URL (`:memory:` or `file:///path/to.db`)

API Endpoints
-------------

### Ingestion

##### POST /logs/ndjson
- **Content-Type**: `application/x-ndjson`
- **Body**: newline-delimited JSON objects with at least:
  - `timestamp` (ISO 8601 string)
  - `level` (string)
  - `service` (string)
  - `message` (string)
  - plus any additional key/value attributes

##### POST /v1/logs
- **Content-Type**: `application/json`
- **Body**: OpenTelemetry Logs HTTP JSON payload (OTLP)

### Tools (HTTP RPC)

##### POST /tools/list
List available tools and their input schemas.

##### POST /tools/call
Invoke a named tool; payload: `{ name: string, arguments?: object }`.

### MCP JSON-RPC over SSE

1. **GET /mcp/sse** — establish an SSE connection; server streams JSON-RPC responses as SSE "message" events.
2. **POST /mcp/sse** — send a JSON-RPC 2.0 request:
   ```json
   {
     "jsonrpc": "2.0",
     "id": 1,
     "method": "search_logs",
     "params": { "q": "error", "limit": 10 }
   }
   ```
   Responses arrive on the SSE stream as:  
   ```text
   event: message
   data: { "jsonrpc":"2.0", "id":1, "result": [...] }
   ```

#### Available MCP Tools

- **list_services**  — returns an array of distinct service names in the logs.
- **search_logs**    — full-text search on the `message` field.
  - **params**: `q` (string, required), `limit` (integer), `offset` (integer)
- **get_log**        — retrieve a single log by its `id`.
  - **params**: `id` (integer, required)

### Health & Metrics

##### GET /healthz
Returns `200 OK` with body `OK` if the database is reachable; `500` otherwise.

##### GET /metrics
Prometheus-style metrics:

```
# HELP loggy_ingested_total Total number of log records ingested
# TYPE loggy_ingested_total counter
loggy_ingested_total 123
# HELP loggy_failed_ingestions_total Total number of failed ingestion requests
# TYPE loggy_failed_ingestions_total counter
loggy_failed_ingestions_total 4
# HELP loggy_db_size_bytes Current size of the DB file in bytes
# TYPE loggy_db_size_bytes gauge
loggy_db_size_bytes 1048576
```

Examples
--------

###### NDJSON Ingestion

```bash
curl -X POST http://localhost:8080/logs/ndjson \
     -H "Content-Type: application/x-ndjson" \
     --data-binary $'
{"timestamp":"2025-05-19T12:00:00Z","level":"INFO","service":"auth","message":"User login successful"}\n'
```

###### OTLP Ingestion

```bash
curl -X POST http://localhost:8080/v1/logs \
     -H "Content-Type: application/json" \
     -d '@payload_otlp.json'
```

###### MCP JSON-RPC (search_logs)

```bash
# In one terminal: listen for SSE
curl -N http://localhost:8080/mcp/sse

# In another terminal: send a JSON-RPC request
curl -X POST http://localhost:8080/mcp/sse \
     -H "Content-Type: application/json" \
     -d '{"jsonrpc":"2.0","id":1,"method":"search_logs","params":{"q":"error"}}'
```

Configuration & Environment
---------------------------

| Option   | Env var        | Default    | Description                           |
|----------|----------------|------------|---------------------------------------|
| port     | LOGGY_PORT     | `8080`     | HTTP server listen port               |
| db_url   | LOGGY_DB_URL   | `:memory:` | libSQL connection URL                 |

License
-------

[MIT](LICENSE)

Contributing
------------

Contributions, issues, and feature requests are welcome! Feel free to open a PR or an issue on GitHub. 