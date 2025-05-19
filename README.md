# Loggy

A fast, embeddable MCP log server for local development, LLM debugging, and app observability. Accepts logs via NDJSON and OTLP, stores in libSQL, and exposes a full MCP (Model Context Protocol) server for search and tool integration.

## Features
- Ingest logs via NDJSON or OTLP (HTTP)
- Store logs in a local libSQL database (file or in-memory)
- Full-text and structured search
- MCP server with SSE endpoint for Cursor and other LLM tools
- Cross-platform: macOS, Windows, Linux (x86_64 and arm64)

---

## Building

You need Rust (https://rustup.rs/) and a recent toolchain.

```sh
cargo build --release
```

The binary will be at `target/release/loggy` (or `.exe` on Windows).

---

## Publishing for Multiple Platforms

Use the provided `publish.sh` script to build and package for multiple platforms and architectures:

```sh
./publish.sh macos           # Build for macOS (x86_64 and arm64)
./publish.sh windows         # Build for Windows (x86_64 and arm64)
./publish.sh linux           # Build for Linux (x86_64 and arm64)
./publish.sh macos windows   # Build for both macOS and Windows
```

Artifacts and checksums will be in `target/publish/`.

---

## Running

```sh
./target/release/loggy --port 8080 --db-url ./loggy.db
```

- `--port` (default: 8080): HTTP port to listen on
- `--db-url` (default: :memory:): Path to libSQL database file, or `:memory:` for in-memory

---

## MCP Integration with Cursor

Loggy implements the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) and can be used as a local MCP server for Cursor and other LLM tools.

### 1. Add an `mcp.json` to your project root:

```
{
  "name": "Loggy Local Logs",
  "description": "Local log search and ingestion via Loggy MCP server",
  "endpoint": "http://localhost:8080/mcp/sse",
  "transport": "sse",
  "capabilities": ["tools", "logging", "resources", "prompts"],
  "version": "2025-03-26"
}
```

- `endpoint` should match the port you run loggy on
- `transport` must be `sse`
- `version` should match the protocol version supported by loggy

### 2. Start Loggy

```sh
./target/release/loggy --port 8080 --db-url ./loggy.db
```

### 3. In Cursor

- Open the Command Palette and search for "Connect MCP Server"
- Select your `mcp.json` file
- You should see Loggy's tools (list_services, search_logs, get_log) available in the Cursor UI

---

## Example: Ingesting Logs

### NDJSON

```sh
curl -X POST --data-binary @logs.ndjson \
     -H 'Content-Type: application/x-ndjson' \
     http://localhost:8080/logs/ndjson
```

### OTLP (OpenTelemetry Logs)

```sh
curl -X POST -H 'Content-Type: application/json' \
     --data @otlp.json \
     http://localhost:8080/v1/logs
```

## Integrating Loggy with Serilog in ASP.NET Core

## 1. Prerequisites  
- Have Loggy running locally:  
  ```bash
  loggy --port 8080 --db-url ./loggy.db
  ```  
- Your ASP.NET Core project (target .NET 7/8)  

## 2. Add NuGet Packages  
```bash
dotnet add package Serilog.AspNetCore
dotnet add package Serilog.Sinks.OpenTelemetry
```

## 3. Bootstrap Serilog in `Program.cs`  
1. **At the very top** of the file, before `CreateBuilder(args)`:  
   ```csharp
   using Serilog;
   using Serilog.Sinks.OpenTelemetry;
   ```  
2. **Replace** the default host logger with Serilog, and configure sinks:  
   ```csharp
   var builder = WebApplication.CreateBuilder(args);

   // Replace built-in logging
   builder.Host.UseSerilog((hostingContext, services, lc) => {
       lc.Enrich.FromLogContext()
         // Always write to console:
         .WriteTo.Console();

   #if DEBUG
       // Only in DEBUG + Development, send to your local Loggy OTLP endpoint:
       if (hostingContext.HostingEnvironment.IsDevelopment())
       {
           lc.WriteTo.OpenTelemetry(
             endpoint: "http://localhost:8080/v1/logs",
             protocol: OtlpProtocol.HttpProtobuf);
       }
   #endif
   });
   ```

## 4. (Optional) Enable HTTP-Request Logging  
```csharp
var app = builder.Build();

// **Before** UseRouting/UseAuthorization/AddControllers:
app.UseSerilogRequestLogging();  
```
This emits one concise log per HTTP request, with method, path, status, and timing.

## 5. Run & Verify  
1. Start your ASP.NET app in **Development** (`dotnet run` in DEBUG).  
2. Exercise an endpoint (e.g. `curl http://localhost:5000/`).  
3. In another shell, tail Loggy's logs:  
   ```bash
   loggy tail_logs --lines=20
   ```  
   You should see your app's info/debug entries flowing into Loggy.

## 6. Tips & Best Practices  
- Use `Log.Debug(...)` and structure-enriched logs liberally in services and controllers.  
- In production (non-DEBUG), you'll still get console logs (or add other sinks) without spamming your OTLP endpoint.  
- Pair with filters/overrides (`MinimumLevel.Override("Microsoft", LogEventLevel.Warning)`) to suppress verbose framework noise.

---

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