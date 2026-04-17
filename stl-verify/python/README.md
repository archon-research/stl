# stl-verify python

FastAPI service for sentinel verify.

## Setup

```bash
uv sync
```

Configuration defaults live in `.env.default`. Optional local overrides go in
`.env` (same directory), which takes precedence over `.env.default`.

## Run

Start the local infrastructure first (PostgreSQL, Redis, Jaeger):

```bash
# From stl-verify/
make dev-up
```

Then start the FastAPI server:

```bash
# From stl-verify/python/
make run
```

## Test

```bash
make test-unit
make test-integration
```

## Lint

```bash
make lint
make lint-fix
```

## Observability

### Request ID

Every HTTP request is assigned a UUID via `RequestIdMiddleware`. The ID is:
- Available in application code via `get_request_id()` from `app.middleware.request_id`
- Returned to callers in the `X-Request-ID` response header
- Automatically included in structured log lines
- Attached as a span attribute (`request_id`) in OpenTelemetry traces
- Preserved from inbound requests if the `X-Request-ID` header is already set

### Structured Logging

All application code uses `get_logger(name)` from `app.logging` instead of the
standard `logging` module directly. Logs are structured JSON by default.

| Env var      | Values              | Default |
|-------------|---------------------|---------|
| `LOG_FORMAT` | `json` / `text`     | `json`  |
| `LOG_LEVEL`  | `DEBUG` / `INFO` / `WARNING` / `ERROR` | `INFO` |

### OpenTelemetry Tracing

Tracing is disabled by default. Enable it with `OTEL_ENABLED=true`.

| Env var                        | Default                    |
|-------------------------------|----------------------------|
| `OTEL_ENABLED`                 | `false`                    |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317`    |
| `OTEL_SERVICE_NAME`            | `stl-verify-python`        |

### Local Verification

1. Start infrastructure (PostgreSQL, Redis, Jaeger):
   ```bash
   # From stl-verify/
   make dev-up
   ```

2. Start the FastAPI server with tracing enabled:
   ```bash
   # From stl-verify/python/
   OTEL_ENABLED=true LOG_FORMAT=text make run
   ```

3. Send a request and note the request ID:
   ```bash
   curl -si http://localhost:8000/v1/status
   # Look for: X-Request-ID: <uuid>
   ```

4. Check logs: the same request ID should appear in the log output.

5. Open Jaeger UI at http://localhost:16686, find the trace for `stl-verify-python`,
   confirm the `request_id` attribute matches the response header.
