# Agent Instructions: Pipelines

## Purpose
Data pipelines using Temporal.io for ETL and background processing.

## Structure
```
pipelines/
├── temporal/
│   ├── workflows/    # Workflow definitions (deterministic)
│   ├── activities/   # Activity definitions (can be non-deterministic)
│   ├── worker/       # Worker registration and startup
│   └── starter/      # Workflow trigger utilities
└── schemas/          # Data schemas (Avro, etc.)
```

## Temporal Guidelines
- **Workflows** must be deterministic (no random, no time.Now(), no I/O)
- **Activities** perform actual work (API calls, DB writes, etc.)
- Use `workflow.ExecuteActivity()` to call activities from workflows

## Adding a Workflow
1. Define workflow in `workflows/<name>_workflow.go`
2. Define activities in `activities/<name>.go`
3. Register in `worker/main.go`
4. Trigger via `starter/` or external caller

## Running
```bash
# Start worker
go run ./pipelines/temporal/worker

# Trigger workflow
go run ./pipelines/temporal/starter
```

## Schemas
- Avro schemas in `schemas/` for data serialization
- Used for event payloads and data warehouse ingestion
