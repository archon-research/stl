# Pipelines

Data pipelines using Temporal.io for ETL and background processing.

## Contents
- `temporal/workflows/` - Workflow definitions (must be deterministic)
- `temporal/activities/` - Activity definitions (can do I/O)
- `temporal/worker/` - Worker registration and startup
- `temporal/starter/` - Workflow trigger utilities
- `schemas/` - Data schemas (Avro, etc.)

## Temporal Guidelines
- **Workflows**: No random, no `time.Now()`, no I/O
- **Activities**: API calls, DB writes, external operations
- Use `workflow.ExecuteActivity()` to call activities

## Running
```bash
go run ./pipelines/temporal/worker   # Start worker
go run ./pipelines/temporal/starter  # Trigger workflow
```
