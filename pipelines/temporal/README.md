# Temporal Pipelines

This directory contains the **Temporal.io** workflow and activity definitions for the STL project's ETL pipelines.

## Structure

- **`workflows/`**: Contains the Temporal Workflow definitions. These orchestrate the execution of activities.
  - Workflows must be deterministic.
- **`activities/`**: Contains the Temporal Activity definitions. These perform the actual work (e.g., fetching data, writing to DB).
  - Activities can contain non-deterministic code (API calls, DB operations).
- **`worker/`**: Contains the worker initialization code. Registers and runs workflows/activities.
- **`starter/`**: Contains helper code to trigger workflows programmatically.

## Usage

```go
// Example Worker Setup (in worker/main.go)
w := worker.New(client, "task-queue-name", worker.Options{})
w.RegisterWorkflow(workflows.RiskAnalysisWorkflow)
w.RegisterActivity(activities.FetchData)
w.Run(worker.InterruptCh())
```

## Running

```bash
# Start the worker
go run ./pipelines/temporal/worker

# Trigger a workflow
go run ./pipelines/temporal/starter
```
