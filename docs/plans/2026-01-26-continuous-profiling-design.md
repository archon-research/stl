# Continuous Profiling to S3 Design

## Overview

Add continuous pprof and trace capture to live servers with automatic upload to S3 for streamlined debugging.

## Architecture

### New Component: ProfileUploader Service

Location: `stl-verify/internal/adapters/outbound/s3/profile_uploader.go`

A background goroutine that:
1. Runs alongside existing services when enabled via `--enable-profiling` flag
2. Captures 2-minute profiles every 5 minutes
3. Uploads to S3 with gzip compression
4. Captures: CPU, block, mutex profiles and execution traces

### S3 Path Structure

```
s3://stl-debugging-{environment}/profiles/{service-name}/{profile-type}/{date}/{timestamp}.pprof.gz
```

Examples:
```
s3://stl-debugging-prod/profiles/watcher/cpu/2026-01-26/15-30-00.pprof.gz
s3://stl-debugging-prod/profiles/watcher/block/2026-01-26/15-30-00.pprof.gz
s3://stl-debugging-prod/profiles/watcher/mutex/2026-01-26/15-30-00.pprof.gz
s3://stl-debugging-prod/profiles/watcher/trace/2026-01-26/15-30-00.trace.gz
```

## Configuration

### ProfileUploaderConfig

```go
type ProfileUploaderConfig struct {
    Bucket          string        // "stl-debugging-{env}"
    ServiceName     string        // "watcher"
    CaptureInterval time.Duration // 5 minutes
    ProfileDuration time.Duration // 2 minutes
    EnableCPU       bool
    EnableBlock     bool
    EnableMutex     bool
    EnableTrace     bool
    Logger          *slog.Logger
}
```

### Command-line Flag

```go
enableProfiling := flag.Bool("enable-profiling", false, "Enable continuous profiling upload to S3")
```

Disabled by default. Opt-in per environment.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PROFILE_S3_BUCKET` | `stl-debugging` | S3 bucket name (environment suffix added) |
| `PROFILE_INTERVAL` | `5m` | Time between profile captures |
| `PROFILE_DURATION` | `2m` | Duration of each CPU/trace capture |

## Capture Flow

1. Timer fires every 5 minutes
2. Capture all enabled profile types in parallel
3. Upload each to S3 with gzip compression
4. Log success/failure for observability

### Profile Capture Methods

- **CPU:** `runtime/pprof.StartCPUProfile()` / `StopCPUProfile()` for 2 minutes
- **Block:** `runtime/pprof.Lookup("block").WriteTo()` (point-in-time snapshot)
- **Mutex:** `runtime/pprof.Lookup("mutex").WriteTo()` (point-in-time snapshot)
- **Trace:** `runtime/trace.Start()` / `Stop()` for 2 minutes

CPU and trace run for the full duration; block/mutex are snapshots taken at the end.

## Terraform

### New File: `infra/s3_profiling.tf`

```hcl
resource "aws_s3_bucket" "debugging" {
  bucket = "stl-debugging-${var.environment}"
}

resource "aws_s3_bucket_lifecycle_configuration" "debugging" {
  bucket = aws_s3_bucket.debugging.id

  rule {
    id     = "expire-old-profiles"
    status = "Enabled"

    expiration {
      days = 30
    }

    filter {
      prefix = "profiles/"
    }
  }
}
```

30-day lifecycle policy auto-expires old profiles.

### IAM Updates

Add `s3:PutObject` permission to ECS task role for the new bucket.

## Implementation Tasks

1. Create `ProfileUploader` service in `internal/adapters/outbound/s3/profile_uploader.go`
2. Add `--enable-profiling` flag and env var parsing in `cmd/watcher/main.go`
3. Create `infra/s3_profiling.tf` with bucket and lifecycle configuration
4. Update `infra/iam_ecs_task.tf` to grant S3 write access
5. Add unit tests for ProfileUploader
6. Update deployment configs to enable profiling in staging/prod
