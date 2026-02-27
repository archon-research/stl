package testutil

// Container image tags used by integration and benchmark tests.
// The CI workflow derives its docker pull list from these constants, so
// updating a tag here is the only change needed to keep CI and tests aligned.
const (
	ImageTimescaleDB = "timescale/timescaledb:2.25.1-pg17"
	ImageRedis       = "redis:8.0.6-alpine"
	ImageLocalStack  = "localstack/localstack:4.3"
)
