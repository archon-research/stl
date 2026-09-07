package testutil

// Container image tags used by integration and benchmark tests. The integration
// job runs the same tags from its own `services:` block, so a bump here needs the
// same bump there; ci/check-ci-services.sh fails when the two disagree.
//
// The TimescaleDB tag tracks the engine TigerData runs (2.29 on PostgreSQL 18):
// a suite on an older major cannot see what production plans, stores, or names.
const (
	ImageTimescaleDB = "timescale/timescaledb:2.29.2-pg18"
	ImageRedis       = "redis:8.0.6-alpine"
	ImageLocalStack  = "localstack/localstack:4.3"
)
