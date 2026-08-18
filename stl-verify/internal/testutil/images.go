package testutil

// Container image tags used by integration and benchmark tests. The integration
// job runs the same tags from its own `services:` block, so a bump here needs the
// same bump there; ci/check-ci-services.sh fails when the two disagree.
const (
	ImageTimescaleDB = "timescale/timescaledb:2.25.1-pg17"
	ImageRedis       = "redis:8.0.6-alpine"
	ImageLocalStack  = "localstack/localstack:4.3"
)
