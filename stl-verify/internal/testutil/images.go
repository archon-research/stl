package testutil

// Container image tags used by integration and benchmark tests. The integration
// job runs the same tags from its own `services:` block, so a bump here needs the
// same bump there; ci/check-ci-services.sh fails when the two disagree.
const (
	ImagePostgres   = "postgres:18"
	ImageRedis      = "redis:8.0.6-alpine"
	ImageLocalStack = "localstack/localstack:4.3"
)
