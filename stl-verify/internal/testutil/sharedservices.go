package testutil

import (
	"fmt"
	"os"
)

// Environment variables through which CI hands the suite services it owns
// itself, one set per integration shard. Unset locally, where each package
// starts its own container from TestMain instead.
const (
	EnvPostgresDSN        = "STL_TEST_POSTGRES_DSN"
	EnvRedisAddr          = "STL_TEST_REDIS_ADDR"
	EnvLocalStackEndpoint = "STL_TEST_LOCALSTACK_ENDPOINT"
)

// sharedService returns the value of a job-scoped service variable, reporting
// whether CI set it. An empty value counts as unset so a blank variable in a
// workflow does not silently point tests at nothing.
func sharedService(envVar string) (value string, ok bool) {
	value = os.Getenv(envVar)
	return value, value != ""
}

// noopCleanup stands in for a container terminator when the service belongs to
// CI: tearing it down is the job's business, not a test package's.
func noopCleanup() {}

// processTag identifies this test binary among the sibling packages `go test -p`
// runs concurrently. Go guarantees test names are unique only within a package,
// so any name derived from t.Name() needs it once packages share a service.
func processTag() string {
	return fmt.Sprintf("p%d", os.Getpid())
}
