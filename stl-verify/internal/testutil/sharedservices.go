package testutil

import (
	"fmt"
	"os"
)

// Environment variables through which CI hands the suite services it owns
// itself, one set per integration shard. Unset locally, where each package
// starts its own container from TestMain instead.
//
// The Postgres server named here must be disposable: the suite creates and drops
// databases on it and permanently disables its TimescaleDB background workers
// (see disableBackgroundWorkers).
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

// maxIdentifierLen is PostgreSQL's identifier limit, which every name handed to a
// shared server has to fit.
const maxIdentifierLen = 63

// processTag identifies this test binary among the sibling packages `go test -p`
// runs concurrently.
func processTag() string {
	return fmt.Sprintf("p%d", os.Getpid())
}

// withProcessTag scopes a name to this test binary, so two packages cannot address
// — or drop — one another's resources on a server they share.
//
// Neither kind of name is unique on its own: Go guarantees test names are unique
// only within a package, and the names test files pass to SetupDBForMain are
// hand-written. Tagging here is what keeps that from being each caller's problem.
func withProcessTag(name string) string {
	suffix := "_" + processTag()
	if budget := maxIdentifierLen - len(suffix); len(name) > budget {
		name = name[:budget]
	}
	return name + suffix
}
