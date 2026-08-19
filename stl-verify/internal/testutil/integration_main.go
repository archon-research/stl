package testutil

import (
	"slices"
	"testing"
)

// IntegrationMain builds the TestMain of an integration-test package: it starts
// the one container set the whole package shares (the rule `make
// shared-container-check` enforces), hands each container's address to the
// package var the tests read, and folds the goroutine-leak check into the exit
// code — so no package carries its own copy of that bookkeeping.
//
//	func TestMain(m *testing.M) {
//		os.Exit(testutil.NewIntegrationMain(m).WithTimescaleDB(&sharedDSN).Run())
//	}
type IntegrationMain struct {
	run       func() int
	starts    []containerStart
	beforeRun []func()
	afterRun  []func()
}

// containerStart starts one container and returns its teardown.
type containerStart func() (cleanup func())

// NewIntegrationMain returns a builder that runs m's tests inside the
// containers registered on it.
func NewIntegrationMain(m *testing.M) *IntegrationMain {
	return &IntegrationMain{run: m.Run}
}

// WithTimescaleDB starts a TimescaleDB container and publishes its DSN through dsn.
func (im *IntegrationMain) WithTimescaleDB(dsn *string) *IntegrationMain {
	return im.withContainer(func() func() {
		value, cleanup := StartTimescaleDBForMain()
		*dsn = value
		return cleanup
	})
}

// WithRedis starts a Redis container and publishes its address through addr.
func (im *IntegrationMain) WithRedis(addr *string) *IntegrationMain {
	return im.withContainer(func() func() {
		value, cleanup := StartRedisForMain()
		*addr = value
		return cleanup
	})
}

// WithLocalStack starts a LocalStack container offering services (the comma-separated
// list LocalStack's SERVICES takes) and publishes its config through cfg.
func (im *IntegrationMain) WithLocalStack(services string, cfg *LocalStackConfig) *IntegrationMain {
	return im.withContainer(func() func() {
		value, cleanup := StartLocalStackForMain(services)
		*cfg = value
		return cleanup
	})
}

// BeforeRun registers work to do once the containers are up, before the tests run.
func (im *IntegrationMain) BeforeRun(fn func()) *IntegrationMain {
	im.beforeRun = append(im.beforeRun, fn)
	return im
}

// AfterRun registers work to do once the tests are done, before the containers stop.
func (im *IntegrationMain) AfterRun(fn func()) *IntegrationMain {
	im.afterRun = append(im.afterRun, fn)
	return im
}

// Run runs the package's tests inside the requested containers and returns the
// exit code to pass to os.Exit.
func (im *IntegrationMain) Run() int {
	return CheckGoroutineLeaks(im.runTests())
}

func (im *IntegrationMain) withContainer(start containerStart) *IntegrationMain {
	im.starts = append(im.starts, start)
	return im
}

func (im *IntegrationMain) runTests() int {
	stopContainers := im.startContainers()
	defer stopContainers()

	runHooks(im.beforeRun)
	code := im.run()
	runHooks(im.afterRun)

	return code
}

// startContainers starts the containers in the order they were requested and
// returns a teardown that stops them in reverse, so a container never outlives
// one that was started after it.
func (im *IntegrationMain) startContainers() func() {
	cleanups := make([]func(), 0, len(im.starts))
	for _, start := range im.starts {
		cleanups = append(cleanups, start())
	}

	return func() {
		for _, cleanup := range slices.Backward(cleanups) {
			cleanup()
		}
	}
}

func runHooks(hooks []func()) {
	for _, hook := range hooks {
		hook()
	}
}
