package testutil

import (
	"errors"
	"log"
	"slices"
	"testing"
)

// Shared declares which services a test binary needs. A nil handle means the
// package does not want that service; a non-nil one is where its address is
// published before the tests run, so package vars stay the way tests reach it.
type Shared struct {
	TimescaleDSN *string
	RedisAddr    *string
	LocalStack   *LocalStackConfig
	// LocalStackServices is LocalStack's SERVICES list, required alongside it.
	LocalStackServices string

	// BeforeRun and AfterRun bracket the tests, for a package that needs more than
	// a service handle — a database shared by one test file, say. BeforeRun runs
	// with every handle already published, AfterRun before any service stops.
	BeforeRun func()
	AfterRun  func()
}

// RunShared owns the whole TestMain lifecycle and returns the exit code, leaving
// the caller a single statement:
//
//	func TestMain(m *testing.M) {
//		os.Exit(testutil.RunShared(m, testutil.Shared{TimescaleDSN: &sharedDSN}))
//	}
//
// The order it keeps is load-bearing and unenforceable by the compiler: a package
// that stops its services after the leak check reports the teardown it is waiting
// for, and one that lets m.Run own the exit code drops the leaks it finds. It
// returns the code rather than exiting so that order can be tested.
func RunShared(m *testing.M, s Shared) int {
	return runShared(m.Run, s, liveStarters())
}

// serviceStarters is the set of service constructors runShared drives, injected
// so the lifecycle can be tested without a container in sight.
type serviceStarters struct {
	timescaleDB func() (dsn string, cleanup func())
	redis       func() (addr string, cleanup func())
	localStack  func(services string) (cfg LocalStackConfig, cleanup func())
	checkLeaks  func(code int) int
}

func liveStarters() serviceStarters {
	return serviceStarters{
		timescaleDB: StartTimescaleDBForMain,
		redis:       StartRedisForMain,
		localStack:  StartLocalStackForMain,
		checkLeaks:  CheckGoroutineLeaks,
	}
}

func runShared(run func() int, s Shared, start serviceStarters) int {
	if err := s.validate(); err != nil {
		log.Fatalf("testutil.RunShared: %v", err)
	}
	stopServices := s.startServices(start)

	code := s.runTests(run)

	// Before the leak check, never after: a service client still shutting down is a
	// live goroutine, and the check would report the teardown it is waiting for.
	stopServices()

	return start.checkLeaks(code)
}

// startServices starts each declared service, publishes its handle, and returns
// the teardown that unwinds them in reverse start order.
func (s Shared) startServices(start serviceStarters) (stopServices func()) {
	var stops []func()

	if s.TimescaleDSN != nil {
		dsn, stop := start.timescaleDB()
		*s.TimescaleDSN = dsn
		stops = append(stops, stop)
	}
	if s.RedisAddr != nil {
		addr, stop := start.redis()
		*s.RedisAddr = addr
		stops = append(stops, stop)
	}
	if s.LocalStack != nil {
		cfg, stop := start.localStack(s.LocalStackServices)
		*s.LocalStack = cfg
		stops = append(stops, stop)
	}

	return func() {
		for _, stop := range slices.Backward(stops) {
			stop()
		}
	}
}

// runTests runs the package's tests inside the caller's hooks.
func (s Shared) runTests(run func() int) int {
	if s.BeforeRun != nil {
		s.BeforeRun()
	}

	code := run()

	if s.AfterRun != nil {
		s.AfterRun()
	}
	return code
}

// validate rejects a declaration that would start the wrong thing. LocalStack is
// the pair that can go wrong quietly: an empty SERVICES list makes it start every
// service it ships, and a list with no LocalStack requested starts none.
func (s Shared) validate() error {
	if s.LocalStack != nil && s.LocalStackServices == "" {
		return errors.New("LocalStack needs LocalStackServices, e.g. \"s3,sqs\"")
	}
	if s.LocalStack == nil && s.LocalStackServices != "" {
		return errors.New("LocalStackServices was given without a LocalStack handle to fill")
	}
	return nil
}
