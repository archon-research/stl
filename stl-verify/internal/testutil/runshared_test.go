package testutil

import (
	"slices"
	"testing"
)

// recorder collects the lifecycle steps runShared takes, in the order it takes
// them: what this helper exists to own is an order, so the order is the assertion.
type recorder struct {
	steps []string
}

func (r *recorder) record(step string) { r.steps = append(r.steps, step) }

func (r *recorder) cleanupFor(service string) func() {
	return func() { r.record("stop " + service) }
}

// starters returns fakes for every service, so the ordering tests need no
// container and no server.
func (r *recorder) starters() serviceStarters {
	return serviceStarters{
		timescaleDB: func() (string, func()) {
			r.record("start timescaledb")
			return "postgres://fake/db", r.cleanupFor("timescaledb")
		},
		redis: func() (string, func()) {
			r.record("start redis")
			return "127.0.0.1:6379", r.cleanupFor("redis")
		},
		localStack: func(services string) (LocalStackConfig, func()) {
			r.record("start localstack " + services)
			return LocalStackConfig{Endpoint: "http://fake"}, r.cleanupFor("localstack")
		},
		checkLeaks: func(code int) int {
			r.record("check leaks")
			return code
		},
	}
}

func (r *recorder) run(code int) func() int {
	return func() int {
		r.record("run tests")
		return code
	}
}

func TestRunShared_StopsServicesInReverseStartOrder(t *testing.T) {
	var rec recorder
	var dsn, redisAddr string
	var localStack LocalStackConfig

	runShared(rec.run(0), Shared{
		TimescaleDSN:       &dsn,
		RedisAddr:          &redisAddr,
		LocalStack:         &localStack,
		LocalStackServices: "s3,sqs",
	}, rec.starters())

	want := []string{
		"start timescaledb",
		"start redis",
		"start localstack s3,sqs",
		"run tests",
		"stop localstack",
		"stop redis",
		"stop timescaledb",
		"check leaks",
	}
	if !slices.Equal(rec.steps, want) {
		t.Errorf("lifecycle ran as\n\t%v\nwant\n\t%v", rec.steps, want)
	}
}

func TestRunShared_StartsOnlyTheServicesTheCallerAsksFor(t *testing.T) {
	var rec recorder
	var localStack LocalStackConfig

	runShared(rec.run(0), Shared{
		LocalStack:         &localStack,
		LocalStackServices: "s3",
	}, rec.starters())

	want := []string{"start localstack s3", "run tests", "stop localstack", "check leaks"}
	if !slices.Equal(rec.steps, want) {
		t.Errorf("lifecycle ran as\n\t%v\nwant\n\t%v", rec.steps, want)
	}
}

func TestRunShared_PublishesEveryServiceHandleBeforeTheTestsRun(t *testing.T) {
	var rec recorder
	var dsn, redisAddr string
	var localStack LocalStackConfig

	starters := rec.starters()
	run := func() int {
		if dsn == "" || redisAddr == "" || localStack.Endpoint == "" {
			t.Errorf("tests ran with dsn=%q redisAddr=%q localStackEndpoint=%q, want all populated",
				dsn, redisAddr, localStack.Endpoint)
		}
		return 0
	}

	runShared(run, Shared{
		TimescaleDSN:       &dsn,
		RedisAddr:          &redisAddr,
		LocalStack:         &localStack,
		LocalStackServices: "s3",
	}, starters)
}

func TestRunShared_RunsTheCallersHooksAroundTheTests(t *testing.T) {
	var rec recorder
	var dsn string

	runShared(rec.run(0), Shared{
		TimescaleDSN: &dsn,
		BeforeRun:    func() { rec.record("before") },
		AfterRun:     func() { rec.record("after") },
	}, rec.starters())

	want := []string{
		"start timescaledb",
		"before",
		"run tests",
		"after",
		"stop timescaledb",
		"check leaks",
	}
	if !slices.Equal(rec.steps, want) {
		t.Errorf("lifecycle ran as\n\t%v\nwant\n\t%v", rec.steps, want)
	}
}

func TestRunShared_LetsTheLeakCheckDecideTheExitCode(t *testing.T) {
	var rec recorder
	starters := rec.starters()
	starters.checkLeaks = func(int) int {
		rec.record("check leaks")
		return 1
	}

	if code := runShared(rec.run(0), Shared{}, starters); code != 1 {
		t.Errorf("exit code is %d, want 1: a leak the tests did not notice still fails the run", code)
	}
}

func TestRunShared_KeepsAFailingTestRunFailing(t *testing.T) {
	var rec recorder

	if code := runShared(rec.run(3), Shared{}, rec.starters()); code != 3 {
		t.Errorf("exit code is %d, want 3: the leak check must not mask a test failure", code)
	}
}

func TestShared_RejectsLocalStackWithoutServices(t *testing.T) {
	err := Shared{LocalStack: &LocalStackConfig{}}.validate()
	if err == nil {
		t.Error("accepted a LocalStack request with no services, which starts every service it has")
	}
}

func TestShared_RejectsServicesWithoutLocalStack(t *testing.T) {
	err := Shared{LocalStackServices: "s3"}.validate()
	if err == nil {
		t.Error("accepted a services list with nowhere to put it, so LocalStack would never start")
	}
}
