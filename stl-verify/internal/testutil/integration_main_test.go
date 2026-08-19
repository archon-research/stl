package testutil

import (
	"slices"
	"testing"
)

// eventLog records the order of an IntegrationMain's lifecycle steps. The
// container starts are injected through withContainer so the ordering is
// checkable without a container runtime.
type eventLog struct {
	events []string
}

func (l *eventLog) record(event string) {
	l.events = append(l.events, event)
}

func (l *eventLog) hook(name string) func() {
	return func() { l.record(name) }
}

func (l *eventLog) withFakeContainer(im *IntegrationMain, name string) *IntegrationMain {
	return im.withContainer(func() func() {
		l.record("start " + name)
		return func() { l.record("stop " + name) }
	})
}

func (l *eventLog) newIntegrationMain(exitCode int) *IntegrationMain {
	return &IntegrationMain{run: func() int {
		l.record("tests")
		return exitCode
	}}
}

func assertEvents(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("event order:\n got %v\nwant %v", got, want)
	}
}

func TestIntegrationMainStartsContainersInRequestOrder(t *testing.T) {
	var log eventLog
	im := log.newIntegrationMain(0)
	log.withFakeContainer(im, "db")
	log.withFakeContainer(im, "redis")
	log.withFakeContainer(im, "localstack")

	im.runTests()

	assertEvents(t, log.events[:4], []string{"start db", "start redis", "start localstack", "tests"})
}

func TestIntegrationMainStopsContainersInReverseOrder(t *testing.T) {
	var log eventLog
	im := log.newIntegrationMain(0)
	log.withFakeContainer(im, "db")
	log.withFakeContainer(im, "redis")
	log.withFakeContainer(im, "localstack")

	im.runTests()

	assertEvents(t, log.events[3:], []string{"tests", "stop localstack", "stop redis", "stop db"})
}

func TestIntegrationMainRunsHooksBetweenContainersAndTests(t *testing.T) {
	var log eventLog
	im := log.newIntegrationMain(0)
	log.withFakeContainer(im, "db")
	im.BeforeRun(log.hook("before")).AfterRun(log.hook("after"))

	im.runTests()

	assertEvents(t, log.events, []string{"start db", "before", "tests", "after", "stop db"})
}

func TestIntegrationMainRunsHooksInRegistrationOrder(t *testing.T) {
	var log eventLog
	im := log.newIntegrationMain(0)
	im.BeforeRun(log.hook("before 1")).BeforeRun(log.hook("before 2"))
	im.AfterRun(log.hook("after 1")).AfterRun(log.hook("after 2"))

	im.runTests()

	assertEvents(t, log.events, []string{"before 1", "before 2", "tests", "after 1", "after 2"})
}

func TestIntegrationMainReturnsTestExitCode(t *testing.T) {
	var log eventLog
	im := log.newIntegrationMain(3)
	log.withFakeContainer(im, "db")

	if code := im.runTests(); code != 3 {
		t.Fatalf("exit code = %d, want 3", code)
	}
}

func TestIntegrationMainStopsContainersWhenAHookPanics(t *testing.T) {
	var log eventLog
	im := log.newIntegrationMain(0)
	log.withFakeContainer(im, "db")
	im.BeforeRun(func() { panic("setup failed") })

	func() {
		defer func() { _ = recover() }()
		im.runTests()
	}()

	assertEvents(t, log.events, []string{"start db", "stop db"})
}
