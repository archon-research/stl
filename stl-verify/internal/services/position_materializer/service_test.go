package position_materializer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// mockMaterializer implements outbound.PositionMaterializer with a func field.
type mockMaterializer struct {
	fn         func(ctx context.Context, view string, buildID int, runID int64) (int64, error)
	calls      []string
	refused    map[string]int64
	refusedErr error
	cacheRows  map[string]int64
	cacheErr   error
	refusedRun int64
	since      time.Time
	missing    []string
	missingErr error
}

func (m *mockMaterializer) Materialize(ctx context.Context, view string, buildID int, runID int64) (int64, error) {
	m.calls = append(m.calls, view)
	return m.fn(ctx, view, buildID, runID)
}

func (m *mockMaterializer) RefusedByProjection(_ context.Context, runID int64, since time.Time) (map[string]int64, error) {
	m.refusedRun = runID
	m.since = since
	return m.refused, m.refusedErr
}

func (m *mockMaterializer) MissingMaterializers(context.Context, []string) ([]string, error) {
	return m.missing, m.missingErr
}

func (m *mockMaterializer) CacheRowEstimates(context.Context) (map[string]int64, error) {
	return m.cacheRows, m.cacheErr
}

func TestNewService_Validation(t *testing.T) {
	ok := &mockMaterializer{fn: func(context.Context, string, int, int64) (int64, error) { return 0, nil }}
	cases := []struct {
		name    string
		views   []string
		mat     *mockMaterializer
		buildID int
		runID   int64
		want    string
	}{
		{"nil materializer", []string{"v"}, nil, 1, 77, "materializer is required"},
		{"empty views", nil, ok, 1, 77, "no projection materializers configured"},
		{"blank view entry", []string{"a", "  "}, ok, 1, 77, "blank entry"},
		{"duplicate view", []string{"a", "b", "a"}, ok, 1, 77, "configured twice"},
		{"negative buildID", []string{"a"}, ok, -1, 77, "must not be negative"},
		// A zero run is the shape the wiring produces when it never opens one, and every row the
		// sweep appended would then name a writer_run that does not exist.
		{"zero runID", []string{"a"}, ok, 1, 0, "must be a writer_run id"},
		{"negative runID", []string{"a"}, ok, 1, -1, "must be a writer_run id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mat *mockMaterializer
			if tc.mat != nil {
				mat = tc.mat
			}
			var err error
			if mat == nil {
				_, err = NewService(tc.views, nil, tc.buildID, tc.runID, nil, nil)
			} else {
				_, err = NewService(tc.views, mat, tc.buildID, tc.runID, nil, nil)
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("NewService error = %v; want it to contain %q", err, tc.want)
			}
		})
	}
}

// The run id is opened once per process and has to reach every projection, or the rows a sweep
// appends cannot be attributed to the artefact that wrote them (ADR-0006 §2).
func TestRunOncePropagatesTheWriterRunToEveryProjection(t *testing.T) {
	var runs []int64
	mat := &mockMaterializer{fn: func(_ context.Context, _ string, _ int, runID int64) (int64, error) {
		runs = append(runs, runID)
		return 0, nil
	}}
	svc, err := NewService([]string{"va", "vb", "vc"}, mat, 4711, 8823, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("materialized %d projections, want 3", len(runs))
	}
	for _, r := range runs {
		if r != 8823 {
			t.Errorf("runID = %d; want 8823 propagated to every projection", r)
		}
	}
}

func TestRunOnce_AllViewsInOrderWithReason(t *testing.T) {
	var builds []int

	mat := &mockMaterializer{fn: func(_ context.Context, _ string, buildID int, _ int64) (int64, error) {
		builds = append(builds, buildID)
		return 3, nil
	}}
	svc, err := NewService([]string{"va", "vb", "vc"}, mat, 4711, 77, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got := strings.Join(mat.calls, ","); got != "va,vb,vc" {
		t.Errorf("call order = %s; want va,vb,vc (sequential, configured order)", got)
	}
	for _, b := range builds {
		if b != 4711 {
			t.Errorf("buildID = %d; want 4711 propagated to every view", b)
		}
	}
}

func TestRunOnce_OneFailureDoesNotStarveTheRest(t *testing.T) {
	boom := errors.New("contract violation")
	mat := &mockMaterializer{fn: func(_ context.Context, view string, _ int, _ int64) (int64, error) {
		if view == "vb" {
			return 0, boom
		}
		return 1, nil
	}}
	svc, err := NewService([]string{"va", "vb", "vc"}, mat, 4711, 77, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	runErr := svc.RunOnce(context.Background())
	if runErr == nil {
		t.Fatal("RunOnce = nil; want the vb failure surfaced")
	}
	if !errors.Is(runErr, boom) || !strings.Contains(runErr.Error(), "materializer vb") {
		t.Errorf("RunOnce error = %v; want it to wrap the vb failure and name the materializer", runErr)
	}
	if got := strings.Join(mat.calls, ","); got != "va,vb,vc" {
		t.Errorf("calls = %s; want all three views attempted despite vb failing", got)
	}
}

func TestRunOnce_ParentCancellationAborts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mat := &mockMaterializer{fn: func(_ context.Context, view string, _ int, _ int64) (int64, error) {
		if view == "va" {
			cancel() // cancellation arrives while the first view is running
		}
		return 1, nil
	}}
	svc, err := NewService([]string{"va", "vb"}, mat, 1, 77, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	runErr := svc.RunOnce(ctx)
	if !errors.Is(runErr, context.Canceled) {
		t.Errorf("RunOnce error = %v; want context.Canceled surfaced", runErr)
	}
	if got := strings.Join(mat.calls, ","); got != "va" {
		t.Errorf("calls = %s; want only va (vb aborted by cancellation)", got)
	}
}

func TestTelemetry_NilSafeAndConstructible(t *testing.T) {
	var nilT *Telemetry
	nilT.RecordRun(context.Background(), "v", statusOK, 5) // must not panic

	tel, err := NewTelemetry(nil) // global provider is a no-op meter in tests
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}
	tel.RecordRun(context.Background(), "v", statusOK, 5)
	tel.RecordRun(context.Background(), "v", statusError, 0)
}

// The withheld level is read for this process's writer run only. Read across every run, a projection
// retired with a non-zero level republished that level every tick and its alert never cleared.
func TestRunOnce_ReadsTheWithheldLevelForItsOwnRun(t *testing.T) {
	mm := &mockMaterializer{fn: func(context.Context, string, int, int64) (int64, error) { return 0, nil }}
	s, err := NewService([]string{"materialize_a"}, mm, 0, 8823, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if mm.refusedRun != 8823 {
		t.Errorf("withheld level read for run %d; want this process's run 8823", mm.refusedRun)
	}
}

// The withheld level is read from this tick's runs only. Unbounded, a projection that withheld 12 and
// then failed every tick kept returning its old row, and the gauge read 12 for the life of the pod.
func TestRunOnce_ReadsTheWithheldLevelFromThisTickOnly(t *testing.T) {
	mm := &mockMaterializer{fn: func(context.Context, string, int, int64) (int64, error) { return 0, nil }}
	s, err := NewService([]string{"materialize_a"}, mm, 0, 8823, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	before := time.Now()
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if mm.since.IsZero() || mm.since.After(before) || mm.since.Before(before.Add(-2*refusedReadSkew)) {
		t.Errorf("withheld level read since %v; want within %v before the tick started at %v", mm.since, refusedReadSkew, before)
	}
}

// A configured wrapper that does not exist fails at startup naming it, rather than on every tick.
func TestCheckConfigured(t *testing.T) {
	for _, c := range []struct {
		name    string
		missing []string
		err     error
		want    string
	}{
		{"all present", nil, nil, ""},
		{"one missing", []string{"materialize_b"}, nil, "materialize_b"},
		{"lookup fails", nil, errors.New("connection refused"), "connection refused"},
	} {
		t.Run(c.name, func(t *testing.T) {
			mm := &mockMaterializer{missing: c.missing, missingErr: c.err}
			s, err := NewService([]string{"materialize_a", "materialize_b"}, mm, 0, 1, nil, nil)
			if err != nil {
				t.Fatalf("NewService: %v", err)
			}
			err = s.CheckConfigured(context.Background())
			if c.want == "" {
				if err != nil {
					t.Errorf("CheckConfigured = %v; want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("CheckConfigured = %v; want an error naming %q", err, c.want)
			}
		})
	}
}
