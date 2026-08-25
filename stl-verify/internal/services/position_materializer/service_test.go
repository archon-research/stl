package position_materializer

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// mockMaterializer implements outbound.PositionMaterializer with a func field.
type mockMaterializer struct {
	fn    func(ctx context.Context, view string, buildID int) (int64, error)
	calls []string
}

func (m *mockMaterializer) Materialize(ctx context.Context, view string, buildID int) (int64, error) {
	m.calls = append(m.calls, view)
	return m.fn(ctx, view, buildID)
}

func TestNewService_Validation(t *testing.T) {
	ok := &mockMaterializer{fn: func(context.Context, string, int) (int64, error) { return 0, nil }}
	cases := []struct {
		name    string
		views   []string
		mat     *mockMaterializer
		buildID int
		want    string
	}{
		{"nil materializer", []string{"v"}, nil, 1, "materializer is required"},
		{"empty views", nil, ok, 1, "no projection views configured"},
		{"blank view entry", []string{"a", "  "}, ok, 1, "blank entry"},
		{"duplicate view", []string{"a", "b", "a"}, ok, 1, "configured twice"},
		{"negative buildID", []string{"a"}, ok, -1, "must not be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mat *mockMaterializer
			if tc.mat != nil {
				mat = tc.mat
			}
			var err error
			if mat == nil {
				_, err = NewService(tc.views, nil, tc.buildID, nil, nil)
			} else {
				_, err = NewService(tc.views, mat, tc.buildID, nil, nil)
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("NewService error = %v; want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestRunOnce_AllViewsInOrderWithReason(t *testing.T) {
	var builds []int
	mat := &mockMaterializer{fn: func(_ context.Context, _ string, buildID int) (int64, error) {
		builds = append(builds, buildID)
		return 3, nil
	}}
	svc, err := NewService([]string{"va", "vb", "vc"}, mat, 4711, nil, nil)
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
	mat := &mockMaterializer{fn: func(_ context.Context, view string, _ int) (int64, error) {
		if view == "vb" {
			return 0, boom
		}
		return 1, nil
	}}
	svc, err := NewService([]string{"va", "vb", "vc"}, mat, 4711, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	runErr := svc.RunOnce(context.Background())
	if runErr == nil {
		t.Fatal("RunOnce = nil; want the vb failure surfaced")
	}
	if !errors.Is(runErr, boom) || !strings.Contains(runErr.Error(), "view vb") {
		t.Errorf("RunOnce error = %v; want it to wrap the vb failure and name the view", runErr)
	}
	if got := strings.Join(mat.calls, ","); got != "va,vb,vc" {
		t.Errorf("calls = %s; want all three views attempted despite vb failing", got)
	}
}

func TestRunOnce_ParentCancellationAborts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mat := &mockMaterializer{fn: func(_ context.Context, view string, _ int) (int64, error) {
		if view == "va" {
			cancel() // cancellation arrives while the first view is running
		}
		return 1, nil
	}}
	svc, err := NewService([]string{"va", "vb"}, mat, 1, nil, nil)
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
	nilT.RecordRun(context.Background(), "v", "ok", 5) // must not panic

	tel, err := NewTelemetry() // global provider is a no-op meter in tests
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}
	tel.RecordRun(context.Background(), "v", "ok", 5)
	tel.RecordRun(context.Background(), "v", "error", 0)
}
