package position_daily_crystallizer

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// stubCrystallizer records what the service asked for and returns what it is told to.
type stubCrystallizer struct {
	calls      int
	gotSettle  time.Duration
	gotCtx     context.Context
	returnRows int64
	returnErr  error
}

func (s *stubCrystallizer) Crystallize(ctx context.Context, settleAfter time.Duration) (int64, error) {
	s.calls++
	s.gotSettle = settleAfter
	s.gotCtx = ctx
	return s.returnRows, s.returnErr
}

var _ outbound.PositionDailyCrystallizer = (*stubCrystallizer)(nil)

func mustService(t *testing.T, c outbound.PositionDailyCrystallizer, settleAfter time.Duration) *Service {
	t.Helper()
	svc, err := NewService(c, settleAfter, slog.Default(), nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

func TestNewServiceRefusesBadConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name         string
		crystallizer outbound.PositionDailyCrystallizer
		settleAfter  time.Duration
		wantErr      string
	}{
		{name: "a crystallizer is required", settleAfter: time.Hour,
			wantErr: "position daily crystallizer is required"},
		{name: "a negative window is refused", crystallizer: &stubCrystallizer{},
			settleAfter: -time.Second, wantErr: "must not be negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, err := NewService(tc.crystallizer, tc.settleAfter, nil, nil)
			if err == nil {
				t.Fatalf("NewService succeeded; want an error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, tc.wantErr)
			}
			if svc != nil {
				t.Error("a failed NewService returned a service")
			}
		})
	}
}

func TestNewServiceAcceptsValidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name        string
		settleAfter time.Duration
		logger      *slog.Logger
	}{
		{name: "zero means crystallize as soon as the day closes"},
		{name: "a nil logger is defaulted", settleAfter: time.Hour},
		{name: "a logger is used as given", settleAfter: time.Hour, logger: slog.Default()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, err := NewService(&stubCrystallizer{}, tc.settleAfter, tc.logger, nil)
			if err != nil {
				t.Fatalf("NewService: %v", err)
			}
			if svc.logger == nil {
				t.Error("logger is nil; it must default")
			}
			if svc.settleAfter != tc.settleAfter {
				t.Errorf("settleAfter = %s, want %s", svc.settleAfter, tc.settleAfter)
			}
		})
	}
}

func TestRunOncePassesTheConfiguredWindow(t *testing.T) {
	stub := &stubCrystallizer{returnRows: 3}
	if err := mustService(t, stub, 90*time.Minute).RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if stub.calls != 1 {
		t.Errorf("crystallized %d times, want 1", stub.calls)
	}
	if stub.gotSettle != 90*time.Minute {
		t.Errorf("settle window = %s, want 90m", stub.gotSettle)
	}
}

// Writing nothing is the steady state, not a fault: the pass recomputes every settled
// day and only writes where the answer changed.
func TestRunOnceTreatsNoRowsAsSuccess(t *testing.T) {
	if err := mustService(t, &stubCrystallizer{returnRows: 0}, time.Hour).RunOnce(context.Background()); err != nil {
		t.Errorf("a run that wrote no rows returned %v; want success", err)
	}
}

func TestRunOnceReturnsAWrappedFailure(t *testing.T) {
	sentinel := errors.New("connection refused")
	err := mustService(t, &stubCrystallizer{returnErr: sentinel}, time.Hour).RunOnce(context.Background())
	if err == nil {
		t.Fatal("RunOnce succeeded despite the crystallizer failing")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error %v does not wrap the cause; the caller cannot tell why the tick failed", err)
	}
	if !strings.Contains(err.Error(), "crystallizing position_daily") {
		t.Errorf("error %q carries no context about what failed", err)
	}
}

// Temporal cancels a tick that overruns its activity timeout; the context must reach
// the statement so it stops rather than running on past the cancellation.
func TestRunOncePassesTheCallersContext(t *testing.T) {
	type ctxKey struct{}
	stub := &stubCrystallizer{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "marker")
	if err := mustService(t, stub, time.Hour).RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if stub.gotCtx.Value(ctxKey{}) != "marker" {
		t.Error("the crystallizer was called with a different context; a cancelled tick would not stop the statement")
	}
}

// A nil *Telemetry is the documented fallback for a worker that could not build
// instruments, so both paths must run without it.
func TestRunOnceWorksWithoutTelemetry(t *testing.T) {
	ok := mustService(t, &stubCrystallizer{returnRows: 1}, time.Hour)
	ok.telemetry = nil
	if err := ok.RunOnce(context.Background()); err != nil {
		t.Errorf("success path with nil telemetry: %v", err)
	}
	bad := mustService(t, &stubCrystallizer{returnErr: errors.New("boom")}, time.Hour)
	bad.telemetry = nil
	if err := bad.RunOnce(context.Background()); err == nil {
		t.Error("failure path with nil telemetry returned success")
	}
}

// And with real instruments, so the recording paths execute rather than being skipped
// by the nil guard above.
func TestRunOnceRecordsThroughRealInstruments(t *testing.T) {
	telemetry, err := NewTelemetry()
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}
	ok, err := NewService(&stubCrystallizer{returnRows: 2}, time.Hour, nil, telemetry)
	if err != nil {
		t.Fatal(err)
	}
	if err := ok.RunOnce(context.Background()); err != nil {
		t.Errorf("success path: %v", err)
	}
	failing, err := NewService(&stubCrystallizer{returnErr: errors.New("boom")}, time.Hour, nil, telemetry)
	if err != nil {
		t.Fatal(err)
	}
	if err := failing.RunOnce(context.Background()); err == nil {
		t.Error("failure path returned success")
	}
}
