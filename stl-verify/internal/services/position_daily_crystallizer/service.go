// Package position_daily_crystallizer runs position_daily's daily write on a schedule.
// Each tick recomputes every settled UTC day's winning observation over position_state
// and writes the ones that are missing; the pick and the conflict handling live in the
// database procedure, so this service is the scheduler around it.
package position_daily_crystallizer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Service crystallizes position_daily once per RunOnce.
type Service struct {
	crystallizer outbound.PositionDailyCrystallizer
	settleAfter  time.Duration
	logger       *slog.Logger
	telemetry    *Telemetry
}

// NewService creates a Service. crystallizer is required; settleAfter must not be
// negative, which would crystallize the current UTC day and append a row every time
// its answer moved; logger defaults to slog.Default(); telemetry may be nil.
func NewService(crystallizer outbound.PositionDailyCrystallizer, settleAfter time.Duration, logger *slog.Logger, telemetry *Telemetry) (*Service, error) {
	if crystallizer == nil {
		return nil, fmt.Errorf("position daily crystallizer is required")
	}
	if settleAfter < 0 {
		return nil, fmt.Errorf("settle after must not be negative, got %s: a negative window crystallizes the open UTC day", settleAfter)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		crystallizer: crystallizer,
		settleAfter:  settleAfter,
		logger:       logger.With("component", "position-daily-crystallizer"),
		telemetry:    telemetry,
	}, nil
}

// RunOnce crystallizes every settled UTC day once.
//
// Writing nothing is the normal outcome and is reported as success: the pass recomputes
// each settled day's winner and only writes where the answer has changed, so a steady
// state is zero rows. A failure is returned rather than swallowed, so Temporal retries
// the tick; the pass is idempotent, so a retry repeats the scan and not the writes.
func (s *Service) RunOnce(ctx context.Context) error {
	start := time.Now()
	appended, err := s.crystallizer.Crystallize(ctx, s.settleAfter)
	elapsed := time.Since(start)
	if err != nil {
		s.telemetry.RecordFailure(ctx)
		return fmt.Errorf("crystallizing position_daily: %w", err)
	}

	s.telemetry.RecordRun(ctx, appended, elapsed)
	s.logger.InfoContext(ctx, "crystallized position_daily",
		"rows_written", appended, "settle_after", s.settleAfter, "duration", elapsed)
	return nil
}
