package outbound

import (
	"context"
	"time"
)

// PositionDailyCrystallizer writes each settled UTC day's winning position_state
// observation into position_daily_observation (VEC-636).
//
// The work is one statement in the database: crystallize_position_daily() recomputes
// the winner per (position, UTC date) over the spine and offers it with ON CONFLICT
// DO NOTHING, so a day whose answer has not changed costs a scan and writes nothing.
// That is what makes a tick safe to miss, repeat or retry, which Temporal requires.
type PositionDailyCrystallizer interface {
	// Crystallize runs one pass and returns the number of rows written. Zero is the
	// normal result: it means every settled day already held its winning observation.
	// settleAfter holds back days that have only just closed.
	Crystallize(ctx context.Context, settleAfter time.Duration) (int64, error)
}
