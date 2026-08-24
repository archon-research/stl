package uniswapv4bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// windowPolicy is the adaptive sizing rule for the log scan: a provider's real
// ceiling depends on how dense the logs are in a given stretch of history, so
// the scan discovers it per stretch instead of crawling at a fixed safe size.
type windowPolicy struct {
	initial int64
	min     int64
	max     int64
}

// shrink halves a refused window, never below min. Halving (rather than
// stepping down to the range some providers suggest in the refusal text) keeps
// the policy provider-independent and reaches any ceiling in log2 attempts.
func (p windowPolicy) shrink(size int64) int64 {
	return max(size/2, p.min)
}

// grow widens a window by a quarter after a success, capped at max. Doubling
// would re-refuse on the very next window after a bisect and pay a wasted
// request every time; a quarter takes a few successes to climb back, so a
// stretch of dense history costs roughly one wasted request in three.
func (p windowPolicy) grow(size int64) int64 {
	grown := size + max(size/4, 1)
	return min(grown, p.max)
}

// logWindow is one window's result: the range queried and the logs it returned.
type logWindow struct {
	from int64
	to   int64
	logs []outbound.FilteredLog
}

// scanStats is what a run reports about the scan itself, so an operator reading
// the log can tell a quiet range from a scan that never widened.
type scanStats struct {
	windows    int
	narrowings int
	logs       int
}

// logWindowScanner walks a block range in adaptive windows, handing each
// window's logs to a callback. filter carries everything but the range.
type logWindowScanner struct {
	client outbound.LogScanClient
	filter outbound.LogFilter
	policy windowPolicy
	logger *slog.Logger
}

// scan walks [from, to] inclusive, emitting each window's logs in ascending
// block order. A range refusal narrows the window and retries the SAME start,
// so no block is ever skipped; a refusal that survives down to the minimum
// window, any other RPC failure, and any emit failure all stop the scan with an
// error — a partial scan would silently omit positions no rerun could find.
func (s *logWindowScanner) scan(ctx context.Context, from, to int64, emit func(logWindow) error) (scanStats, error) {
	var stats scanStats
	size := s.policy.initial

	for cursor := from; cursor <= to; {
		if err := ctx.Err(); err != nil {
			return stats, err
		}

		end := min(cursor+size-1, to)
		logs, err := s.client.GetLogs(ctx, s.rangeFilter(cursor, end))
		if err != nil {
			// Shrink from the span actually requested, not the nominal window:
			// on the clamped last window the two differ, and halving the nominal
			// one re-requests the identical range until it drops below the clamp.
			narrowed, retryErr := s.narrow(end-cursor+1, cursor, end, err)
			if retryErr != nil {
				return stats, retryErr
			}
			size = narrowed
			stats.narrowings++
			continue
		}

		if err := emit(logWindow{from: cursor, to: end, logs: logs}); err != nil {
			return stats, fmt.Errorf("handling logs for blocks %d-%d: %w", cursor, end, err)
		}
		stats.windows++
		stats.logs += len(logs)
		s.logger.Debug("uniswap-v4 position scan window",
			"fromBlock", cursor, "toBlock", end, "windowBlocks", end-cursor+1, "logs", len(logs),
			"windowsDone", stats.windows, "logsTotal", stats.logs, "scanToBlock", to)

		cursor = end + 1
		size = s.policy.grow(size)
	}
	return stats, nil
}

// narrow returns the next window size for a refused query, or an error when the
// failure is not a range refusal or the window is already at the minimum.
func (s *logWindowScanner) narrow(size, from, to int64, err error) (int64, error) {
	if !errors.Is(err, outbound.ErrLogRangeTooLarge) {
		return 0, fmt.Errorf("scanning blocks %d-%d for uniswap-v4 ModifyLiquidity logs: %w", from, to, err)
	}
	if size <= s.policy.min {
		return 0, fmt.Errorf("blocks %d-%d refused at the minimum window of %d blocks; a single block's logs exceed the provider's response limit: %w",
			from, to, s.policy.min, err)
	}
	narrowed := s.policy.shrink(size)
	s.logger.Info("narrowing the uniswap-v4 position scan window",
		"fromBlock", from, "refusedToBlock", to, "refusedWindowBlocks", size, "nextWindowBlocks", narrowed, "error", err)
	return narrowed, nil
}

func (s *logWindowScanner) rangeFilter(from, to int64) outbound.LogFilter {
	filter := s.filter
	filter.FromBlock = from
	filter.ToBlock = to
	return filter
}
