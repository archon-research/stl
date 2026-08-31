package uniswapv4bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type windowPolicy struct {
	initial int64
	min     int64
	max     int64
}

func (p windowPolicy) shrink(size int64) int64 {
	return max(size/2, p.min)
}

// Doubling would re-refuse on the window right after a bisect and pay a wasted
// request each time; a quarter climbs back over a few successes instead.
func (p windowPolicy) grow(size int64) int64 {
	grown := size + max(size/4, 1)
	return min(grown, p.max)
}

type logWindow struct {
	from int64
	to   int64
	logs []outbound.FilteredLog
}

type scanStats struct {
	windows    int
	narrowings int
	logs       int
}

type logWindowScanner struct {
	client outbound.LogScanClient
	filter outbound.LogFilter
	policy windowPolicy
	logger *slog.Logger
}

// A refusal narrows the window and retries the SAME start, so no block is ever
// skipped; every other failure stops the scan, because a partial one omits
// positions no rerun would look for again.
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
			// Halving the nominal window instead of the span actually requested
			// re-requests the identical clamped range until it drops below the clamp.
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
