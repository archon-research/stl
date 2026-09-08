package main

import (
	"context"
	"fmt"
	"log/slog"
)

// finalizedHeadReader reports the height the node considers final.
type finalizedHeadReader interface {
	GetFinalizedBlockNumber(ctx context.Context) (int64, error)
}

// guardFinality refuses a range reaching into the unfinalized head. A losing
// fork archived there can never be corrected: the watcher re-publishes the
// winner at a lower version, and WriteFileIfNotExists keeps the fork.
func guardFinality(ctx context.Context, node finalizedHeadReader, cfg Config, logger *slog.Logger) error {
	if cfg.AllowUnfinalized {
		logger.Warn("archiving past the finalized head on request",
			"endBlock", cfg.EndBlock,
			"flag", "--allow-unfinalized",
			"risk", "a height that loses its fork stays wrong: the watcher's re-publish lands at a lower version",
		)
		return nil
	}

	finalized, err := node.GetFinalizedBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("reading the finalized head the range is checked against: %w; the node must serve the \"finalized\" block tag, or pass --allow-unfinalized to archive without the check", err)
	}
	if cfg.EndBlock > finalized {
		return fmt.Errorf("--end-block %d is above the finalized head %d: a height that loses its fork up there can never be corrected, because the watcher's re-publish lands at a lower version; lower --end-block to %d or pass --allow-unfinalized", cfg.EndBlock, finalized, finalized)
	}

	logger.Info("range is final", "endBlock", cfg.EndBlock, "finalizedHead", finalized)
	return nil
}
