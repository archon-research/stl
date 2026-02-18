package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// LogHandler prints position snapshots to the logger.
type LogHandler struct {
	logger *slog.Logger
}

func NewLogHandler(logger *slog.Logger) *LogHandler {
	return &LogHandler{logger: logger.With("component", "log-handler")}
}

func (h *LogHandler) HandleSnapshots(ctx context.Context, snapshots []*PositionSnapshot) error {
	for _, s := range snapshots {
		decimals := 18
		balanceStr := shared.FormatAmount(s.Balance, decimals)

		fields := []any{
			"star", s.Entry.Star,
			"chain", s.Entry.Chain,
			"protocol", s.Entry.Protocol,
			"type", s.Entry.TokenType,
			"contract", s.Entry.ContractAddress.Hex(),
			"balance", balanceStr,
			"block", s.BlockNumber,
		}

		if s.ScaledBalance != nil {
			fields = append(fields, "scaled", shared.FormatAmount(s.ScaledBalance, decimals))
		}

		if s.TxHash != "" {
			fields = append(fields,
				"direction", string(s.Direction),
				"tx_amount", shared.FormatAmount(s.TxAmount, decimals),
				"tx", s.TxHash,
			)
		}

		h.logger.Info("position snapshot", fields...)
	}
	return nil
}

// MultiHandler fans out to multiple handlers.
type MultiHandler struct {
	handlers []AllocationHandler
}

func NewMultiHandler(handlers ...AllocationHandler) *MultiHandler {
	return &MultiHandler{handlers: handlers}
}

func (h *MultiHandler) HandleSnapshots(ctx context.Context, snapshots []*PositionSnapshot) error {
	var errs []error
	for _, handler := range h.handlers {
		if err := handler.HandleSnapshots(ctx, snapshots); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("handler errors: %v", errs)
	}
	return nil
}
