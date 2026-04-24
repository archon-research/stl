package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
)

type LogHandler struct {
	logger *slog.Logger
}

func NewLogHandler(logger *slog.Logger) *LogHandler {
	return &LogHandler{logger: logger.With("component", "log-handler")}
}

func (h *LogHandler) HandleBatch(ctx context.Context, batch *SnapshotBatch) error {
	if batch == nil {
		return nil
	}
	for _, s := range batch.Snapshots {
		fields := []any{
			"star", s.Entry.Star,
			"chain", s.Entry.Chain,
			"protocol", s.Entry.Protocol,
			"type", s.Entry.TokenType,
			"contract", s.Entry.ContractAddress.Hex(),
			"balance_raw", s.Balance.String(),
			"block", s.BlockNumber,
		}

		if s.ScaledBalance != nil {
			fields = append(fields, "scaled_raw", s.ScaledBalance.String())
		}

		if s.TxHash != "" {
			fields = append(fields,
				"direction", string(s.Direction),
				"tx_amount_raw", s.TxAmount.String(),
				"tx", s.TxHash,
			)
		}

		h.logger.Info("position snapshot", fields...)
	}
	for _, sup := range batch.Supplies {
		fields := []any{
			"chain", sup.ChainID,
			"contract", sup.TokenAddress.Hex(),
			"total_supply_raw", sup.TotalSupply.String(),
			"block", sup.BlockNumber,
			"source", sup.Source,
		}
		if sup.ScaledTotalSupply != nil {
			fields = append(fields, "scaled_total_supply_raw", sup.ScaledTotalSupply.String())
		}
		h.logger.Info("supply snapshot", fields...)
	}
	return nil
}

type MultiHandler struct {
	handlers []AllocationHandler
}

func NewMultiHandler(handlers ...AllocationHandler) *MultiHandler {
	return &MultiHandler{handlers: handlers}
}

func (h *MultiHandler) HandleBatch(ctx context.Context, batch *SnapshotBatch) error {
	var errs []error
	for _, handler := range h.handlers {
		if err := handler.HandleBatch(ctx, batch); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("handler errors: %v", errs)
	}
	return nil
}
