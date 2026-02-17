package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// AllocationEvent represents a token transfer involving an ALM proxy,
// enriched with the post-transfer on-chain balance.
type AllocationEvent struct {
	ChainID      int64
	BlockNumber  int64
	BlockVersion int
	TxHash       string
	LogIndex     int
	TokenAddress common.Address
	From         common.Address
	To           common.Address
	Amount       *big.Int
	Direction    Direction
	Star         string
	ProxyAddress common.Address

	// Post-transfer on-chain balance of the token held by the proxy
	Balance *big.Int

	// Token metadata (populated after fetch)
	TokenSymbol   string
	TokenDecimals int
	TokenName     string
}

// FormattedAmount returns the human-readable decimal-adjusted transfer amount.
func (e *AllocationEvent) FormattedAmount() string {
	return shared.FormatAmount(e.Amount, e.TokenDecimals)
}

// FormattedBalance returns the human-readable decimal-adjusted balance.
func (e *AllocationEvent) FormattedBalance() string {
	return shared.FormatAmount(e.Balance, e.TokenDecimals)
}

// AllocationHandler processes allocation events.
// Implement this interface to store events in a database, send notifications, etc.
type AllocationHandler interface {
	HandleAllocationChanges(ctx context.Context, events []*AllocationEvent) error
}

// LogHandler prints allocation events to the logger. Useful for development and debugging.
type LogHandler struct {
	logger *slog.Logger
}

func NewLogHandler(logger *slog.Logger) *LogHandler {
	return &LogHandler{logger: logger}
}

func (h *LogHandler) HandleAllocationChanges(ctx context.Context, events []*AllocationEvent) error {
	for _, e := range events {
		dirSymbol := "→"
		counterparty := e.To.Hex()
		if e.Direction == DirectionIn {
			dirSymbol = "←"
			counterparty = e.From.Hex()
		}

		symbol := e.TokenSymbol
		if symbol == "" {
			symbol = e.TokenAddress.Hex()[:10] + "..."
		}

		h.logger.Info("allocation change",
			"star", e.Star,
			"direction", string(e.Direction),
			"flow", fmt.Sprintf("%s %s %s %s", e.ProxyAddress.Hex()[:10], dirSymbol, e.FormattedAmount(), symbol),
			"balance", fmt.Sprintf("%s %s", e.FormattedBalance(), symbol),
			"counterparty", counterparty,
			"token", e.TokenAddress.Hex(),
			"block", e.BlockNumber,
			"tx", e.TxHash,
		)
	}
	return nil
}

// MultiHandler fans out events to multiple handlers.
type MultiHandler struct {
	handlers []AllocationHandler
}

func NewMultiHandler(handlers ...AllocationHandler) *MultiHandler {
	return &MultiHandler{handlers: handlers}
}

func (h *MultiHandler) HandleAllocationChanges(ctx context.Context, events []*AllocationEvent) error {
	var errs []error
	for _, handler := range h.handlers {
		if err := handler.HandleAllocationChanges(ctx, events); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("handler errors: %v", errs)
	}
	return nil
}
