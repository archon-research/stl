package morpho_indexer

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// ---------------------------------------------------------------------------
// Morpho Blue event types
// ---------------------------------------------------------------------------

// MorphoBlueEvent is implemented by all Morpho Blue event types.
type MorphoBlueEvent interface {
	Type() entity.MorphoEventType
	TxHash() string
	MarketID() [32]byte
	ToJSON() (json.RawMessage, error)
}

// morphoBlueBase holds the common fields shared by all Morpho Blue events.
type morphoBlueBase struct {
	marketID [32]byte
	txHash   string
}

func (b morphoBlueBase) TxHash() string     { return b.txHash }
func (b morphoBlueBase) MarketID() [32]byte { return b.marketID }

// CreateMarketEvent is emitted when a new Morpho Blue market is created.
type CreateMarketEvent struct {
	morphoBlueBase
	Params *MarketParams
}

func (e *CreateMarketEvent) Type() entity.MorphoEventType { return entity.MorphoEventCreateMarket }

func (e *CreateMarketEvent) ToJSON() (json.RawMessage, error) {
	data := map[string]any{
		"eventType": string(entity.MorphoEventCreateMarket),
		"marketId":  common.Bytes2Hex(e.marketID[:]),
	}
	if e.Params != nil {
		data["loanToken"] = e.Params.LoanToken.Hex()
		data["collateralToken"] = e.Params.CollateralToken.Hex()
		data["oracle"] = e.Params.Oracle.Hex()
		data["irm"] = e.Params.Irm.Hex()
		data["lltv"] = e.Params.LLTV.String()
	}
	return marshalEventJSON(data)
}

// SupplyEvent is emitted on Morpho Blue supply.
type SupplyEvent struct {
	morphoBlueBase
	Caller   common.Address
	OnBehalf common.Address
	Assets   *big.Int
	Shares   *big.Int
}

func (e *SupplyEvent) Type() entity.MorphoEventType { return entity.MorphoEventSupply }

func (e *SupplyEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType": string(entity.MorphoEventSupply),
		"marketId":  common.Bytes2Hex(e.marketID[:]),
		"caller":    e.Caller.Hex(),
		"onBehalf":  e.OnBehalf.Hex(),
		"assets":    e.Assets.String(),
		"shares":    e.Shares.String(),
	})
}

// WithdrawEvent is emitted on Morpho Blue withdraw.
type WithdrawEvent struct {
	morphoBlueBase
	Caller   common.Address
	OnBehalf common.Address
	Receiver common.Address
	Assets   *big.Int
	Shares   *big.Int
}

func (e *WithdrawEvent) Type() entity.MorphoEventType { return entity.MorphoEventWithdraw }

func (e *WithdrawEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType": string(entity.MorphoEventWithdraw),
		"marketId":  common.Bytes2Hex(e.marketID[:]),
		"caller":    e.Caller.Hex(),
		"onBehalf":  e.OnBehalf.Hex(),
		"receiver":  e.Receiver.Hex(),
		"assets":    e.Assets.String(),
		"shares":    e.Shares.String(),
	})
}

// BorrowEvent is emitted on Morpho Blue borrow.
type BorrowEvent struct {
	morphoBlueBase
	Caller   common.Address
	OnBehalf common.Address
	Receiver common.Address
	Assets   *big.Int
	Shares   *big.Int
}

func (e *BorrowEvent) Type() entity.MorphoEventType { return entity.MorphoEventBorrow }

func (e *BorrowEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType": string(entity.MorphoEventBorrow),
		"marketId":  common.Bytes2Hex(e.marketID[:]),
		"caller":    e.Caller.Hex(),
		"onBehalf":  e.OnBehalf.Hex(),
		"receiver":  e.Receiver.Hex(),
		"assets":    e.Assets.String(),
		"shares":    e.Shares.String(),
	})
}

// RepayEvent is emitted on Morpho Blue repay.
type RepayEvent struct {
	morphoBlueBase
	Caller   common.Address
	OnBehalf common.Address
	Assets   *big.Int
	Shares   *big.Int
}

func (e *RepayEvent) Type() entity.MorphoEventType { return entity.MorphoEventRepay }

func (e *RepayEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType": string(entity.MorphoEventRepay),
		"marketId":  common.Bytes2Hex(e.marketID[:]),
		"caller":    e.Caller.Hex(),
		"onBehalf":  e.OnBehalf.Hex(),
		"assets":    e.Assets.String(),
		"shares":    e.Shares.String(),
	})
}

// SupplyCollateralEvent is emitted on Morpho Blue supply collateral.
type SupplyCollateralEvent struct {
	morphoBlueBase
	Caller   common.Address
	OnBehalf common.Address
	Assets   *big.Int
}

func (e *SupplyCollateralEvent) Type() entity.MorphoEventType {
	return entity.MorphoEventSupplyCollateral
}

func (e *SupplyCollateralEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType": string(entity.MorphoEventSupplyCollateral),
		"marketId":  common.Bytes2Hex(e.marketID[:]),
		"caller":    e.Caller.Hex(),
		"onBehalf":  e.OnBehalf.Hex(),
		"assets":    e.Assets.String(),
	})
}

// WithdrawCollateralEvent is emitted on Morpho Blue withdraw collateral.
type WithdrawCollateralEvent struct {
	morphoBlueBase
	Caller   common.Address
	OnBehalf common.Address
	Receiver common.Address
	Assets   *big.Int
}

func (e *WithdrawCollateralEvent) Type() entity.MorphoEventType {
	return entity.MorphoEventWithdrawCollateral
}

func (e *WithdrawCollateralEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType": string(entity.MorphoEventWithdrawCollateral),
		"marketId":  common.Bytes2Hex(e.marketID[:]),
		"caller":    e.Caller.Hex(),
		"onBehalf":  e.OnBehalf.Hex(),
		"receiver":  e.Receiver.Hex(),
		"assets":    e.Assets.String(),
	})
}

// LiquidateEvent is emitted on Morpho Blue liquidation.
type LiquidateEvent struct {
	morphoBlueBase
	Caller        common.Address
	Borrower      common.Address
	RepaidAssets  *big.Int
	RepaidShares  *big.Int
	SeizedAssets  *big.Int
	BadDebtAssets *big.Int
	BadDebtShares *big.Int
}

func (e *LiquidateEvent) Type() entity.MorphoEventType { return entity.MorphoEventLiquidate }

func (e *LiquidateEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType":     string(entity.MorphoEventLiquidate),
		"marketId":      common.Bytes2Hex(e.marketID[:]),
		"caller":        e.Caller.Hex(),
		"borrower":      e.Borrower.Hex(),
		"repaidAssets":  e.RepaidAssets.String(),
		"repaidShares":  e.RepaidShares.String(),
		"seizedAssets":  e.SeizedAssets.String(),
		"badDebtAssets": e.BadDebtAssets.String(),
		"badDebtShares": e.BadDebtShares.String(),
	})
}

// AccrueInterestEvent is emitted when interest accrues on a Morpho Blue market.
type AccrueInterestEvent struct {
	morphoBlueBase
	PrevBorrowRate *big.Int
	Interest       *big.Int
	FeeShares      *big.Int
}

func (e *AccrueInterestEvent) Type() entity.MorphoEventType {
	return entity.MorphoEventAccrueInterest
}

func (e *AccrueInterestEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType":      string(entity.MorphoEventAccrueInterest),
		"marketId":       common.Bytes2Hex(e.marketID[:]),
		"prevBorrowRate": e.PrevBorrowRate.String(),
		"interest":       e.Interest.String(),
		"feeShares":      e.FeeShares.String(),
	})
}

// SetFeeEvent is emitted when the fee is updated on a Morpho Blue market.
type SetFeeEvent struct {
	morphoBlueBase
	NewFee *big.Int
}

func (e *SetFeeEvent) Type() entity.MorphoEventType { return entity.MorphoEventSetFee }

func (e *SetFeeEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType": string(entity.MorphoEventSetFee),
		"marketId":  common.Bytes2Hex(e.marketID[:]),
		"newFee":    e.NewFee.String(),
	})
}

// MarketParams holds the market parameters from a CreateMarket event.
type MarketParams struct {
	LoanToken       common.Address
	CollateralToken common.Address
	Oracle          common.Address
	Irm             common.Address
	LLTV            *big.Int
}

// abiMarketParams mirrors the ABI tuple field names for safe conversion via abi.ConvertType.
// Field names must match the capitalized ABI parameter names exactly (e.g. "lltv" → "Lltv").
type abiMarketParams struct {
	LoanToken       common.Address
	CollateralToken common.Address
	Oracle          common.Address
	Irm             common.Address
	Lltv            *big.Int
}

// ---------------------------------------------------------------------------
// MetaMorpho event types
// ---------------------------------------------------------------------------

// MetaMorphoEvent is implemented by all MetaMorpho vault event types.
type MetaMorphoEvent interface {
	Type() entity.MorphoEventType
	TxHash() string
	ToJSON() (json.RawMessage, error)
}

// metaMorphoBase holds the common fields shared by all MetaMorpho events.
type metaMorphoBase struct {
	txHash string
}

func (b metaMorphoBase) TxHash() string { return b.txHash }

// VaultDepositEvent is emitted on MetaMorpho vault deposit.
type VaultDepositEvent struct {
	metaMorphoBase
	Sender common.Address
	Owner  common.Address
	Assets *big.Int
	Shares *big.Int
}

func (e *VaultDepositEvent) Type() entity.MorphoEventType { return entity.MorphoEventVaultDeposit }

func (e *VaultDepositEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType": string(entity.MorphoEventVaultDeposit),
		"sender":    e.Sender.Hex(),
		"owner":     e.Owner.Hex(),
		"assets":    e.Assets.String(),
		"shares":    e.Shares.String(),
	})
}

// VaultWithdrawEvent is emitted on MetaMorpho vault withdraw.
type VaultWithdrawEvent struct {
	metaMorphoBase
	Sender   common.Address
	Receiver common.Address
	Owner    common.Address
	Assets   *big.Int
	Shares   *big.Int
}

func (e *VaultWithdrawEvent) Type() entity.MorphoEventType { return entity.MorphoEventVaultWithdraw }

func (e *VaultWithdrawEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType": string(entity.MorphoEventVaultWithdraw),
		"sender":    e.Sender.Hex(),
		"receiver":  e.Receiver.Hex(),
		"owner":     e.Owner.Hex(),
		"assets":    e.Assets.String(),
		"shares":    e.Shares.String(),
	})
}

// VaultTransferEvent is emitted on MetaMorpho vault share transfer.
type VaultTransferEvent struct {
	metaMorphoBase
	From  common.Address
	To    common.Address
	Value *big.Int
}

func (e *VaultTransferEvent) Type() entity.MorphoEventType { return entity.MorphoEventVaultTransfer }

func (e *VaultTransferEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType": string(entity.MorphoEventVaultTransfer),
		"from":      e.From.Hex(),
		"to":        e.To.Hex(),
		"value":     e.Value.String(),
	})
}

// VaultAccrueInterestEvent is emitted when interest accrues on a MetaMorpho vault.
type VaultAccrueInterestEvent struct {
	metaMorphoBase
	NewTotalAssets      *big.Int
	FeeShares           *big.Int
	PreviousTotalAssets *big.Int // V2 only
	ManagementFeeShares *big.Int // V2 only
}

func (e *VaultAccrueInterestEvent) Type() entity.MorphoEventType {
	return entity.MorphoEventVaultAccrueInterest
}

func (e *VaultAccrueInterestEvent) ToJSON() (json.RawMessage, error) {
	data := map[string]any{
		"eventType":      string(entity.MorphoEventVaultAccrueInterest),
		"newTotalAssets": e.NewTotalAssets.String(),
		"feeShares":      e.FeeShares.String(),
	}
	if e.PreviousTotalAssets != nil {
		data["previousTotalAssets"] = e.PreviousTotalAssets.String()
	}
	if e.ManagementFeeShares != nil {
		data["managementFeeShares"] = e.ManagementFeeShares.String()
	}
	return marshalEventJSON(data)
}

func marshalEventJSON(data map[string]any) (json.RawMessage, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event data: %w", err)
	}
	return raw, nil
}
