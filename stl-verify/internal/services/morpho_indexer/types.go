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

// ---------------------------------------------------------------------------
// Morpho VaultV2 adapter / allocation / cap / fee event types
// ---------------------------------------------------------------------------
//
// These are emitted only by VaultV2 vaults (morpho-org/vault-v2). Field names
// mirror EventsLib.sol; indexed params are decoded from topics, the rest from
// data. See event_extractor.go for the extraction and service.go for the
// structured handlers that consume them.

// AddAdapterEvent is emitted when a liquidity adapter is registered on a VaultV2.
type AddAdapterEvent struct {
	metaMorphoBase
	Account common.Address
}

func (e *AddAdapterEvent) Type() entity.MorphoEventType { return entity.MorphoEventVaultAddAdapter }

func (e *AddAdapterEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType": string(entity.MorphoEventVaultAddAdapter),
		"account":   e.Account.Hex(),
	})
}

// RemoveAdapterEvent is emitted when a liquidity adapter is removed from a VaultV2.
type RemoveAdapterEvent struct {
	metaMorphoBase
	Account common.Address
}

func (e *RemoveAdapterEvent) Type() entity.MorphoEventType {
	return entity.MorphoEventVaultRemoveAdapter
}

func (e *RemoveAdapterEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType": string(entity.MorphoEventVaultRemoveAdapter),
		"account":   e.Account.Hex(),
	})
}

// AllocateEvent is emitted when a VaultV2 allocates assets into an adapter.
// Change is a signed per-id delta, not a running total; the adapter's current
// value is read from realAssets() by the handler.
type AllocateEvent struct {
	metaMorphoBase
	Sender  common.Address
	Adapter common.Address
	Assets  *big.Int
	IDs     []common.Hash
	Change  *big.Int
}

func (e *AllocateEvent) Type() entity.MorphoEventType { return entity.MorphoEventVaultAllocate }

func (e *AllocateEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType": string(entity.MorphoEventVaultAllocate),
		"sender":    e.Sender.Hex(),
		"adapter":   e.Adapter.Hex(),
		"assets":    e.Assets.String(),
		"ids":       hashesToHex(e.IDs),
		"change":    e.Change.String(),
	})
}

// DeallocateEvent is emitted when a VaultV2 withdraws assets from an adapter.
// Shares AllocateEvent's shape; Change is a signed per-id delta.
type DeallocateEvent struct {
	metaMorphoBase
	Sender  common.Address
	Adapter common.Address
	Assets  *big.Int
	IDs     []common.Hash
	Change  *big.Int
}

func (e *DeallocateEvent) Type() entity.MorphoEventType { return entity.MorphoEventVaultDeallocate }

func (e *DeallocateEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType": string(entity.MorphoEventVaultDeallocate),
		"sender":    e.Sender.Hex(),
		"adapter":   e.Adapter.Hex(),
		"assets":    e.Assets.String(),
		"ids":       hashesToHex(e.IDs),
		"change":    e.Change.String(),
	})
}

// ForceDeallocateEvent is emitted on a sentinel-triggered emergency exit. The
// contract's forceDeallocate() calls the shared internal deallocate path
// (deallocateInternal), which emits the Deallocate event, so a companion
// Deallocate log accompanies every ForceDeallocate in the same transaction.
type ForceDeallocateEvent struct {
	metaMorphoBase
	Sender        common.Address
	Adapter       common.Address
	Assets        *big.Int
	OnBehalf      common.Address
	IDs           []common.Hash
	PenaltyAssets *big.Int
}

func (e *ForceDeallocateEvent) Type() entity.MorphoEventType {
	return entity.MorphoEventVaultForceDeallocate
}

func (e *ForceDeallocateEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType":     string(entity.MorphoEventVaultForceDeallocate),
		"sender":        e.Sender.Hex(),
		"adapter":       e.Adapter.Hex(),
		"assets":        e.Assets.String(),
		"onBehalf":      e.OnBehalf.Hex(),
		"ids":           hashesToHex(e.IDs),
		"penaltyAssets": e.PenaltyAssets.String(),
	})
}

// IncreaseAbsoluteCapEvent raises the absolute allocation cap for a cap id.
// ID = keccak256(IDData) is the canonical cap key; IDData is its pre-image.
type IncreaseAbsoluteCapEvent struct {
	metaMorphoBase
	ID             common.Hash
	IDData         []byte
	NewAbsoluteCap *big.Int
}

func (e *IncreaseAbsoluteCapEvent) Type() entity.MorphoEventType {
	return entity.MorphoEventVaultIncreaseAbsoluteCap
}

func (e *IncreaseAbsoluteCapEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType":      string(entity.MorphoEventVaultIncreaseAbsoluteCap),
		"id":             e.ID.Hex(),
		"idData":         common.Bytes2Hex(e.IDData),
		"newAbsoluteCap": e.NewAbsoluteCap.String(),
	})
}

// DecreaseAbsoluteCapEvent lowers the absolute allocation cap for a cap id.
// Unlike IncreaseAbsoluteCap it carries the sender that submitted the change.
type DecreaseAbsoluteCapEvent struct {
	metaMorphoBase
	Sender         common.Address
	ID             common.Hash
	IDData         []byte
	NewAbsoluteCap *big.Int
}

func (e *DecreaseAbsoluteCapEvent) Type() entity.MorphoEventType {
	return entity.MorphoEventVaultDecreaseAbsoluteCap
}

func (e *DecreaseAbsoluteCapEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType":      string(entity.MorphoEventVaultDecreaseAbsoluteCap),
		"sender":         e.Sender.Hex(),
		"id":             e.ID.Hex(),
		"idData":         common.Bytes2Hex(e.IDData),
		"newAbsoluteCap": e.NewAbsoluteCap.String(),
	})
}

// IncreaseRelativeCapEvent raises the relative (WAD-fraction) allocation cap.
type IncreaseRelativeCapEvent struct {
	metaMorphoBase
	ID             common.Hash
	IDData         []byte
	NewRelativeCap *big.Int
}

func (e *IncreaseRelativeCapEvent) Type() entity.MorphoEventType {
	return entity.MorphoEventVaultIncreaseRelativeCap
}

func (e *IncreaseRelativeCapEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType":      string(entity.MorphoEventVaultIncreaseRelativeCap),
		"id":             e.ID.Hex(),
		"idData":         common.Bytes2Hex(e.IDData),
		"newRelativeCap": e.NewRelativeCap.String(),
	})
}

// DecreaseRelativeCapEvent lowers the relative (WAD-fraction) allocation cap.
// Unlike IncreaseRelativeCap it carries the sender that submitted the change.
type DecreaseRelativeCapEvent struct {
	metaMorphoBase
	Sender         common.Address
	ID             common.Hash
	IDData         []byte
	NewRelativeCap *big.Int
}

func (e *DecreaseRelativeCapEvent) Type() entity.MorphoEventType {
	return entity.MorphoEventVaultDecreaseRelativeCap
}

func (e *DecreaseRelativeCapEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType":      string(entity.MorphoEventVaultDecreaseRelativeCap),
		"sender":         e.Sender.Hex(),
		"id":             e.ID.Hex(),
		"idData":         common.Bytes2Hex(e.IDData),
		"newRelativeCap": e.NewRelativeCap.String(),
	})
}

// SetPerformanceFeeEvent updates a VaultV2's performance fee (uint96 WAD
// fraction of accrued interest).
type SetPerformanceFeeEvent struct {
	metaMorphoBase
	NewPerformanceFee *big.Int
}

func (e *SetPerformanceFeeEvent) Type() entity.MorphoEventType {
	return entity.MorphoEventVaultSetPerformanceFee
}

func (e *SetPerformanceFeeEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType":         string(entity.MorphoEventVaultSetPerformanceFee),
		"newPerformanceFee": e.NewPerformanceFee.String(),
	})
}

// SetManagementFeeEvent updates a VaultV2's management fee (uint96 WAD
// per-second rate).
type SetManagementFeeEvent struct {
	metaMorphoBase
	NewManagementFee *big.Int
}

func (e *SetManagementFeeEvent) Type() entity.MorphoEventType {
	return entity.MorphoEventVaultSetManagementFee
}

func (e *SetManagementFeeEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType":        string(entity.MorphoEventVaultSetManagementFee),
		"newManagementFee": e.NewManagementFee.String(),
	})
}

// SetPerformanceFeeRecipientEvent updates who receives performance-fee shares.
type SetPerformanceFeeRecipientEvent struct {
	metaMorphoBase
	NewPerformanceFeeRecipient common.Address
}

func (e *SetPerformanceFeeRecipientEvent) Type() entity.MorphoEventType {
	return entity.MorphoEventVaultSetPerformanceFeeRecipient
}

func (e *SetPerformanceFeeRecipientEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType":                  string(entity.MorphoEventVaultSetPerformanceFeeRecipient),
		"newPerformanceFeeRecipient": e.NewPerformanceFeeRecipient.Hex(),
	})
}

// SetManagementFeeRecipientEvent updates who receives management-fee shares.
type SetManagementFeeRecipientEvent struct {
	metaMorphoBase
	NewManagementFeeRecipient common.Address
}

func (e *SetManagementFeeRecipientEvent) Type() entity.MorphoEventType {
	return entity.MorphoEventVaultSetManagementFeeRecipient
}

func (e *SetManagementFeeRecipientEvent) ToJSON() (json.RawMessage, error) {
	return marshalEventJSON(map[string]any{
		"eventType":                 string(entity.MorphoEventVaultSetManagementFeeRecipient),
		"newManagementFeeRecipient": e.NewManagementFeeRecipient.Hex(),
	})
}

// hashesToHex renders a bytes32 id array as hex strings for JSON snapshots.
func hashesToHex(hashes []common.Hash) []string {
	out := make([]string, len(hashes))
	for i, h := range hashes {
		out[i] = h.Hex()
	}
	return out
}

func marshalEventJSON(data map[string]any) (json.RawMessage, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event data: %w", err)
	}
	return raw, nil
}
