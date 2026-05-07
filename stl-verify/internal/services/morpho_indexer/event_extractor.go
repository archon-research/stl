package morpho_indexer

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// ---------------------------------------------------------------------------
// EventExtractor
// ---------------------------------------------------------------------------

// EventExtractor parses Morpho Blue and MetaMorpho events from transaction logs.
type EventExtractor struct {
	morphoBlueABI        *abi.ABI
	morphoBlueSignatures map[common.Hash]*abi.Event
	metaMorphoABI        *abi.ABI
	metaMorphoSignatures map[common.Hash]*abi.Event
}

// NewEventExtractor creates a new EventExtractor with loaded ABIs.
func NewEventExtractor() (*EventExtractor, error) {
	e := &EventExtractor{
		morphoBlueSignatures: make(map[common.Hash]*abi.Event),
		metaMorphoSignatures: make(map[common.Hash]*abi.Event),
	}
	if err := e.loadABIs(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *EventExtractor) loadABIs() error {
	morphoABI, err := abis.GetMorphoBlueEventsABI()
	if err != nil {
		return fmt.Errorf("failed to parse Morpho Blue events ABI: %w", err)
	}
	e.morphoBlueABI = morphoABI

	morphoEventNames := []string{
		"CreateMarket", "Supply", "Withdraw", "Borrow", "Repay",
		"SupplyCollateral", "WithdrawCollateral", "Liquidate", "AccrueInterest", "SetFee",
	}
	for _, name := range morphoEventNames {
		event, ok := e.morphoBlueABI.Events[name]
		if !ok {
			return fmt.Errorf("morpho Blue %s event not found in ABI", name)
		}
		e.morphoBlueSignatures[event.ID] = &event
	}

	metaMorphoABI, err := abis.GetMetaMorphoV1EventsABI()
	if err != nil {
		return fmt.Errorf("failed to parse MetaMorpho events ABI: %w", err)
	}
	e.metaMorphoABI = metaMorphoABI

	metaMorphoEventNames := []string{"Deposit", "Withdraw", "Transfer", "AccrueInterest"}
	for _, name := range metaMorphoEventNames {
		event, ok := e.metaMorphoABI.Events[name]
		if !ok {
			return fmt.Errorf("metaMorpho %s event not found in ABI", name)
		}
		e.metaMorphoSignatures[event.ID] = &event
	}

	// Register V2 AccrueInterest (different topic hash due to different signature)
	v2ABI, err := abis.GetMetaMorphoV2AccrueInterestABI()
	if err != nil {
		return fmt.Errorf("failed to parse MetaMorpho V2 AccrueInterest ABI: %w", err)
	}
	v2Event := v2ABI.Events["AccrueInterest"]
	e.metaMorphoSignatures[v2Event.ID] = &v2Event

	return nil
}

// IsMorphoBlueEvent returns true if the log matches a known Morpho Blue event.
func (e *EventExtractor) IsMorphoBlueEvent(log shared.Log) bool {
	if len(log.Topics) == 0 {
		return false
	}
	_, ok := e.morphoBlueSignatures[common.HexToHash(log.Topics[0])]
	return ok
}

// IsMetaMorphoEvent returns true if the log matches a known MetaMorpho event.
func (e *EventExtractor) IsMetaMorphoEvent(log shared.Log) bool {
	if len(log.Topics) == 0 {
		return false
	}
	_, ok := e.metaMorphoSignatures[common.HexToHash(log.Topics[0])]
	return ok
}

// ExtractMorphoBlueEvent parses a Morpho Blue event from a log entry.
func (e *EventExtractor) ExtractMorphoBlueEvent(log shared.Log) (MorphoBlueEvent, error) {
	if len(log.Topics) == 0 {
		return nil, fmt.Errorf("no topics")
	}

	eventSig := common.HexToHash(log.Topics[0])
	event, ok := e.morphoBlueSignatures[eventSig]
	if !ok {
		return nil, fmt.Errorf("not a tracked Morpho Blue event")
	}

	eventData := make(map[string]any)

	if err := parseTopics(event, log.Topics, eventData); err != nil {
		return nil, fmt.Errorf("parsing indexed params for %s: %w", event.Name, err)
	}
	if err := parseData(event, log.Data, eventData); err != nil {
		return nil, fmt.Errorf("parsing non-indexed params for %s: %w", event.Name, err)
	}

	switch event.Name {
	case "CreateMarket":
		return extractCreateMarket(eventData, log.TransactionHash)
	case "Supply":
		return extractSupply(eventData, log.TransactionHash)
	case "Withdraw":
		return extractWithdrawBlue(eventData, log.TransactionHash)
	case "Borrow":
		return extractBorrow(eventData, log.TransactionHash)
	case "Repay":
		return extractRepay(eventData, log.TransactionHash)
	case "SupplyCollateral":
		return extractSupplyCollateral(eventData, log.TransactionHash)
	case "WithdrawCollateral":
		return extractWithdrawCollateral(eventData, log.TransactionHash)
	case "Liquidate":
		return extractLiquidate(eventData, log.TransactionHash)
	case "AccrueInterest":
		return extractAccrueInterest(eventData, log.TransactionHash)
	case "SetFee":
		return extractSetFee(eventData, log.TransactionHash)
	default:
		return nil, fmt.Errorf("unknown Morpho Blue event: %s", event.Name)
	}
}

// ExtractMetaMorphoEvent parses a MetaMorpho event from a log entry.
func (e *EventExtractor) ExtractMetaMorphoEvent(log shared.Log) (MetaMorphoEvent, error) {
	if len(log.Topics) == 0 {
		return nil, fmt.Errorf("no topics")
	}

	eventSig := common.HexToHash(log.Topics[0])
	event, ok := e.metaMorphoSignatures[eventSig]
	if !ok {
		return nil, fmt.Errorf("not a tracked MetaMorpho event")
	}

	eventData := make(map[string]any)

	if err := parseTopics(event, log.Topics, eventData); err != nil {
		return nil, fmt.Errorf("parsing indexed params for %s: %w", event.Name, err)
	}
	if err := parseData(event, log.Data, eventData); err != nil {
		return nil, fmt.Errorf("parsing non-indexed params for %s: %w", event.Name, err)
	}

	switch event.Name {
	case "Deposit":
		return extractVaultDeposit(eventData, log.TransactionHash)
	case "Withdraw":
		return extractVaultWithdraw(eventData, log.TransactionHash)
	case "Transfer":
		return extractVaultTransfer(eventData, log.TransactionHash)
	case "AccrueInterest":
		return extractVaultAccrueInterest(eventData, log.TransactionHash)
	default:
		return nil, fmt.Errorf("unknown MetaMorpho event: %s", event.Name)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// parseTopics extracts indexed parameters from log topics into eventData.
func parseTopics(event *abi.Event, topics []string, eventData map[string]any) error {
	var indexed abi.Arguments
	for _, arg := range event.Inputs {
		if arg.Indexed {
			indexed = append(indexed, arg)
		}
	}

	if len(indexed) == 0 {
		return nil
	}

	hashes := make([]common.Hash, 0, len(topics)-1)
	for i := 1; i < len(topics); i++ {
		hashes = append(hashes, common.HexToHash(topics[i]))
	}

	return abi.ParseTopicsIntoMap(eventData, indexed, hashes)
}

// parseData extracts non-indexed parameters from log data into eventData.
func parseData(event *abi.Event, data string, eventData map[string]any) error {
	var nonIndexed abi.Arguments
	for _, arg := range event.Inputs {
		if !arg.Indexed {
			nonIndexed = append(nonIndexed, arg)
		}
	}

	if len(nonIndexed) == 0 || len(data) <= 2 {
		return nil
	}

	raw := common.FromHex(data)
	return nonIndexed.UnpackIntoMap(eventData, raw)
}

func getBytes32(eventData map[string]any, key string) ([32]byte, error) {
	v, ok := eventData[key]
	if !ok {
		return [32]byte{}, fmt.Errorf("missing field: %s", key)
	}
	b, ok := v.([32]byte)
	if !ok {
		return [32]byte{}, fmt.Errorf("invalid type for %s: %T", key, v)
	}
	return b, nil
}

func getAddress(eventData map[string]any, key string) (common.Address, error) {
	v, ok := eventData[key]
	if !ok {
		return common.Address{}, fmt.Errorf("missing field: %s", key)
	}
	addr, ok := v.(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("invalid type for %s: %T", key, v)
	}
	return addr, nil
}

func getBigInt(eventData map[string]any, key string) (*big.Int, error) {
	v, ok := eventData[key]
	if !ok {
		return nil, fmt.Errorf("missing field: %s", key)
	}
	b, ok := v.(*big.Int)
	if !ok {
		return nil, fmt.Errorf("invalid type for %s: %T", key, v)
	}
	return b, nil
}

// ---------------------------------------------------------------------------
// Morpho Blue event extractors
// ---------------------------------------------------------------------------

func extractCreateMarket(eventData map[string]any, txHash string) (*CreateMarketEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}

	// marketParams is a struct (tuple)
	mp, ok := eventData["marketParams"]
	if !ok {
		return nil, fmt.Errorf("missing field: marketParams")
	}

	// Use abi.ConvertType for safe conversion from the ABI-generated anonymous struct.
	converted := abi.ConvertType(mp, abiMarketParams{})
	mps, ok := converted.(abiMarketParams)
	if !ok {
		return nil, fmt.Errorf("failed to convert marketParams: %T", mp)
	}

	return &CreateMarketEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Params: &MarketParams{
			LoanToken:       mps.LoanToken,
			CollateralToken: mps.CollateralToken,
			Oracle:          mps.Oracle,
			Irm:             mps.Irm,
			LLTV:            mps.Lltv,
		},
	}, nil
}

func extractSupply(eventData map[string]any, txHash string) (*SupplyEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	caller, err := getAddress(eventData, "caller")
	if err != nil {
		return nil, err
	}
	onBehalf, err := getAddress(eventData, "onBehalf")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}
	shares, err := getBigInt(eventData, "shares")
	if err != nil {
		return nil, err
	}

	return &SupplyEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Caller:         caller,
		OnBehalf:       onBehalf,
		Assets:         assets,
		Shares:         shares,
	}, nil
}

func extractWithdrawBlue(eventData map[string]any, txHash string) (*WithdrawEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	caller, err := getAddress(eventData, "caller")
	if err != nil {
		return nil, err
	}
	onBehalf, err := getAddress(eventData, "onBehalf")
	if err != nil {
		return nil, err
	}
	receiver, err := getAddress(eventData, "receiver")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}
	shares, err := getBigInt(eventData, "shares")
	if err != nil {
		return nil, err
	}

	return &WithdrawEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Caller:         caller,
		OnBehalf:       onBehalf,
		Receiver:       receiver,
		Assets:         assets,
		Shares:         shares,
	}, nil
}

func extractBorrow(eventData map[string]any, txHash string) (*BorrowEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	caller, err := getAddress(eventData, "caller")
	if err != nil {
		return nil, err
	}
	onBehalf, err := getAddress(eventData, "onBehalf")
	if err != nil {
		return nil, err
	}
	receiver, err := getAddress(eventData, "receiver")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}
	shares, err := getBigInt(eventData, "shares")
	if err != nil {
		return nil, err
	}

	return &BorrowEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Caller:         caller,
		OnBehalf:       onBehalf,
		Receiver:       receiver,
		Assets:         assets,
		Shares:         shares,
	}, nil
}

func extractRepay(eventData map[string]any, txHash string) (*RepayEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	caller, err := getAddress(eventData, "caller")
	if err != nil {
		return nil, err
	}
	onBehalf, err := getAddress(eventData, "onBehalf")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}
	shares, err := getBigInt(eventData, "shares")
	if err != nil {
		return nil, err
	}

	return &RepayEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Caller:         caller,
		OnBehalf:       onBehalf,
		Assets:         assets,
		Shares:         shares,
	}, nil
}

func extractSupplyCollateral(eventData map[string]any, txHash string) (*SupplyCollateralEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	caller, err := getAddress(eventData, "caller")
	if err != nil {
		return nil, err
	}
	onBehalf, err := getAddress(eventData, "onBehalf")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}

	return &SupplyCollateralEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Caller:         caller,
		OnBehalf:       onBehalf,
		Assets:         assets,
	}, nil
}

func extractWithdrawCollateral(eventData map[string]any, txHash string) (*WithdrawCollateralEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	caller, err := getAddress(eventData, "caller")
	if err != nil {
		return nil, err
	}
	onBehalf, err := getAddress(eventData, "onBehalf")
	if err != nil {
		return nil, err
	}
	receiver, err := getAddress(eventData, "receiver")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}

	return &WithdrawCollateralEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Caller:         caller,
		OnBehalf:       onBehalf,
		Receiver:       receiver,
		Assets:         assets,
	}, nil
}

func extractLiquidate(eventData map[string]any, txHash string) (*LiquidateEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	caller, err := getAddress(eventData, "caller")
	if err != nil {
		return nil, err
	}
	borrower, err := getAddress(eventData, "borrower")
	if err != nil {
		return nil, err
	}
	repaidAssets, err := getBigInt(eventData, "repaidAssets")
	if err != nil {
		return nil, err
	}
	repaidShares, err := getBigInt(eventData, "repaidShares")
	if err != nil {
		return nil, err
	}
	seizedAssets, err := getBigInt(eventData, "seizedAssets")
	if err != nil {
		return nil, err
	}
	badDebtAssets, err := getBigInt(eventData, "badDebtAssets")
	if err != nil {
		return nil, err
	}
	badDebtShares, err := getBigInt(eventData, "badDebtShares")
	if err != nil {
		return nil, err
	}

	return &LiquidateEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Caller:         caller,
		Borrower:       borrower,
		RepaidAssets:   repaidAssets,
		RepaidShares:   repaidShares,
		SeizedAssets:   seizedAssets,
		BadDebtAssets:  badDebtAssets,
		BadDebtShares:  badDebtShares,
	}, nil
}

func extractAccrueInterest(eventData map[string]any, txHash string) (*AccrueInterestEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	prevBorrowRate, err := getBigInt(eventData, "prevBorrowRate")
	if err != nil {
		return nil, err
	}
	interest, err := getBigInt(eventData, "interest")
	if err != nil {
		return nil, err
	}
	feeShares, err := getBigInt(eventData, "feeShares")
	if err != nil {
		return nil, err
	}

	return &AccrueInterestEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		PrevBorrowRate: prevBorrowRate,
		Interest:       interest,
		FeeShares:      feeShares,
	}, nil
}

func extractSetFee(eventData map[string]any, txHash string) (*SetFeeEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	newFee, err := getBigInt(eventData, "newFee")
	if err != nil {
		return nil, err
	}

	return &SetFeeEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		NewFee:         newFee,
	}, nil
}

// ---------------------------------------------------------------------------
// MetaMorpho event extractors
// ---------------------------------------------------------------------------

func extractVaultDeposit(eventData map[string]any, txHash string) (*VaultDepositEvent, error) {
	sender, err := getAddress(eventData, "sender")
	if err != nil {
		return nil, err
	}
	owner, err := getAddress(eventData, "owner")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}
	shares, err := getBigInt(eventData, "shares")
	if err != nil {
		return nil, err
	}

	return &VaultDepositEvent{
		metaMorphoBase: metaMorphoBase{txHash: txHash},
		Sender:         sender,
		Owner:          owner,
		Assets:         assets,
		Shares:         shares,
	}, nil
}

func extractVaultWithdraw(eventData map[string]any, txHash string) (*VaultWithdrawEvent, error) {
	sender, err := getAddress(eventData, "sender")
	if err != nil {
		return nil, err
	}
	receiver, err := getAddress(eventData, "receiver")
	if err != nil {
		return nil, err
	}
	owner, err := getAddress(eventData, "owner")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}
	shares, err := getBigInt(eventData, "shares")
	if err != nil {
		return nil, err
	}

	return &VaultWithdrawEvent{
		metaMorphoBase: metaMorphoBase{txHash: txHash},
		Sender:         sender,
		Receiver:       receiver,
		Owner:          owner,
		Assets:         assets,
		Shares:         shares,
	}, nil
}

func extractVaultTransfer(eventData map[string]any, txHash string) (*VaultTransferEvent, error) {
	from, err := getAddress(eventData, "from")
	if err != nil {
		return nil, err
	}
	to, err := getAddress(eventData, "to")
	if err != nil {
		return nil, err
	}
	value, err := getBigInt(eventData, "value")
	if err != nil {
		return nil, err
	}

	return &VaultTransferEvent{
		metaMorphoBase: metaMorphoBase{txHash: txHash},
		From:           from,
		To:             to,
		Value:          value,
	}, nil
}

func extractVaultAccrueInterest(eventData map[string]any, txHash string) (*VaultAccrueInterestEvent, error) {
	newTotalAssets, err := getBigInt(eventData, "newTotalAssets")
	if err != nil {
		return nil, err
	}

	// V1 uses "feeShares", V2 uses "performanceFeeShares" for the same concept.
	feeShares, err := getBigInt(eventData, "feeShares")
	if err != nil {
		feeShares, err = getBigInt(eventData, "performanceFeeShares")
		if err != nil {
			return nil, fmt.Errorf("missing both feeShares and performanceFeeShares")
		}
	}

	result := &VaultAccrueInterestEvent{
		metaMorphoBase: metaMorphoBase{txHash: txHash},
		NewTotalAssets: newTotalAssets,
		FeeShares:      feeShares,
	}

	// V2 fields (only present when parsed from V2 ABI)
	if prev, err := getBigInt(eventData, "previousTotalAssets"); err == nil {
		result.PreviousTotalAssets = prev
	}
	if mgmt, err := getBigInt(eventData, "managementFeeShares"); err == nil {
		result.ManagementFeeShares = mgmt
	}

	return result, nil
}
