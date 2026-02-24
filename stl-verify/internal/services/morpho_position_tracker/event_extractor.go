package morpho_position_tracker

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
)

// MorphoBlueEventData holds parsed data from a Morpho Blue event.
type MorphoBlueEventData struct {
	EventType entity.MorphoEventType
	TxHash    string
	MarketID  [32]byte

	// Common fields
	Caller   common.Address
	OnBehalf common.Address
	Receiver common.Address
	Assets   *big.Int
	Shares   *big.Int

	// CreateMarket fields
	MarketParams *MarketParams

	// Liquidate fields
	Borrower      common.Address
	RepaidAssets  *big.Int
	RepaidShares  *big.Int
	SeizedAssets  *big.Int
	BadDebtAssets *big.Int
	BadDebtShares *big.Int

	// AccrueInterest fields
	PrevBorrowRate *big.Int
	Interest       *big.Int
	FeeShares      *big.Int

	// SetFee fields
	NewFee *big.Int
}

// MarketParams holds the market parameters from a CreateMarket event.
type MarketParams struct {
	LoanToken       common.Address
	CollateralToken common.Address
	Oracle          common.Address
	Irm             common.Address
	LLTV            *big.Int
}

// MetaMorphoEventData holds parsed data from a MetaMorpho vault event.
type MetaMorphoEventData struct {
	EventType entity.MorphoEventType
	TxHash    string

	// Deposit/Withdraw fields
	Sender   common.Address
	Owner    common.Address
	Receiver common.Address
	Assets   *big.Int
	Shares   *big.Int

	// Transfer fields
	From  common.Address
	To    common.Address
	Value *big.Int

	// AccrueInterest fields
	NewTotalAssets *big.Int
	FeeShares      *big.Int
}

// ToJSON converts MorphoBlueEventData to JSON for protocol_event storage.
func (e *MorphoBlueEventData) ToJSON() (json.RawMessage, error) {
	data := map[string]any{
		"eventType": string(e.EventType),
		"marketId":  common.Bytes2Hex(e.MarketID[:]),
	}

	switch e.EventType {
	case entity.MorphoEventCreateMarket:
		if e.MarketParams != nil {
			data["loanToken"] = e.MarketParams.LoanToken.Hex()
			data["collateralToken"] = e.MarketParams.CollateralToken.Hex()
			data["oracle"] = e.MarketParams.Oracle.Hex()
			data["irm"] = e.MarketParams.Irm.Hex()
			data["lltv"] = e.MarketParams.LLTV.String()
		}
	case entity.MorphoEventSupply, entity.MorphoEventRepay, entity.MorphoEventSupplyCollateral:
		data["caller"] = e.Caller.Hex()
		data["onBehalf"] = e.OnBehalf.Hex()
		if e.Assets != nil {
			data["assets"] = e.Assets.String()
		}
		if e.Shares != nil {
			data["shares"] = e.Shares.String()
		}
	case entity.MorphoEventWithdraw, entity.MorphoEventBorrow, entity.MorphoEventWithdrawCollateral:
		data["caller"] = e.Caller.Hex()
		data["onBehalf"] = e.OnBehalf.Hex()
		data["receiver"] = e.Receiver.Hex()
		if e.Assets != nil {
			data["assets"] = e.Assets.String()
		}
		if e.Shares != nil {
			data["shares"] = e.Shares.String()
		}
	case entity.MorphoEventLiquidate:
		data["caller"] = e.Caller.Hex()
		data["borrower"] = e.Borrower.Hex()
		data["repaidAssets"] = e.RepaidAssets.String()
		data["repaidShares"] = e.RepaidShares.String()
		data["seizedAssets"] = e.SeizedAssets.String()
		data["badDebtAssets"] = e.BadDebtAssets.String()
		data["badDebtShares"] = e.BadDebtShares.String()
	case entity.MorphoEventAccrueInterest:
		data["prevBorrowRate"] = e.PrevBorrowRate.String()
		data["interest"] = e.Interest.String()
		data["feeShares"] = e.FeeShares.String()
	case entity.MorphoEventSetFee:
		data["newFee"] = e.NewFee.String()
	}

	raw, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal morpho event data: %w", err)
	}
	return raw, nil
}

// ToJSON converts MetaMorphoEventData to JSON for protocol_event storage.
func (e *MetaMorphoEventData) ToJSON() (json.RawMessage, error) {
	data := map[string]any{
		"eventType": string(e.EventType),
	}

	switch e.EventType {
	case entity.MorphoEventVaultDeposit:
		data["sender"] = e.Sender.Hex()
		data["owner"] = e.Owner.Hex()
		data["assets"] = e.Assets.String()
		data["shares"] = e.Shares.String()
	case entity.MorphoEventVaultWithdraw:
		data["sender"] = e.Sender.Hex()
		data["receiver"] = e.Receiver.Hex()
		data["owner"] = e.Owner.Hex()
		data["assets"] = e.Assets.String()
		data["shares"] = e.Shares.String()
	case entity.MorphoEventVaultTransfer:
		data["from"] = e.From.Hex()
		data["to"] = e.To.Hex()
		data["value"] = e.Value.String()
	case entity.MorphoEventVaultAccrueInterest:
		data["newTotalAssets"] = e.NewTotalAssets.String()
		data["feeShares"] = e.FeeShares.String()
	}

	raw, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metamorpho event data: %w", err)
	}
	return raw, nil
}

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
			return fmt.Errorf("Morpho Blue %s event not found in ABI", name)
		}
		e.morphoBlueSignatures[event.ID] = &event
	}

	metaMorphoABI, err := abis.GetMetaMorphoEventsABI()
	if err != nil {
		return fmt.Errorf("failed to parse MetaMorpho events ABI: %w", err)
	}
	e.metaMorphoABI = metaMorphoABI

	metaMorphoEventNames := []string{"Deposit", "Withdraw", "Transfer", "AccrueInterest"}
	for _, name := range metaMorphoEventNames {
		event, ok := e.metaMorphoABI.Events[name]
		if !ok {
			return fmt.Errorf("MetaMorpho %s event not found in ABI", name)
		}
		e.metaMorphoSignatures[event.ID] = &event
	}

	return nil
}

// IsMorphoBlueEvent returns true if the log matches a known Morpho Blue event.
func (e *EventExtractor) IsMorphoBlueEvent(log Log) bool {
	if len(log.Topics) == 0 {
		return false
	}
	_, ok := e.morphoBlueSignatures[common.HexToHash(log.Topics[0])]
	return ok
}

// IsMetaMorphoEvent returns true if the log matches a known MetaMorpho event.
func (e *EventExtractor) IsMetaMorphoEvent(log Log) bool {
	if len(log.Topics) == 0 {
		return false
	}
	_, ok := e.metaMorphoSignatures[common.HexToHash(log.Topics[0])]
	return ok
}

// ExtractMorphoBlueEvent parses a Morpho Blue event from a log entry.
func (e *EventExtractor) ExtractMorphoBlueEvent(log Log) (*MorphoBlueEventData, error) {
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
func (e *EventExtractor) ExtractMetaMorphoEvent(log Log) (*MetaMorphoEventData, error) {
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

// Morpho Blue event extractors

func extractCreateMarket(eventData map[string]any, txHash string) (*MorphoBlueEventData, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}

	// marketParams is a struct (tuple)
	mp, ok := eventData["marketParams"]
	if !ok {
		return nil, fmt.Errorf("missing field: marketParams")
	}

	type marketParamsTuple struct {
		LoanToken       common.Address `abi:"loanToken"`
		CollateralToken common.Address `abi:"collateralToken"`
		Oracle          common.Address `abi:"oracle"`
		Irm             common.Address `abi:"irm"`
		Lltv            *big.Int       `abi:"lltv"`
	}

	// The ABI unpacker returns a struct for tuple types
	mps, ok := mp.(struct {
		LoanToken       common.Address `json:"loanToken"`
		CollateralToken common.Address `json:"collateralToken"`
		Oracle          common.Address `json:"oracle"`
		Irm             common.Address `json:"irm"`
		Lltv            *big.Int       `json:"lltv"`
	})
	if !ok {
		return nil, fmt.Errorf("invalid marketParams type: %T", mp)
	}

	return &MorphoBlueEventData{
		EventType: entity.MorphoEventCreateMarket,
		TxHash:    txHash,
		MarketID:  id,
		MarketParams: &MarketParams{
			LoanToken:       mps.LoanToken,
			CollateralToken: mps.CollateralToken,
			Oracle:          mps.Oracle,
			Irm:             mps.Irm,
			LLTV:            mps.Lltv,
		},
	}, nil
}

func extractSupply(eventData map[string]any, txHash string) (*MorphoBlueEventData, error) {
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

	return &MorphoBlueEventData{
		EventType: entity.MorphoEventSupply,
		TxHash:    txHash,
		MarketID:  id,
		Caller:    caller,
		OnBehalf:  onBehalf,
		Assets:    assets,
		Shares:    shares,
	}, nil
}

func extractWithdrawBlue(eventData map[string]any, txHash string) (*MorphoBlueEventData, error) {
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

	return &MorphoBlueEventData{
		EventType: entity.MorphoEventWithdraw,
		TxHash:    txHash,
		MarketID:  id,
		Caller:    caller,
		OnBehalf:  onBehalf,
		Receiver:  receiver,
		Assets:    assets,
		Shares:    shares,
	}, nil
}

func extractBorrow(eventData map[string]any, txHash string) (*MorphoBlueEventData, error) {
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

	return &MorphoBlueEventData{
		EventType: entity.MorphoEventBorrow,
		TxHash:    txHash,
		MarketID:  id,
		Caller:    caller,
		OnBehalf:  onBehalf,
		Receiver:  receiver,
		Assets:    assets,
		Shares:    shares,
	}, nil
}

func extractRepay(eventData map[string]any, txHash string) (*MorphoBlueEventData, error) {
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

	return &MorphoBlueEventData{
		EventType: entity.MorphoEventRepay,
		TxHash:    txHash,
		MarketID:  id,
		Caller:    caller,
		OnBehalf:  onBehalf,
		Assets:    assets,
		Shares:    shares,
	}, nil
}

func extractSupplyCollateral(eventData map[string]any, txHash string) (*MorphoBlueEventData, error) {
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

	return &MorphoBlueEventData{
		EventType: entity.MorphoEventSupplyCollateral,
		TxHash:    txHash,
		MarketID:  id,
		Caller:    caller,
		OnBehalf:  onBehalf,
		Assets:    assets,
	}, nil
}

func extractWithdrawCollateral(eventData map[string]any, txHash string) (*MorphoBlueEventData, error) {
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

	return &MorphoBlueEventData{
		EventType: entity.MorphoEventWithdrawCollateral,
		TxHash:    txHash,
		MarketID:  id,
		Caller:    caller,
		OnBehalf:  onBehalf,
		Receiver:  receiver,
		Assets:    assets,
	}, nil
}

func extractLiquidate(eventData map[string]any, txHash string) (*MorphoBlueEventData, error) {
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

	return &MorphoBlueEventData{
		EventType:     entity.MorphoEventLiquidate,
		TxHash:        txHash,
		MarketID:      id,
		Caller:        caller,
		Borrower:      borrower,
		RepaidAssets:  repaidAssets,
		RepaidShares:  repaidShares,
		SeizedAssets:  seizedAssets,
		BadDebtAssets: badDebtAssets,
		BadDebtShares: badDebtShares,
	}, nil
}

func extractAccrueInterest(eventData map[string]any, txHash string) (*MorphoBlueEventData, error) {
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

	return &MorphoBlueEventData{
		EventType:      entity.MorphoEventAccrueInterest,
		TxHash:         txHash,
		MarketID:       id,
		PrevBorrowRate: prevBorrowRate,
		Interest:       interest,
		FeeShares:      feeShares,
	}, nil
}

func extractSetFee(eventData map[string]any, txHash string) (*MorphoBlueEventData, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	newFee, err := getBigInt(eventData, "newFee")
	if err != nil {
		return nil, err
	}

	return &MorphoBlueEventData{
		EventType: entity.MorphoEventSetFee,
		TxHash:    txHash,
		MarketID:  id,
		NewFee:    newFee,
	}, nil
}

// MetaMorpho event extractors

func extractVaultDeposit(eventData map[string]any, txHash string) (*MetaMorphoEventData, error) {
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

	return &MetaMorphoEventData{
		EventType: entity.MorphoEventVaultDeposit,
		TxHash:    txHash,
		Sender:    sender,
		Owner:     owner,
		Assets:    assets,
		Shares:    shares,
	}, nil
}

func extractVaultWithdraw(eventData map[string]any, txHash string) (*MetaMorphoEventData, error) {
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

	return &MetaMorphoEventData{
		EventType: entity.MorphoEventVaultWithdraw,
		TxHash:    txHash,
		Sender:    sender,
		Receiver:  receiver,
		Owner:     owner,
		Assets:    assets,
		Shares:    shares,
	}, nil
}

func extractVaultTransfer(eventData map[string]any, txHash string) (*MetaMorphoEventData, error) {
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

	return &MetaMorphoEventData{
		EventType: entity.MorphoEventVaultTransfer,
		TxHash:    txHash,
		From:      from,
		To:        to,
		Value:     value,
	}, nil
}

func extractVaultAccrueInterest(eventData map[string]any, txHash string) (*MetaMorphoEventData, error) {
	newTotalAssets, err := getBigInt(eventData, "newTotalAssets")
	if err != nil {
		return nil, err
	}
	feeShares, err := getBigInt(eventData, "feeShares")
	if err != nil {
		return nil, err
	}

	return &MetaMorphoEventData{
		EventType:      entity.MorphoEventVaultAccrueInterest,
		TxHash:         txHash,
		NewTotalAssets: newTotalAssets,
		FeeShares:      feeShares,
	}, nil
}
