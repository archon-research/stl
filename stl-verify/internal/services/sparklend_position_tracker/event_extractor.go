package sparklend_position_tracker

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

// ReserveEventData contains the data extracted from a ReserveDataUpdated event.
type ReserveEventData struct {
	Reserve common.Address
	TxHash  string
}

// EventExtractor handles parsing and extraction of SparkLend position events from logs.
type EventExtractor struct {
	positionEventsABI       *abi.ABI
	positionEventSignatures map[common.Hash]*abi.Event
	reserveEventsABI        *abi.ABI
	reserveEventSignatures  map[common.Hash]*abi.Event
}

// NewEventExtractor creates a new EventExtractor with all SparkLend position event ABIs loaded.
func NewEventExtractor() (*EventExtractor, error) {
	extractor := &EventExtractor{
		positionEventSignatures: make(map[common.Hash]*abi.Event),
		reserveEventSignatures:  make(map[common.Hash]*abi.Event),
	}

	if err := extractor.loadEventABIs(); err != nil {
		return nil, err
	}

	return extractor, nil
}

func (e *EventExtractor) loadEventABIs() error {
	// All 7 SparkLend position-changing events
	positionEventsABI := `[
		{"anonymous":false,"inputs":[{"indexed":true,"name":"reserve","type":"address"},{"indexed":false,"name":"user","type":"address"},{"indexed":true,"name":"onBehalfOf","type":"address"},{"indexed":false,"name":"amount","type":"uint256"},{"indexed":false,"name":"interestRateMode","type":"uint8"},{"indexed":false,"name":"borrowRate","type":"uint256"},{"indexed":true,"name":"referralCode","type":"uint16"}],"name":"Borrow","type":"event"},
		{"anonymous":false,"inputs":[{"indexed":true,"name":"reserve","type":"address"},{"indexed":true,"name":"user","type":"address"},{"indexed":true,"name":"repayer","type":"address"},{"indexed":false,"name":"amount","type":"uint256"},{"indexed":false,"name":"useATokens","type":"bool"}],"name":"Repay","type":"event"},
		{"anonymous":false,"inputs":[{"indexed":true,"name":"reserve","type":"address"},{"indexed":false,"name":"user","type":"address"},{"indexed":true,"name":"onBehalfOf","type":"address"},{"indexed":false,"name":"amount","type":"uint256"},{"indexed":true,"name":"referralCode","type":"uint16"}],"name":"Supply","type":"event"},
		{"anonymous":false,"inputs":[{"indexed":true,"name":"reserve","type":"address"},{"indexed":true,"name":"user","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"amount","type":"uint256"}],"name":"Withdraw","type":"event"},
		{"anonymous":false,"inputs":[{"indexed":true,"name":"collateralAsset","type":"address"},{"indexed":true,"name":"debtAsset","type":"address"},{"indexed":true,"name":"user","type":"address"},{"indexed":false,"name":"debtToCover","type":"uint256"},{"indexed":false,"name":"liquidatedCollateralAmount","type":"uint256"},{"indexed":false,"name":"liquidator","type":"address"},{"indexed":false,"name":"receiveAToken","type":"bool"}],"name":"LiquidationCall","type":"event"},
		{"anonymous":false,"inputs":[{"indexed":true,"name":"reserve","type":"address"},{"indexed":true,"name":"user","type":"address"}],"name":"ReserveUsedAsCollateralEnabled","type":"event"},
		{"anonymous":false,"inputs":[{"indexed":true,"name":"reserve","type":"address"},{"indexed":true,"name":"user","type":"address"}],"name":"ReserveUsedAsCollateralDisabled","type":"event"}
	]`
	parsedPositionABI, err := abi.JSON(strings.NewReader(positionEventsABI))
	if err != nil {
		return fmt.Errorf("failed to parse position events ABI: %w", err)
	}
	e.positionEventsABI = &parsedPositionABI

	// Register all position event signatures for quick lookup
	positionEventNames := []string{
		"Borrow", "Repay", "Supply", "Withdraw",
		"LiquidationCall", "ReserveUsedAsCollateralEnabled", "ReserveUsedAsCollateralDisabled",
	}
	for _, name := range positionEventNames {
		if event, ok := e.positionEventsABI.Events[name]; ok {
			e.positionEventSignatures[event.ID] = &event
		} else {
			return fmt.Errorf("%s event not found in ABI", name)
		}
	}

	// Reserve events ABI (ReserveDataUpdated)
	reserveEventsABI := `[
		{"anonymous":false,"inputs":[{"indexed":true,"name":"reserve","type":"address"},{"indexed":false,"name":"liquidityRate","type":"uint256"},{"indexed":false,"name":"stableBorrowRate","type":"uint256"},{"indexed":false,"name":"variableBorrowRate","type":"uint256"},{"indexed":false,"name":"liquidityIndex","type":"uint256"},{"indexed":false,"name":"variableBorrowIndex","type":"uint256"}],"name":"ReserveDataUpdated","type":"event"}
	]`
	parsedReserveABI, err := abi.JSON(strings.NewReader(reserveEventsABI))
	if err != nil {
		return fmt.Errorf("failed to parse reserve events ABI: %w", err)
	}
	e.reserveEventsABI = &parsedReserveABI

	// Register reserve event signatures
	reserveEventNames := []string{"ReserveDataUpdated"}
	for _, name := range reserveEventNames {
		if event, ok := e.reserveEventsABI.Events[name]; ok {
			e.reserveEventSignatures[event.ID] = &event
		} else {
			return fmt.Errorf("%s event not found in ABI", name)
		}
	}

	return nil
}

// IsPositionEvent checks if the log contains a tracked SparkLend position event.
func (e *EventExtractor) IsPositionEvent(log Log) bool {
	if len(log.Topics) == 0 {
		return false
	}
	eventSig := common.HexToHash(log.Topics[0])
	_, ok := e.positionEventSignatures[eventSig]
	return ok
}

// IsReserveEvent checks if the log contains a ReserveDataUpdated event.
func (e *EventExtractor) IsReserveEvent(log Log) bool {
	if len(log.Topics) == 0 {
		return false
	}
	eventSig := common.HexToHash(log.Topics[0])
	_, ok := e.reserveEventSignatures[eventSig]
	return ok
}

// ExtractEventData parses a position log and returns structured event data.
func (e *EventExtractor) ExtractEventData(log Log) (*PositionEventData, error) {
	if len(log.Topics) == 0 {
		return nil, fmt.Errorf("no topics")
	}

	eventSig := common.HexToHash(log.Topics[0])
	event, ok := e.positionEventSignatures[eventSig]
	if !ok {
		return nil, fmt.Errorf("not a tracked position event")
	}

	eventData := make(map[string]interface{})

	// Parse indexed parameters from topics
	var indexed abi.Arguments
	for _, arg := range event.Inputs {
		if arg.Indexed {
			indexed = append(indexed, arg)
		}
	}

	if len(indexed) > 0 {
		topics := make([]common.Hash, 0, len(log.Topics)-1)
		for i := 1; i < len(log.Topics); i++ {
			topics = append(topics, common.HexToHash(log.Topics[i]))
		}
		if err := abi.ParseTopicsIntoMap(eventData, indexed, topics); err != nil {
			return nil, fmt.Errorf("failed to parse indexed params: %w", err)
		}
	}

	// Parse non-indexed parameters from data
	var nonIndexed abi.Arguments
	for _, arg := range event.Inputs {
		if !arg.Indexed {
			nonIndexed = append(nonIndexed, arg)
		}
	}

	if len(nonIndexed) > 0 && len(log.Data) > 2 {
		data := common.FromHex(log.Data)
		if err := nonIndexed.UnpackIntoMap(eventData, data); err != nil {
			return nil, fmt.Errorf("failed to parse non-indexed params: %w", err)
		}
	}

	// Dispatch to type-specific extractors
	switch event.Name {
	case "Borrow":
		return e.extractBorrowData(eventData, log.TransactionHash)
	case "Repay":
		return e.extractRepayData(eventData, log.TransactionHash)
	case "Supply":
		return e.extractSupplyData(eventData, log.TransactionHash)
	case "Withdraw":
		return e.extractWithdrawData(eventData, log.TransactionHash)
	case "LiquidationCall":
		return e.extractLiquidationData(eventData, log.TransactionHash)
	case "ReserveUsedAsCollateralEnabled":
		return e.extractCollateralEnabledData(eventData, log.TransactionHash)
	case "ReserveUsedAsCollateralDisabled":
		return e.extractCollateralDisabledData(eventData, log.TransactionHash)
	default:
		return nil, fmt.Errorf("unknown event type: %s", event.Name)
	}
}

func (e *EventExtractor) extractBorrowData(eventData map[string]interface{}, txHash string) (*PositionEventData, error) {
	// Borrow: onBehalfOf is the borrower
	user, ok := eventData["onBehalfOf"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid onBehalfOf type in Borrow event")
	}
	reserve, ok := eventData["reserve"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid reserve type in Borrow event")
	}
	amount, ok := eventData["amount"].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("invalid amount type in Borrow event")
	}
	return &PositionEventData{
		EventType: EventBorrow,
		TxHash:    txHash,
		User:      user,
		Reserve:   reserve,
		Amount:    amount,
	}, nil
}

func (e *EventExtractor) extractRepayData(eventData map[string]interface{}, txHash string) (*PositionEventData, error) {
	// Repay: user is the debt holder (whose debt is being repaid)
	user, ok := eventData["user"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid user type in Repay event")
	}
	reserve, ok := eventData["reserve"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid reserve type in Repay event")
	}
	amount, ok := eventData["amount"].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("invalid amount type in Repay event")
	}
	return &PositionEventData{
		EventType: EventRepay,
		TxHash:    txHash,
		User:      user,
		Reserve:   reserve,
		Amount:    amount,
	}, nil
}

func (e *EventExtractor) extractSupplyData(eventData map[string]interface{}, txHash string) (*PositionEventData, error) {
	// Supply: onBehalfOf is the supplier
	user, ok := eventData["onBehalfOf"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid onBehalfOf type in Supply event")
	}
	reserve, ok := eventData["reserve"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid reserve type in Supply event")
	}
	amount, ok := eventData["amount"].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("invalid amount type in Supply event")
	}
	return &PositionEventData{
		EventType: EventSupply,
		TxHash:    txHash,
		User:      user,
		Reserve:   reserve,
		Amount:    amount,
	}, nil
}

func (e *EventExtractor) extractWithdrawData(eventData map[string]interface{}, txHash string) (*PositionEventData, error) {
	// Withdraw: user is the one withdrawing
	user, ok := eventData["user"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid user type in Withdraw event")
	}
	reserve, ok := eventData["reserve"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid reserve type in Withdraw event")
	}
	amount, ok := eventData["amount"].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("invalid amount type in Withdraw event")
	}
	return &PositionEventData{
		EventType: EventWithdraw,
		TxHash:    txHash,
		User:      user,
		Reserve:   reserve,
		Amount:    amount,
	}, nil
}

func (e *EventExtractor) extractLiquidationData(eventData map[string]interface{}, txHash string) (*PositionEventData, error) {
	// LiquidationCall: both user (borrower being liquidated) and liquidator need snapshots
	user, ok := eventData["user"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid user type in LiquidationCall event")
	}
	liquidator, ok := eventData["liquidator"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid liquidator type in LiquidationCall event")
	}
	collateralAsset, ok := eventData["collateralAsset"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid collateralAsset type in LiquidationCall event")
	}
	debtAsset, ok := eventData["debtAsset"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid debtAsset type in LiquidationCall event")
	}
	debtToCover, ok := eventData["debtToCover"].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("invalid debtToCover type in LiquidationCall event")
	}
	liquidatedCollateralAmount, ok := eventData["liquidatedCollateralAmount"].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("invalid liquidatedCollateralAmount type in LiquidationCall event")
	}
	return &PositionEventData{
		EventType:                  EventLiquidationCall,
		TxHash:                     txHash,
		User:                       user,
		Liquidator:                 liquidator,
		CollateralAsset:            collateralAsset,
		DebtAsset:                  debtAsset,
		DebtToCover:                debtToCover,
		LiquidatedCollateralAmount: liquidatedCollateralAmount,
	}, nil
}

func (e *EventExtractor) extractCollateralEnabledData(eventData map[string]interface{}, txHash string) (*PositionEventData, error) {
	user, ok := eventData["user"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid user type in ReserveUsedAsCollateralEnabled event")
	}
	reserve, ok := eventData["reserve"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid reserve type in ReserveUsedAsCollateralEnabled event")
	}
	return &PositionEventData{
		EventType:         EventReserveUsedAsCollateralEnabled,
		TxHash:            txHash,
		User:              user,
		Reserve:           reserve,
		CollateralEnabled: true,
	}, nil
}

func (e *EventExtractor) extractCollateralDisabledData(eventData map[string]interface{}, txHash string) (*PositionEventData, error) {
	user, ok := eventData["user"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid user type in ReserveUsedAsCollateralDisabled event")
	}
	reserve, ok := eventData["reserve"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("invalid reserve type in ReserveUsedAsCollateralDisabled event")
	}
	return &PositionEventData{
		EventType:         EventReserveUsedAsCollateralDisabled,
		TxHash:            txHash,
		User:              user,
		Reserve:           reserve,
		CollateralEnabled: false,
	}, nil
}

// ExtractReserveEventData parses a ReserveDataUpdated log and returns the reserve address.
func (e *EventExtractor) ExtractReserveEventData(log Log) (*ReserveEventData, error) {
	if len(log.Topics) < 2 {
		return nil, fmt.Errorf("ReserveDataUpdated event requires at least 2 topics")
	}

	eventSig := common.HexToHash(log.Topics[0])
	_, ok := e.reserveEventSignatures[eventSig]
	if !ok {
		return nil, fmt.Errorf("not a ReserveDataUpdated event")
	}

	// reserve is indexed (topic[1])
	reserve := common.HexToAddress(log.Topics[1])

	return &ReserveEventData{
		Reserve: reserve,
		TxHash:  log.TransactionHash,
	}, nil
}

// ToJSON converts PositionEventData to a JSON-serializable map.
// Addresses are hex strings, amounts are decimal strings.
func (p *PositionEventData) ToJSON() (json.RawMessage, error) {
	data := make(map[string]interface{})
	data["eventType"] = string(p.EventType)
	data["user"] = p.User.Hex()

	if p.Reserve != (common.Address{}) {
		data["reserve"] = p.Reserve.Hex()
	}
	if p.Amount != nil {
		data["amount"] = p.Amount.String()
	}
	if p.Liquidator != (common.Address{}) {
		data["liquidator"] = p.Liquidator.Hex()
	}
	if p.CollateralAsset != (common.Address{}) {
		data["collateralAsset"] = p.CollateralAsset.Hex()
	}
	if p.DebtAsset != (common.Address{}) {
		data["debtAsset"] = p.DebtAsset.Hex()
	}
	if p.DebtToCover != nil {
		data["debtToCover"] = p.DebtToCover.String()
	}
	if p.LiquidatedCollateralAmount != nil {
		data["liquidatedCollateralAmount"] = p.LiquidatedCollateralAmount.String()
	}
	if p.EventType == EventReserveUsedAsCollateralEnabled || p.EventType == EventReserveUsedAsCollateralDisabled {
		data["collateralEnabled"] = p.CollateralEnabled
	}

	raw, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event data: %w", err)
	}
	return raw, nil
}
