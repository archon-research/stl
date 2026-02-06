package entity

import (
	"encoding/json"
	"fmt"
)

// ProtocolEvent represents a decoded protocol event stored for analytics and auditability.
type ProtocolEvent struct {
	ID              int64
	ChainID         int
	ProtocolID      int64
	BlockNumber     int64
	BlockVersion    int
	TxHash          []byte
	LogIndex        int
	ContractAddress []byte
	EventName       string
	EventData       json.RawMessage
}

// NewProtocolEvent creates a new ProtocolEvent with validation.
func NewProtocolEvent(chainID int, protocolID, blockNumber int64, blockVersion int, txHash []byte, logIndex int, contractAddress []byte, eventName string, eventData json.RawMessage) (*ProtocolEvent, error) {
	e := &ProtocolEvent{
		ChainID:         chainID,
		ProtocolID:      protocolID,
		BlockNumber:     blockNumber,
		BlockVersion:    blockVersion,
		TxHash:          txHash,
		LogIndex:        logIndex,
		ContractAddress: contractAddress,
		EventName:       eventName,
		EventData:       eventData,
	}
	if err := e.validate(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *ProtocolEvent) validate() error {
	if e.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", e.ChainID)
	}
	if e.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", e.ProtocolID)
	}
	if e.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", e.BlockNumber)
	}
	if e.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", e.BlockVersion)
	}
	if len(e.TxHash) == 0 {
		return fmt.Errorf("txHash must not be empty")
	}
	if e.LogIndex < 0 {
		return fmt.Errorf("logIndex must be non-negative, got %d", e.LogIndex)
	}
	if len(e.ContractAddress) == 0 {
		return fmt.Errorf("contractAddress must not be empty")
	}
	if e.EventName == "" {
		return fmt.Errorf("eventName must not be empty")
	}
	if len(e.EventData) == 0 {
		return fmt.Errorf("eventData must not be empty")
	}
	if !json.Valid(e.EventData) {
		return fmt.Errorf("eventData must be valid JSON")
	}
	return nil
}
