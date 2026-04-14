package entity

import (
	"encoding/json"
	"fmt"
	"time"
)

// ProtocolEvent represents a decoded protocol event stored for analytics and auditability.
type ProtocolEvent struct {
	ChainID         int
	ProtocolID      int64
	BlockNumber     int64
	BlockVersion    int
	TxHash          []byte
	LogIndex        int
	ContractAddress []byte
	EventName       string
	EventData       json.RawMessage
	CreatedAt       time.Time // block timestamp — deterministic for hypertable dedup
}

// NewProtocolEvent creates a new ProtocolEvent with validation.
func NewProtocolEvent(chainID int, protocolID, blockNumber int64, blockVersion int, txHash []byte, logIndex int, contractAddress []byte, eventName string, eventData json.RawMessage, createdAt time.Time) (*ProtocolEvent, error) {
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
		CreatedAt:       createdAt,
	}
	if err := e.Validate(); err != nil {
		return nil, fmt.Errorf("NewProtocolEvent: %w", err)
	}
	return e, nil
}

func (e *ProtocolEvent) Validate() error {
	if e.CreatedAt.IsZero() {
		return fmt.Errorf("createdAt must be set explicitly (block timestamp)")
	}
	if e.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", e.ChainID)
	}
	if e.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", e.ProtocolID)
	}
	// Non-negative (allows block 0 / genesis) — unlike position entities which require
	// positive block numbers, events can occur in the genesis block (e.g. contract deploys).
	if e.BlockNumber < 0 {
		return fmt.Errorf("blockNumber must be non-negative, got %d", e.BlockNumber)
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
