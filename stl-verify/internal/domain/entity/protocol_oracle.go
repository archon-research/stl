package entity

import (
	"fmt"
	"time"
)

// ProtocolOracle is a temporal binding: which oracle a protocol uses at which blocks.
// When a protocol changes oracle, close the old row (set ToBlock) and insert a new row.
type ProtocolOracle struct {
	ID         int64
	ProtocolID int64
	OracleID   int64
	FromBlock  int64
	ToBlock    *int64 // nil means currently active
	CreatedAt  time.Time
}

// NewProtocolOracle creates a new ProtocolOracle with validation.
func NewProtocolOracle(protocolID, oracleID, fromBlock int64) (*ProtocolOracle, error) {
	po := &ProtocolOracle{
		ProtocolID: protocolID,
		OracleID:   oracleID,
		FromBlock:  fromBlock,
	}
	if err := po.validate(); err != nil {
		return nil, err
	}
	return po, nil
}

func (po *ProtocolOracle) validate() error {
	if po.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", po.ProtocolID)
	}
	if po.OracleID <= 0 {
		return fmt.Errorf("oracleID must be positive, got %d", po.OracleID)
	}
	if po.FromBlock <= 0 {
		return fmt.Errorf("fromBlock must be positive, got %d", po.FromBlock)
	}
	return nil
}

// IsActive returns true if this binding has no end block.
func (po *ProtocolOracle) IsActive() bool {
	return po.ToBlock == nil
}
