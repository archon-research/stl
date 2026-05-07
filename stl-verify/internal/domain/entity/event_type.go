package entity

// EventType represents the type of position-changing event.
// On-chain events correspond to actual log emissions from smart contracts.
// Internal events (prefixed with "internal:") are computed by our pipeline.
type EventType string

const (
	// On-chain events (from actual log emissions)
	EventBorrow                          EventType = "Borrow"
	EventRepay                           EventType = "Repay"
	EventSupply                          EventType = "Supply"
	EventWithdraw                        EventType = "Withdraw"
	EventLiquidationCall                 EventType = "LiquidationCall"
	EventReserveUsedAsCollateralEnabled  EventType = "ReserveUsedAsCollateralEnabled"
	EventReserveUsedAsCollateralDisabled EventType = "ReserveUsedAsCollateralDisabled"

	// Internal events (computed by our pipeline, not from on-chain events)
	InternalSnapshot EventType = "internal:Snapshot"
)

// validEventTypes contains all valid event types for quick lookup
var validEventTypes = map[EventType]bool{
	EventBorrow:                          true,
	EventRepay:                           true,
	EventSupply:                          true,
	EventWithdraw:                        true,
	EventLiquidationCall:                 true,
	EventReserveUsedAsCollateralEnabled:  true,
	EventReserveUsedAsCollateralDisabled: true,
	InternalSnapshot:                     true,
}

// IsValid returns true if the EventType is a known valid type
func (e EventType) IsValid() bool {
	return validEventTypes[e]
}

// String returns the string representation of the EventType
func (e EventType) String() string {
	return string(e)
}
