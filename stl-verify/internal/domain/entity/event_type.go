package entity

// EventType represents the type of SparkLend position-changing event
type EventType string

const (
	EventBorrow                          EventType = "Borrow"
	EventRepay                           EventType = "Repay"
	EventSupply                          EventType = "Supply"
	EventWithdraw                        EventType = "Withdraw"
	EventLiquidationCall                 EventType = "LiquidationCall"
	EventReserveUsedAsCollateralEnabled  EventType = "ReserveUsedAsCollateralEnabled"
	EventReserveUsedAsCollateralDisabled EventType = "ReserveUsedAsCollateralDisabled"
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
}

// IsValid returns true if the EventType is a known valid type
func (e EventType) IsValid() bool {
	return validEventTypes[e]
}

// String returns the string representation of the EventType
func (e EventType) String() string {
	return string(e)
}
