package entity

import "time"

// AnchorageOperation represents a single collateral or exposure operation
// from the Anchorage collateral management API.
type AnchorageOperation struct {
	PrimeID       int64
	OperationID   string
	Action        string // INITIAL_DEPOSIT, TOP_UP, MARGIN_RETURN, PAY_DOWN, etc.
	OperationType string // COLLATERAL_PACKAGE, EXPOSURE, LIQUIDATION
	TypeID        string // packageId or exposureId
	AssetType     string // BTC, USD, etc.
	CustodyType   string // ANCHORAGECUSTODY, EXTERNAL
	Quantity      string
	Notes         string
	CreatedAt     time.Time
}
