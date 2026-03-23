package entity

import "time"

// AnchoragePackageSnapshot represents a point-in-time snapshot of an Anchorage
// collateral management package. One row per collateral asset per package.
type AnchoragePackageSnapshot struct {
	PrimeID        int64
	PackageID      string
	PledgorID      string
	SecuredPartyID string
	Active         bool
	State          string

	CurrentLTV    string
	ExposureValue string
	PackageValue  string

	MarginCallLTV   string
	CriticalLTV     string
	MarginReturnLTV string

	AssetType          string
	CustodyType        string
	AssetPrice         string
	AssetQuantity      string
	AssetWeightedValue string

	LTVTimestamp time.Time
	SnapshotTime time.Time
}
