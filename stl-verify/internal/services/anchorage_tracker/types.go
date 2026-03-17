package anchorage_tracker

// ---------------------------------------------------------------------------
// Anchorage API response types (/v2/collateral_management/packages)
// ---------------------------------------------------------------------------

// PackagesResponse is the top-level response from the Anchorage API.
type PackagesResponse struct {
	Data []Package `json:"data"`
	Page PageInfo  `json:"page"`
}

type PageInfo struct {
	Next *string `json:"next"`
}

type Package struct {
	Active            bool               `json:"active"`
	ClientReferenceID string             `json:"clientReferenceId"`
	CollateralAssets  []CollateralAsset  `json:"collateralAssets"`
	Critical          MarginConfig       `json:"critical"`
	CurrentLTV        string             `json:"currentLtv"`
	ExposureValue     string             `json:"exposureValue"`
	LTVTimestamp      string             `json:"ltvTimestamp"`
	MarginCall        MarginConfig       `json:"marginCall"`
	MarginReturn      MarginReturnConfig `json:"marginReturn"`
	PackageID         string             `json:"packageId"`
	PackageValue      string             `json:"packageValue"`
	PledgorID         string             `json:"pledgorId"`
	SecuredPartyID    string             `json:"securedPartyId"`
	State             string             `json:"state"`
}

type CollateralAsset struct {
	Asset         AssetInfo `json:"asset"`
	Price         string    `json:"price"`
	Quantity      string    `json:"quantity"`
	Weight        string    `json:"weight"`
	WeightedValue string    `json:"weightedValue"`
}

type AssetInfo struct {
	AssetType string `json:"assetType"`
	Type      string `json:"type"`
}

type MarginConfig struct {
	Action      string `json:"action"`
	LTV         string `json:"ltv"`
	ReturnToLTV string `json:"returnToLtv"`
	WarningLTV  string `json:"warningLtv"`
}

type MarginReturnConfig struct {
	Action      string `json:"action"`
	LTV         string `json:"ltv"`
	ReturnToLTV string `json:"returnToLtv"`
}

// ---------------------------------------------------------------------------
// Anchorage API response types (/v2/collateral_management/operations)
// ---------------------------------------------------------------------------

// OperationsResponse is the top-level response from the operations endpoint.
type OperationsResponse struct {
	Data []Operation `json:"data"`
	Page PageInfo    `json:"page"`
}

type Operation struct {
	Action                     string     `json:"action"`
	Asset                      AssetInfo  `json:"asset"`
	CreatedAt                  string     `json:"createdAt"`
	ID                         string     `json:"id"`
	LiquidationCollateralPkgID string     `json:"liquidationCollateralPackageId,omitempty"`
	LiquidationProceedAsset    *AssetInfo `json:"liquidationProceedAsset,omitempty"`
	LiquidationProceedQuantity string     `json:"liquidationProceedQuantity,omitempty"`
	Notes                      string     `json:"notes"`
	Quantity                   string     `json:"quantity"`
	Type                       string     `json:"type"`
	TypeID                     string     `json:"typeId"`
	UpdatedAt                  string     `json:"updatedAt"`
}
