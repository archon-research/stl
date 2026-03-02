package entity

// ProtocolAsset is the bridge between a protocol and the underlying token it uses.
// EVM assets use the lowercase hex contract address as AssetKey to avoid symbol
// collisions. Non-EVM / symbol-only assets (e.g. Maple's BTC, XRP, SOL) use
// the symbol string as AssetKey.
type ProtocolAsset struct {
	ID         int64
	ProtocolID int64
	AssetKey   string // EVM: lowercase hex address; non-EVM: symbol
	Symbol     string
	Decimals   int
	ChainID    *int64 // nil for non-EVM assets
	Address    []byte // nil for non-EVM assets
	TokenID    *int64 // nil if no matching canonical token exists
}
