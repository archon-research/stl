# Protocol Asset Registry Design

## Problem Statement

The current `Token` table assumes all assets have:
- A numeric `chain_id` (EVM chain identifier)
- A contract `address` (ERC20 token address)
- A unique `symbol` per chain

This doesn't work for protocols like Maple Finance where:
- Collateral assets are identified by symbol/enum only (e.g., "BTC", "XRP")
- Many assets are non-EVM (Bitcoin, XRP, Solana, etc.) with no contract address
- The `collateralBlockchain` field is nullable and uses a string enum, not numeric chain ID
- Symbol uniqueness isn't guaranteed across chains

### Current Maple Data Structure

From the Maple GraphQL API:
```graphql
type Collateral {
  asset: CollateralAsset!        # Enum: BTC, XRP, weETH, PYUSD, etc.
  collateralBlockchain: Blockchain  # Nullable enum: Ethereum, Bitcoin, XRP, etc.
  decimals: Int!
  assetAmount: BigInt!
  assetValueUsd: Float!
  # ... no token address
}
```

The `CollateralAsset` enum includes: BTC, XRP, weETH, PYUSD, SOL, and many others.
The `Blockchain` enum includes: Ethereum, Bitcoin, XRP, Solana, Hyper, Unknown, etc.

## Proposed Solution

Create a global `protocol_asset` table that serves as a registry for all protocol-specific assets, with optional linking to the canonical `Token` table when an ERC20 address exists.

### Database Schema

```sql
CREATE TABLE protocol_asset (
    id BIGSERIAL PRIMARY KEY,
    protocol_id BIGINT NOT NULL REFERENCES protocol(id),
    asset_key TEXT NOT NULL,           -- Protocol's asset identifier (e.g., Maple's CollateralAsset enum)
    symbol TEXT NOT NULL,              -- Display symbol
    decimals INT NOT NULL,
    blockchain TEXT,                   -- Protocol's blockchain identifier (nullable for unknown)
    chain_id BIGINT,                   -- Numeric EVM chain ID (nullable for non-EVM)
    address BYTEA,                     -- Token contract address (nullable for non-EVM)
    token_id BIGINT REFERENCES token(id),  -- Link to canonical token when available
    metadata JSONB,                    -- Extra protocol-specific fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE(protocol_id, asset_key, blockchain)
);

CREATE INDEX idx_protocol_asset_protocol_id ON protocol_asset(protocol_id);
CREATE INDEX idx_protocol_asset_token_id ON protocol_asset(token_id) WHERE token_id IS NOT NULL;
```

### Entity Changes

Update `BorrowerCollateral` to support either token reference:

```go
type BorrowerCollateral struct {
    ID                int64
    UserID            int64
    ProtocolID        int64
    TokenID           *int64  // Nullable - for Spark/Aave (direct token reference)
    ProtocolAssetID   *int64  // Nullable - for Maple (protocol asset reference)
    BlockNumber       int64
    BlockVersion      int
    Amount            *big.Int
    Change            *big.Int
    EventType         EventType
    TxHash            []byte
    CollateralEnabled bool
}

// Validation: exactly one of TokenID or ProtocolAssetID must be set
```

Similarly for `Borrower`:

```go
type Borrower struct {
    ID              int64
    UserID          int64
    ProtocolID      int64
    TokenID         *int64  // Nullable - for Spark/Aave
    ProtocolAssetID *int64  // Nullable - for Maple
    // ... rest unchanged
}
```

## Seeding Strategy: Auto-Discovery + Optional Linking

### Primary Mechanism: Auto-Discovery

The Maple indexer automatically creates `protocol_asset` records on first encounter:

```go
func (s *Service) getOrCreateProtocolAsset(ctx context.Context, tx pgx.Tx, collateral MapleCollateral) (int64, error) {
    // Try to find existing
    assetID, err := s.assetRepo.GetProtocolAssetByKey(ctx, tx, s.protocolID, collateral.Asset, collateral.CollateralBlockchain)
    if err == nil {
        return assetID, nil
    }
    
    // Not found - create it
    newAsset := entity.ProtocolAsset{
        ProtocolID: s.protocolID,
        AssetKey:   collateral.Asset,              // "BTC"
        Symbol:     collateral.Asset,              // Use same as key initially
        Decimals:   collateral.Decimals,
        Blockchain: collateral.CollateralBlockchain, // "Bitcoin" or nil
        ChainID:    inferChainID(collateral.CollateralBlockchain),
        Address:    nil,
        TokenID:    nil,
    }
    
    return s.assetRepo.CreateProtocolAsset(ctx, tx, newAsset)
}

func inferChainID(blockchain *string) *int64 {
    if blockchain == nil {
        return nil
    }
    switch *blockchain {
    case "Ethereum":
        return ptr(int64(1))
    case "Base":
        return ptr(int64(8453))
    case "Avalanche":
        return ptr(int64(43114))
    // ... other EVM chains
    default:
        return nil  // Non-EVM or unknown
    }
}
```

### Secondary Mechanism: Optional Token Linking

After creating a protocol asset, attempt to link it to an existing Token:

```go
func (s *Service) linkToTokenIfPossible(ctx context.Context, tx pgx.Tx, assetID int64, collateral MapleCollateral) error {
    // Only try to link EVM assets
    if collateral.CollateralBlockchain == nil || *collateral.CollateralBlockchain != "Ethereum" {
        return nil
    }
    
    chainID := inferChainID(collateral.CollateralBlockchain)
    if chainID == nil {
        return nil
    }
    
    // Try to find matching token by symbol
    tokenID, err := s.tokenRepo.GetTokenIDBySymbol(ctx, *chainID, collateral.Asset)
    if err != nil {
        s.logger.Debug("no matching token found", "asset", collateral.Asset, "chain", *chainID)
        return nil  // Not an error - just can't link
    }
    
    return s.assetRepo.UpdateProtocolAssetTokenLink(ctx, tx, assetID, tokenID)
}
```

### Benefits of This Approach

| Benefit | Description |
|---------|-------------|
| Zero config startup | No manual seeding required |
| Self-healing | New Maple assets auto-discovered |
| Progressive enhancement | Token links added when possible |
| No blocking failures | Missing token link doesn't prevent ingestion |
| Manual corrections | Can fix/enhance data post-hoc via scripts |

## Migration Path

### Phase 1: Add New Tables and Columns

1. Create `protocol_asset` table
2. Add nullable `protocol_asset_id` column to `borrower` and `borrower_collateral`
3. Keep existing `token_id` column (nullable for backward compatibility)

### Phase 2: Update Maple Indexer

1. Replace `getTokenID()` with `getOrCreateProtocolAsset()`
2. Set `protocol_asset_id` instead of `token_id` for Maple positions
3. Optionally call `linkToTokenIfPossible()` for EVM assets

### Phase 3: Update Queries (if needed)

Update any queries that JOIN on `token_id` to also handle `protocol_asset_id`:

```sql
-- Example: Get collateral with asset info
SELECT bc.*, 
       COALESCE(t.symbol, pa.symbol) as asset_symbol,
       COALESCE(t.decimals, pa.decimals) as asset_decimals
FROM borrower_collateral bc
LEFT JOIN token t ON bc.token_id = t.id
LEFT JOIN protocol_asset pa ON bc.protocol_asset_id = pa.id
WHERE bc.user_id = $1;
```

## Repository Interface

```go
// outbound/protocol_asset.go
type ProtocolAssetRepository interface {
    GetProtocolAssetByKey(ctx context.Context, tx pgx.Tx, protocolID int64, assetKey string, blockchain *string) (int64, error)
    CreateProtocolAsset(ctx context.Context, tx pgx.Tx, asset entity.ProtocolAsset) (int64, error)
    UpdateProtocolAssetTokenLink(ctx context.Context, tx pgx.Tx, assetID int64, tokenID int64) error
    GetProtocolAssetByID(ctx context.Context, id int64) (*entity.ProtocolAsset, error)
}
```

## Open Questions

1. **Constraint enforcement**: Should we add a database CHECK constraint that exactly one of `token_id` or `protocol_asset_id` is set? Or handle in application code?

2. **Spark/Aave migration**: Should existing Spark/Aave data eventually migrate to use `protocol_asset`, or keep using `token_id` directly?

3. **Symbol conflicts**: If Maple's "USDC" on Ethereum maps to our Token table's USDC, but decimals differ, which takes precedence?

4. **Historical data**: For existing Maple positions (if any), do we backfill `protocol_asset_id`?

## Files to Modify

| File | Changes |
|------|---------|
| `db/migrations/XXX_create_protocol_asset.sql` | New migration |
| `internal/domain/entity/protocol_asset.go` | New entity |
| `internal/domain/entity/borrower.go` | Add `ProtocolAssetID` |
| `internal/domain/entity/borrower_collateral.go` | Add `ProtocolAssetID` |
| `internal/ports/outbound/protocol_asset.go` | New repository interface |
| `internal/adapters/outbound/postgres/protocol_asset.go` | New repository implementation |
| `internal/services/maple_indexer/service.go` | Use `getOrCreateProtocolAsset()` |
| `docs/entity_relation.md` | Update ER diagram |
