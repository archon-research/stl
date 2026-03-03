# Maple Indexer Isolation Plan

## Summary

Create Maple-specific tables and decouple the Maple indexer from the standard `token` and `borrower` tables.

## Why This Approach

The current schema requires every `borrower` and `borrower_collateral` row to reference a `token_id` (FK to the `token` table), which requires `chain_id` + `address`. Maple Finance data doesn't fit this model:
- **Collateral**: Symbol-only assets like "BTC", "XRP", "SOL" - no contract address, no EVM chain
- **Pool assets**: Come from Maple API as just a symbol (USDC, WBTC)
- **No transaction hash**: Maple provides snapshots, not individual transactions

The alternative (protocol_asset indirection) was reverted due to complexity:
- 17 files changed, complex migrations with backfill safety checks
- Nullable dual-FK pattern
- Risk of data corruption if migration fails mid-way

This plan keeps SparkLend/Aave schema clean, avoids nullable FKs, and can be revisited when cross-protocol queries are actually needed.

---

## Implementation Details

### 1. Database Migrations

Reuse existing infrastructure:
- **`user`** table - already stores borrower addresses
- **`protocol`** table - already has Maple Finance entry (seeded in migrations)
- **`chain`** table - for Ethereum mainnet (chain_id = 1)

New tables:
/u
```sql
-- Migration: db/migrations/YYYYMMDD_HHMMSS_create_maple_tables.sql

-- Maple borrower (denominated in pool assets like USDC, WBTC)
CREATE TABLE maple_borrower (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL REFERENCES "user"(id),
    protocol_id     BIGINT NOT NULL REFERENCES protocol(id),
    pool_asset      VARCHAR(50) NOT NULL,           -- e.g., "USDC", "WBTC"
    pool_decimals   INT NOT NULL,
    amount          NUMERIC NOT NULL,               -- current debt
    block_number    BIGINT NOT NULL,
    block_version   INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, protocol_id, pool_asset, block_number, block_version)
);

CREATE INDEX idx_maple_borrower_user ON maple_borrower(user_id);
CREATE INDEX idx_maple_borrower_protocol ON maple_borrower(protocol_id);
CREATE INDEX idx_maple_borrower_block ON maple_borrower(block_number);
CREATE INDEX idx_maple_borrower_block_version ON maple_borrower(block_number, block_version);

-- Maple collateral (denominated in collateral assets like BTC, XRP)
CREATE TABLE maple_collateral (
    id                  BIGSERIAL PRIMARY KEY,
    user_id             BIGINT NOT NULL REFERENCES "user"(id),
    protocol_id         BIGINT NOT NULL REFERENCES protocol(id),
    collateral_asset    VARCHAR(50) NOT NULL,       -- e.g., "BTC", "XRP", "SOL"
    collateral_decimals INT NOT NULL,
    amount              NUMERIC NOT NULL,
    custodian           VARCHAR(100),               -- e.g., "FORDEFI", "ANCHORAGE"
    state               VARCHAR(50),                -- "Deposited" or "DepositPending"
    liquidation_level   NUMERIC,
    block_number        BIGINT NOT NULL,
    block_version       INT NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, protocol_id, collateral_asset, block_number, block_version)
);

CREATE INDEX idx_maple_collateral_user ON maple_collateral(user_id);
CREATE INDEX idx_maple_collateral_protocol ON maple_collateral(protocol_id);
CREATE INDEX idx_maple_collateral_block ON maple_collateral(block_number);
CREATE INDEX idx_maple_collateral_block_version ON maple_collateral(block_number, block_version);

-- Grant permissions
GRANT SELECT ON maple_borrower TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON maple_borrower TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE maple_borrower_id_seq TO stl_readwrite;

GRANT SELECT ON maple_collateral TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON maple_collateral TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE maple_collateral_id_seq TO stl_readwrite;

INSERT INTO migrations (filename) VALUES ('YYYYMMDD_HHMMSS_create_maple_tables.sql')
ON CONFLICT (filename) DO NOTHING;
```

### 2. Domain Entities

- Create `internal/domain/entity/maple_borrower.go`
- Create `internal/domain/entity/maple_collateral.go`

Both entities use `AssetSymbol string` instead of `TokenID int64`.

### 3. Repository Layer (Ports & Adapters)

**Ports**: Create `internal/ports/outbound/maple_position_repository.go`
```go
type MaplePositionRepository interface {
    UpsertBorrower(ctx context.Context, borrower *MapleBorrower) error
    UpsertCollateral(ctx context.Context, collateral *MapleCollateral) error
}
```

**Adapters**: Implement in `internal/adapters/outbound/postgres/maple_position_repository.go`

### 4. Maple Indexer Service Updates

Update `internal/services/maple_indexer/service.go`:
- Replace injected `PositionRepository` with `MaplePositionRepository`
- Remove dependency on `TokenRepository` - no longer needed to look up tokens
- Refactor mapping logic to use new `maple_borrower` and `maple_collateral` entities

### 5. Wiring and Tests

- Update `cmd/maple-indexer/main.go` to construct and inject `MaplePositionRepository`
- Update `internal/services/maple_indexer/service_test.go` to mock new repository
- Run `make test` to verify

---

## Impact

This approach completely isolates Maple data from SparkLend/Aave:
- `sparklend-position-tracker` and core entities are untouched
- Avoids the `protocol_asset` indirection and dummy tokens
- Unblocks development while keeping core components unaffected
- Cross-protocol queries can be added later when needed
