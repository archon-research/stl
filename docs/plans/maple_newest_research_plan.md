# Maple Indexer: Internal Loan Tracking Implementation Plan

## Background

Commit [8d723e6](https://github.com/archon-research/stl/commit/8d723e63f045bda8031586b23b0fa9fcc4eee553) added research notes to `docs/maple_spec.md` revealing important findings about the Maple Finance GraphQL API:

1. **Internal vs External Loans**: The `loanMeta` field distinguishes between:
   - **External loans** - Loans to external parties with traditional collateral (`loanMeta` is null)
   - **Internal loans** - Maple's internal positions (`loanMeta.type` = "amm" or "strategy")

2. **Data Quality**: Maple's frontend only reports collateral backing for external parties. Internal loan collateral is filtered out.

3. **Sky Strategies**: Additional internal DeFi positions that may contribute to pool backing (out of scope for this plan).

## Current State

The current Maple indexer:
- Uses `GetAllActiveLoansAtBlock()` to fetch all active loans
- Does **NOT** query the `loanMeta` field
- Does **NOT** distinguish internal vs external loans
- Stores all loans without classification

## Goal

Track `loan_type` for all Maple loans to enable filtering internal vs external loans at query time.

Internal loans can be identified with: `loan_type IN ('amm', 'strategy')`
External loans can be identified with: `loan_type IS NULL`

---

## Implementation Tasks

### 1. Update Ports (`internal/ports/outbound/maple_client.go`)

- [ ] Add `MapleLoanMeta` struct:
  ```go
  type MapleLoanMeta struct {
      Type          string // "amm", "strategy", or empty
      AssetSymbol   string
      DexName       string
      Location      string
      WalletAddress string
      WalletType    string
  }
  ```
- [ ] Add `LoanMeta *MapleLoanMeta` field to `MapleActiveLoan` struct

### 2. Update GraphQL Client (`internal/adapters/outbound/maple/client.go`)

- [ ] Add `loanMeta` to the GraphQL query:
  ```graphql
  loanMeta {
      type
      assetSymbol
      dexName
      location
      walletAddress
      walletType
  }
  ```
- [ ] Add internal response types for JSON unmarshaling
- [ ] Parse nullable `loanMeta` response and map to `outbound.MapleLoanMeta`

### 3. Update Domain Entities

- [ ] `internal/domain/entity/maple_borrower.go`: Add `LoanType string` field
- [ ] `internal/domain/entity/maple_collateral.go`: Add `LoanType string` field

### 4. Database Migration

- [ ] Create `db/migrations/20260303_110000_add_maple_loan_type.sql`:
  ```sql
  ALTER TABLE maple_borrower ADD COLUMN loan_type VARCHAR(50);
  ALTER TABLE maple_collateral ADD COLUMN loan_type VARCHAR(50);
  
  INSERT INTO migrations (filename) VALUES ('20260303_110000_add_maple_loan_type.sql')
  ON CONFLICT (filename) DO NOTHING;
  ```

### 5. Update Repository (`internal/adapters/outbound/postgres/maple_position_repository.go`)

- [ ] Update `saveBorrowerBatch()` INSERT statement to include `loan_type`
- [ ] Update `saveCollateralBatch()` INSERT statement to include `loan_type`
- [ ] Update ON CONFLICT clauses to handle `loan_type` in updates

### 6. Update Service (`internal/services/maple_indexer/service.go`)

- [ ] Update `buildEntities()` to populate `LoanType` from `loan.LoanMeta.Type`

### 7. Update Tests

- [ ] `internal/adapters/outbound/maple/client_test.go`: Add test cases for loanMeta parsing (null and non-null)
- [ ] `internal/services/maple_indexer/service_test.go`: Add test cases for loan type propagation
- [ ] `internal/domain/entity/maple_borrower_test.go`: Update if validation changes
- [ ] `internal/domain/entity/maple_collateral_test.go`: Update if validation changes

---

## Files to Modify

| File | Action |
|------|--------|
| `stl-verify/internal/ports/outbound/maple_client.go` | Add `MapleLoanMeta`, update `MapleActiveLoan` |
| `stl-verify/internal/adapters/outbound/maple/client.go` | Update query, response types, parsing |
| `stl-verify/internal/domain/entity/maple_borrower.go` | Add `LoanType` field |
| `stl-verify/internal/domain/entity/maple_collateral.go` | Add `LoanType` field |
| `stl-verify/db/migrations/20260303_110000_add_maple_loan_type.sql` | **New file** |
| `stl-verify/internal/adapters/outbound/postgres/maple_position_repository.go` | Update INSERT statements |
| `stl-verify/internal/services/maple_indexer/service.go` | Update `buildEntities()` |
| `stl-verify/internal/adapters/outbound/maple/client_test.go` | Add loanMeta tests |
| `stl-verify/internal/services/maple_indexer/service_test.go` | Add loan type tests |

---

## Out of Scope (Future Work)

1. **Sky Strategies** - Internal DeFi positions that may contribute to pool backing. Requires additional research to determine if they should be tracked.

2. **Pool-level collateral aggregation** - The research notes a discrepancy between `collateralValue` and sum of `assetValueUsd`. This doesn't affect per-loan tracking but may be relevant for future pool-level analytics.

---

## Verification

After implementation:

```bash
make ci           # Unit tests
make test-race      # Unit tests with race detector
```

Query to verify loan types are being tracked:
```sql
SELECT loan_type, COUNT(*) 
FROM maple_borrower 
GROUP BY loan_type;
```
