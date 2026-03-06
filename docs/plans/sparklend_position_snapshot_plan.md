# Sparklend Position Snapshot Plan

| Field | Value |
|---|---|
| Status | Proposed |
| Date | 2026-03-06 |
| Area | `sparklend_position_tracker` |

## Context

The current SparkLend/Aave position tracking path mixes two different persistence models:

- Collateral is persisted from on-chain snapshot data fetched through protocol data providers.
- Debt is persisted from event deltas by writing `event.amount` into the borrower table.

This creates an inconsistency in the meaning of the `amount` column:

- For collateral, `amount` is close to current state at the processed block.
- For debt, `amount` is currently just the event delta.

The existing code already proves that the UI data provider and protocol data provider expose the fields needed to snapshot both sides of the position:

- `getUserReservesData` identifies the assets where the user currently has supplied balance and/or debt.
- `getUserReserveData` exposes the current reserve-level balances, including:
  - `CurrentATokenBalance`
  - `CurrentVariableDebt`
  - `CurrentStableDebt`

The current `change` column exists in the schema, but it does not appear to have an active consumer today. Under the current uniqueness model and append-only write semantics, it is also not a reliable per-event delta field when the same user/token is touched multiple times in one block.

## Goals

- Make debt persistence snapshot-based, just like collateral.
- Keep event-triggered writes so positions are still recorded at the blocks where relevant activity occurred.
- Store full snapped position in `amount`.
- Keep `change` simple and non-misleading for this iteration.
- Reuse the existing provider-based path as much as possible.
- Avoid large schema changes or a major redesign of repository behavior.

## Non-Goals

- Do not change the uniqueness model of borrower or borrower collateral rows.
- Do not redesign all historical snapshot semantics across the codebase.
- Do not introduce per-log indexing keys in this iteration.
- Do not compute `change` by diffing against the previous persisted row in this iteration.
- Do not introduce event-delta semantics into snapshot rows in this iteration.

## Decision

Persist one row per `(user_id, protocol_id, token_id, block_number, block_version)` as we do today, but change the payload semantics:

- `amount` stores the full current snapped balance at the block.
- `change` is written as `0` for snapshot rows in this iteration.

This applies to both borrower and borrower collateral records.

### Row Semantics Under The Current Key

- `amount` is the final block-state snapshot returned by provider calls at `blockNumber`.
- `event_type` and `tx_hash` remain metadata for the triggering event that caused us to snapshot.
- Because the unique key is `(user_id, protocol_id, token_id, block_number, block_version)` and writes use `ON CONFLICT DO NOTHING`, same-token multiple writes in one block are still first-write-wins.
- This means the persisted row is best understood as a block snapshot row, not a guaranteed per-log snapshot row.

For `LiquidationCall`, the first implementation should prioritize correct snapshots. Store:

- correct snapped `amount`
- `change = 0`

This keeps semantics clear while still improving the primary position snapshot model.

## Why This Approach

This delivers the requested behavioral improvement with limited restructuring:

- The service already fetches reserve snapshots from provider contracts.
- The blockchain service already exposes the debt fields we need.
- The main required change is to stop treating debt like an event ledger and instead persist debt snapshots.
- Repository interfaces only need a modest cleanup so `change` can be written deliberately as zero instead of implicitly mirroring `amount`.

## Current Constraints

### Repository API limitation

Today the repository save methods only accept one numeric string and write it into both `amount` and `change`. That prevents storing:

- snapped `amount`
- explicit zero `change`

This must be adjusted before the new semantics can be persisted correctly.

### Supplied positions vs collateral-enabled positions

The existing extraction path is collateral-centric and filters to assets where collateral usage is enabled.

For snapshot correctness, we should broaden this to supplied-position snapshots:

- persist supplied balances even when `collateral_enabled = false`
- persist the `collateral_enabled` flag alongside the supplied balance

This avoids losing valid supplied balances after collateral toggles or in cases where supplied assets are present but not enabled as collateral.

### Uniqueness model

The current unique key is:

- `(user_id, protocol_id, token_id, block_number, block_version)`

This means:

- different tokens in the same block are fine
- different block versions in reorgs are fine
- multiple writes for the same user/protocol/token/block version will conflict

That is acceptable for a per-token final block snapshot model.

It is not sufficient for storing multiple snapshots for the same token within the same block.

### No schema migration required

The current schema already contains:

- `borrower.amount`
- `borrower.change`
- `borrower_collateral.amount`
- `borrower_collateral.change`

This iteration is a service/repository semantics change, not a schema redesign.

### Transaction boundary note

`protocol_event` persistence currently happens in a separate transaction from position snapshot persistence. This plan does not change that behavior.

## Implementation Plan

### 1. Introduce unified position extraction

Refactor the provider-driven extraction path so a single flow can derive both:

- supplied-position snapshots
- debt snapshots

Suggested direction:

- keep `getUserReservesData` as the discovery call
- use it to collect all relevant reserve assets for the user
- call `getUserReserveData` for those assets
- derive per-token snapshots from the returned balances

The extracted model should include enough information to write:

- token metadata
- supplied balance
- debt balance
- collateral-enabled flag for supplied rows

Using one unified extraction path keeps provider-specific logic in one place and avoids duplicated reserve scanning logic.

### 2. Snapshot debt from providers

Replace the current debt write path in borrow/repay handling so that:

- borrower `amount` comes from the provider snapshot
- borrower `change` is stored as zero

Debt amount should be derived as:

- `CurrentVariableDebt + CurrentStableDebt`

If a protocol/version has no stable debt component for that reserve, the stable side is zero.

### 3. Keep supplied-position snapshots provider-based

For supply/withdraw and collateral toggle flows:

- supplied-position `amount` continues to come from the provider snapshot
- supplied-position `change` is stored as zero
- the `collateral_enabled` flag continues to be persisted from provider state

This preserves existing snapshot behavior while keeping the model simple and honest.

### 4. Expand repository contracts

Update the position repository port and postgres adapter so borrower and supplied-position writes do not implicitly mirror `amount` into `change`.

This likely requires:

- changing `SaveBorrower`
- changing `SaveBorrowerCollateral`
- likely introducing small write helper types instead of growing positional arguments further
- updating batch insert SQL to write `change = 0` intentionally

The append-only `ON CONFLICT DO NOTHING` behavior can remain unchanged in this iteration.

### 5. Extend liquidation snapshots carefully

Update liquidation handling so the snapshot path persists:

- borrower debt snapshots
- supplied-position snapshots

For the first iteration, liquidation snapshot rows should persist `change = 0`.

The implementation should explicitly consider the expected rows for:

- borrower on the debt asset
- borrower on the supplied/collateral asset
- liquidator on the supplied/collateral asset when a balance exists after the transaction

### 6. Add and update tests

Tests should cover at least:

- borrower rows store snapped full debt in `amount`
- borrower rows store zero `change`
- supplied-position rows store snapped full balance in `amount`
- supplied-position rows store zero `change`
- supplied-but-not-enabled assets are still persisted with `collateral_enabled = false`
- liquidation snapshots persist debt as well as supplied positions
- multiple tokens for the same user/block persist independently
- same-token duplicate writes in the same block still preserve first-write-wins behavior
- provider data unavailable at a historical block is handled safely
- zero debt / zero supplied balance suppression rules are covered explicitly

## Expected Semantics After Change

For a `Borrow` event on token X:

- `amount` = total current debt on token X after the transaction at that block
- `change` = `0`

For a `Repay` event on token X:

- `amount` = total current debt on token X after the transaction at that block
- `change` = `0`

For a `Supply` event on token Y:

- `amount` = total current supplied balance on token Y after the transaction at that block
- `change` = `0`

For a `Withdraw` event on token Y:

- `amount` = total current supplied balance on token Y after the transaction at that block
- `change` = `0`

For a collateral enable/disable event:

- `amount` = current snapped supplied balance
- `change` = zero

## Risks and Edge Cases

- Some events may be emitted multiple times for the same user/token in the same block. With the current unique key and append-only behavior, only the first persisted row survives.
- If the surviving row is not the final event for that token in the block, the stored snapshot may depend on processing order.
- The current service logic is oriented around collateral-enabled balances; supplied-position extraction must be widened carefully so valid balances are not lost.
- Liquidation events affect multiple users and assets, so borrower and supplied-position expectations need to be validated carefully.
- Historical provider availability still depends on the correct data provider being configured for the queried block.

## Follow-Up Options

If we later need exact per-event deltas or exact per-event snapshots within a single block, we should evaluate one of these follow-ups:

- add transaction/log index to uniqueness
- switch to explicit upsert semantics for final-in-block state
- store a separate event-delta ledger alongside block snapshots

## Acceptance Criteria

- Debt is no longer persisted from `event.amount` as the row `amount`.
- Borrower `amount` reflects snapped current debt.
- Borrower `change` is written deliberately as zero for snapshot rows.
- Borrower collateral `amount` reflects snapped current supplied balance.
- Borrower collateral `change` is written deliberately as zero for snapshot rows.
- Borrower collateral rows are persisted even when `collateral_enabled = false` and supplied balance remains non-zero.
- Liquidation snapshots include debt as well as supplied positions.
- Existing block version behavior for reorgs continues to work unchanged.
