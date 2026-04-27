# Verify Beacon Risk Model Refactor Plan

| Field | Value |
|-------|-------|
| **Status** | In progress (implementation complete, integration test run pending) |
| **Date** | 2026-04-27 |
| **Issues** | VEC-177, VEC-179, VEC-182 |
| **Base branch** | VEC-175 |

## Context

This document covers one coherent implementation unit across three tightly related tasks:

- `VEC-177` introduce the shared `RiskModel` port and `RrcResult` types
- `VEC-179` port `SurafRrcService` to the new `RiskModel` interface
- `VEC-182` port gap-sweep to the new `RiskModel` interface and translate bad debt to RRC

These tasks should not be implemented independently. The shared abstraction must be shaped by the actual needs of both SURAF and gap-sweep so that later work can replace the current factory with a model registry without immediately reworking the model contract.

This work is explicitly on top of `VEC-175`, so `Settings.risk_default_gap_pct`, `RISK_DEFAULT_GAP_PCT`, and the existing config plumbing are assumed to already exist.

## Goals

- Define a small shared `RiskModel` port that works for both real models
- Port SURAF to that interface without losing current behavior
- Port gap-sweep to that interface without changing its math
- Add shared domain result types that make model-specific details explicit
- Keep temporary compatibility shims thin and isolated
- Add tests that prove the abstraction is correct

## Non-goals

- Do not implement the model registry from `VEC-178`
- Do not do the final unified `/v1/risk/rrc` endpoint cutover from `VEC-183`
- Do not broadly redesign unrelated allocation or pricing infrastructure
- Do not guess missing derived inputs; raise typed errors instead

## Working Decisions

### External identifier shape

For this change set, shared model entry points should use:

- `AssetId = int`
- `PrimeId = EthAddress`

`AssetId` should mean the current external asset identifier already used by the API surface for this work. In practice, that means the current receipt-token-facing asset id, which each concrete model can translate internally as needed.

This keeps the shared abstraction anchored in the existing API and avoids prematurely introducing a second public identifier scheme before registry and endpoint work lands.

### Canonical SURAF identity

SURAF should be shaped around resolved allocation assets, not around free-form asset symbols.

For this PR, the current symbol-based SURAF mapping remains a compatibility layer, but symbol should not become the canonical cross-model identifier. The resolved SURAF position should carry enough identity to support the later move to a more explicit rating source such as `{chain_id, asset_address, crr}`.

### Default USD exposure behavior

When SURAF `usd_exposure` is omitted, the model should derive it from the resolved position.

If the position exists but the service cannot derive a USD-valued exposure for it, the model should raise a typed compute-time error rather than guessing, returning zero, or silently becoming non-applicable.

### Non-applicability behavior

The model contract should distinguish between:

- missing position for the requested `(asset_id, prime_id)`
- known position that the model does not apply to
- applicable position that cannot be computed because required derived inputs are unavailable or invalid

That distinction is important for later handler mapping and for the upcoming registry work.

## Proposed Design

### Shared port

Create `app/ports/risk_model.py` with a minimal async protocol:

```python
class RiskModel(Protocol):
    async def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool: ...
    async def compute(
        self,
        asset_id: int,
        prime_id: EthAddress,
        overrides: Mapping[str, Decimal] | None = None,
    ) -> RrcResult: ...
```

Notes:

- The interface should stay async because both real models need I/O.
- `overrides` should remain a generic mapping in the shared port.
- Validation of allowed keys and value ranges should remain model-specific.

### Shared risk domain types

Extend `app/domain/entities/risk.py` with:

- `AssetId = int`
- `PrimeId = EthAddress`
- `RrcResult`
- discriminated `details` types for each concrete model
- shared typed error classes used by model implementations

`RrcResult` should stay intentionally small. The shared fields should be the ones every model can supply without distortion, and model-specific outputs should go into `details`.

Recommended shape:

```python
class RrcResult(BaseModel):
    asset_id: int
    prime_id: EthAddress
    rrc_usd: Decimal
    details: SurafRrcDetails | GapSweepRrcDetails
```

Recommended detail families:

- `SurafRrcDetails`
  - model discriminator
  - asset display fields
  - derived or overridden `usd_exposure`
  - rating identifier and version metadata
  - `crr_pct`
  - source commit sha
- `GapSweepRrcDetails`
  - model discriminator
  - protocol and asset metadata
  - effective `gap_pct`
  - backed asset identifier used internally
  - optional count or summary fields that explain the result without copying the full breakdown

The details payloads should expose what the caller needs to understand the result, but should not duplicate whole intermediate datasets.

### Shared error types

Add typed exceptions for at least:

- missing position for `(asset_id, prime_id)`
- invalid overrides
- model cannot derive required inputs for a position it otherwise applies to

These should be shared because both models benefit from consistent handler behavior and test expectations.

## Required New Seam: Risk Position Resolution

The current Python codebase does not have an existing seam that resolves `(asset_id, prime_id)` into a single risk position with enough information for both SURAF and gap-sweep.

Existing allocation reads return token balances, but not a complete risk-oriented resolved position and not a derived USD exposure.

Add a small risk-specific repository or port that resolves a single position by `(asset_id, prime_id)` and returns the fields needed by the models. The returned shape should include enough identity and display information to support:

- SURAF rating lookup today via symbol compatibility
- SURAF rating lookup later via chain and address identity
- gap-sweep protocol dispatch and asset translation
- derived USD exposure when available

Recommended resolved fields:

- `chain_id`
- `prime_id`
- `receipt_token_id`
- `receipt_token_address`
- `underlying_token_id`
- `underlying_token_address`
- `symbol`
- `underlying_symbol`
- `protocol_name`
- `balance`
- `usd_exposure` when derivable from current pricing data

The resolver should be strict. If no position matches, it should return a typed not-found outcome. If a position exists but cannot be valued, that should remain visible to the caller rather than being hidden in the resolver.

## SURAF Plan

Port `SurafRrcService` to the new `RiskModel` interface.

### Behavior

- New signature: `compute(asset_id, prime_id, overrides)`
- Accept only `usd_exposure`
- Reject unknown override keys with a typed validation error
- Require `usd_exposure > 0` when provided
- If `usd_exposure` is omitted, derive it from the resolved position
- Raise typed not-found if the position does not exist
- Return `RrcResult` with `SurafRrcDetails`

### Applicability

`applies_to(asset_id, prime_id)` should:

1. resolve the position
2. determine the asset identity needed for rating lookup
3. return `True` only if a SURAF rating exists for that resolved asset

This means:

- missing position is not the same thing as non-applicable
- unrated assets should be non-applicable, not an error
- applicable but non-valued assets should still fail at compute time if default exposure is requested and cannot be derived

### Compatibility shim

Keep `/v1/risk/rrc/scenario` working through a thin shim.

The cleanest approach is to extract the pure calculation path that turns a resolved rating and a validated exposure into a SURAF result, then reuse that helper from:

- the new model implementation
- the legacy scenario endpoint

That keeps current behavior alive without making the shared model abstraction depend on the old symbol-only scenario contract.

## Gap-sweep Plan

Port gap-sweep to the same `RiskModel` interface.

### Behavior

- New signature: `compute(asset_id, prime_id, overrides)`
- Accept only `gap_pct`
- Validate `gap_pct` in `[0, 1]`
- Default to `Settings.risk_default_gap_pct`
- Preserve current math and sign conventions internally
- Return positive `rrc_usd`
- Return `RrcResult` with `GapSweepRrcDetails`

### Applicability

`applies_to(asset_id, prime_id)` should return `True` only for supported lending protocol positions.

The applicability decision should be based on the resolved position's protocol metadata, not on ad hoc knowledge embedded in later registry wiring.

### Construction and protocol dispatch

Move Aave-like vs Morpho dispatch into the gap-sweep construction path.

That keeps the later registry work model-agnostic and lets the new shared interface depend only on a resolved position and a concrete model implementation.

For this prime-based path:

- Aave-like models should use the supplied `prime_id` directly as the wallet identity for allocation share lookup
- Morpho should continue to use `FixedAllocationShare(1)` because the breakdown is already vault-scoped

Any older receipt-token-only wallet lookup behavior should survive only in compatibility code that still lacks `prime_id`.

## Compatibility Boundaries

This PR should leave the codebase in a transitional but coherent state.

What should be migrated now:

- shared `RiskModel` port
- shared `RrcResult` and detail types
- SURAF implementation on the new interface
- gap-sweep implementation on the new interface

What should remain temporarily compatible:

- legacy SURAF scenario endpoint
- existing gap-sweep bad-debt and breakdown endpoints where needed until the unified endpoint work lands

What should not be pulled in now:

- model registry
- final endpoint replacement
- frontend or consumer cutover

## Suggested Implementation Order

1. Add shared types and the new `RiskModel` port.
2. Add fake model support and shared tests for the abstraction in isolation.
3. Add the new risk position resolver seam and its tests.
4. Port SURAF to the new interface.
5. Add the SURAF compatibility shim for `/risk/rrc/scenario`.
6. Port gap-sweep to the new interface.
7. Reuse existing compatibility handlers where appropriate without doing final endpoint cutover.
8. Run unit and integration tests and tighten any result or error contracts that prove awkward.

## Test Plan

Add or update tests to prove:

- the shared port and types work with a fake implementation
- SURAF behavior works through the new interface
- gap-sweep behavior works through the new interface
- override validation is strict and model-specific
- non-applicability behavior is distinct from not-found behavior
- SURAF missing-position and missing-derived-exposure behavior are typed and explicit
- gap-sweep still preserves current internal math and returns positive outward-facing `rrc_usd`
- the legacy scenario endpoint still works through its shim

If the new position resolver computes USD exposure from DB state, it should have direct tests. That seam is important enough that it should not be covered only indirectly.

## Risks

### Shared abstraction grows too broad

Mitigation: keep the port minimal, keep override validation model-specific, and put model-specific fields in discriminated `details` types.

### SURAF identity remains too tied to symbols

Mitigation: treat symbol lookup as a temporary adapter on top of a resolved position shape that already carries chain and address identity.

### Gap-sweep behavior changes accidentally during porting

Mitigation: preserve the existing math functions unchanged and assert current sign conventions in tests.

### Compatibility code leaks into the final shape

Mitigation: isolate shims at the API boundary and construction boundary only.

## Implementation Checklist

- [x] Add `app/ports/risk_model.py` with the shared async `RiskModel` protocol
- [x] Extend `app/domain/entities/risk.py` with `AssetId`, `PrimeId`, `RrcResult`, detail types, and shared errors
- [x] Add a fake test implementation for the shared `RiskModel` contract
- [x] Add shared tests that prove the port and result types work in isolation
- [x] Add a risk-specific position resolver seam for `(asset_id, prime_id)`
- [x] Add tests for the position resolver, including missing-position behavior
- [x] Port `SurafRrcService` to `RiskModel`
- [x] Validate SURAF overrides strictly: only `usd_exposure`, must be positive, unknown keys rejected
- [x] Derive SURAF `usd_exposure` from the resolved position when omitted
- [x] Raise a typed not-found error for missing SURAF positions
- [x] Populate `RrcResult.details` with SURAF-specific fields
- [x] Implement SURAF `applies_to(asset_id, prime_id)` based on resolved rating availability
- [x] Keep `/v1/risk/rrc/scenario` working via a thin shim
- [x] Port gap-sweep to `RiskModel`
- [x] Validate gap-sweep overrides strictly: only `gap_pct`, must be in `[0, 1]`, unknown keys rejected
- [x] Use `Settings.risk_default_gap_pct` as the default gap input
- [x] Preserve gap-sweep math and sign conventions internally
- [x] Return positive `rrc_usd` for gap-sweep results
- [x] Populate `RrcResult.details` with gap-sweep-specific fields
- [x] Move Aave-like vs Morpho dispatch into gap-sweep construction
- [x] Implement gap-sweep `applies_to(asset_id, prime_id)` for supported lending protocols
- [x] Keep temporary compatibility code thin and isolated
- [x] Run focused unit tests for shared types, SURAF, and gap-sweep
- [ ] Run integration tests needed to prove compatibility shims still behave correctly

> Note: integration test execution is still pending here because the local Docker daemon was unavailable.

## Notes For The Follow-up PRs

- `VEC-178` should be able to register these models against the shared port without changing their compute contracts.
- `VEC-183` should be able to ship unified `/v1/risk/rrc` endpoints using the shared result types and existing model implementations.
- Once unified endpoints land, the temporary scenario shim and any receipt-token-only compatibility seams should be removed.
