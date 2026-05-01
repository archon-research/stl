# Web3 / domain review — VEC-183 (T9)

## Summary
The unified `/v1/risk/rrc` contract is broadly sound: SURAF and gap-sweep
both implement the same port, the discriminated `details` union is wired
correctly, and the 503 share-data escape hatch is preserved. However the
response surface leaks the ambiguity between `asset_id` (a DB surrogate)
and on-chain identifiers, address normalisation is half-done, and the
multi-model envelope shape invites callers to do unsafe arithmetic on
RRC values whose semantics differ.

## Domain concerns

### `asset_id` is a DB surrogate but the API surface treats it as canonical
**major** — `stl-verify/python/app/api/v1/risk.py:169,177`,
`stl-verify/python/app/domain/entities/risk.py:64`

`asset_id: int` over the wire is `receipt_token_id` — a per-environment,
per-chain DB primary key. The plan's T0/T4 acknowledges this is an
environment-specific surrogate and uses `chain_id:receipt_token_address`
as the portable key for static config. The HTTP envelope only echoes
`asset_id`. A consumer reading staging data and comparing to prod (or
vice versa) will see different ints for the same on-chain token, and
nothing in the response identifies the chain or the receipt-token
address. For a Web3 audit trail this is dangerous: RRC numbers are
indistinguishable across chains. Add `chain_id` and
`receipt_token_address` to `RrcEnvelope` (or to each `RrcResult`),
populated from a single lookup at envelope construction. Document in
the OpenAPI summary that `asset_id` is the local DB id, not an on-chain
token id.

### `prime_id` is not normalised — checksum/case round-trip is lossy
**major** — `stl-verify/python/app/api/v1/risk.py:34,236`,
`stl-verify/python/app/domain/entities/allocation.py:5,16`

`EthAddress` accepts both upper- and lower-case hex (`[0-9a-fA-F]{40}`)
and stores the value verbatim (no lowercase or EIP-55 normalisation in
`__init__`). `RrcEnvelope.prime_id` is set with `str(prime_id)` which
preserves the caller's casing. This means:
1. Two requests for the same prime with different casings produce
   responses whose `prime_id` strings differ — bad for caching, bad for
   downstream equality checks.
2. The DB lookup uses `prime_id.hex` (still original case) which is then
   hex-decoded (`decode(:proxy_hex, 'hex')`) — Postgres `decode` is
   case-insensitive so this works, but a mixed-case input that *isn't*
   a valid EIP-55 checksum is silently accepted. For a risk endpoint
   this is a footgun: a tool sending the wrong checksum thinks the
   address is valid.

Recommend either (a) lowercase-normalise inside `EthAddress.__init__`
and round-trip lowercased, or (b) accept only EIP-55 or all-lowercase,
reject mixed-case-with-bad-checksum. Echo a normalised value in the
response.

Also: nothing rejects smart-contract / multisig primes. Today the
allocation lookup will simply return zero rows (raising 422 via
`get_usd_exposure`'s `ValueError`), which is acceptable, but the error
message ("no position or price found") doesn't help a caller debug a
Safe / 4337 wallet that hasn't been indexed. Worth a glossary note.

### CRR math: no quantisation, default `Decimal` context can produce
27-digit fractions
**major** — `stl-verify/python/app/services/suraf_rrc_service.py:75`

`rrc_usd = usd_exposure * rating.crr_pct / _HUNDRED`. `crr_pct` is a
Decimal on a 0–100 scale (e.g. `33.7`); `usd_exposure` comes from
`balance × price_usd` where both originate as `Decimal(str(row...))` —
arbitrary precision. Default `decimal.Context` precision is 28; division
by 100 of a `Decimal("33.7")` produces `0.337`, but `usd_exposure *
0.337` of a 6-decimal price × an 18-decimal balance can yield ~25
significant digits — the JSON serialisation then ships all of them. No
explicit quantisation at the boundary. Recommend `rrc_usd =
(usd_exposure * crr_pct / 100).quantize(Decimal("0.01"),
rounding=ROUND_HALF_UP)` (or 8dp — pick a convention and write it in
the OpenAPI doc). Same applies to `bad_debt_usd` from `gap_sweep`,
which inherits `Decimal.from_float(lgd) * amount_usd` — the
`from_float` introduces float-rounding noise that survives into the
response. This is the largest correctness wart in the change.

### `unadjusted_crr_pct + penalty_pp ≠ crr_pct` in general — invariant
not surfaced
**minor** — `stl-verify/python/app/domain/entities/risk.py:16-31`,
`stl-verify/python/app/risk_engine/suraf/result.py:18-22`

The new fields are useful for transparency, but the docstring says
`crr_pct = min(unadj + penalty, 100)` while `SurafResult` warns the
identity holds only up to ~1e-10 (float-pre-Decimal noise). A consumer
reconstructing CRR from parts will get answers that differ from
`crr_pct` by sub-bps. Either (a) explicitly document "use `crr_pct`,
the parts are advisory" in `SurafDetails`, or (b) re-derive
`crr_pct = min(unadj + penalty, Decimal(100))` at the Decimal boundary
so the invariant is exact. Today the docstring states the cap but
doesn't tell consumers which to trust.

### `gap_pct` units are not echoed as percent — possible 1.0 vs 100%
confusion
**minor** — `stl-verify/python/app/domain/entities/risk.py:40`,
`stl-verify/python/app/services/crypto_lending_risk_service.py:76-77`

`gap_pct` is a fraction in `[0, 1]` (0.15 = 15% drop), not 0–100 like
SURAF's CRR. The two models use opposite conventions for "percent" on
the same envelope. `GapSweepDetails.gap_pct` ships the raw fraction
without a unit field. Given SURAF's `crr_pct` is on 0–100, a caller
glancing at both fields will see `crr_pct=33.7, gap_pct=0.15` and very
plausibly mis-scale one of them. Suggest renaming the field or adding a
docstring/`Field(description=...)` that explicitly states the
convention; ideally the two models would agree on a single scale.

### Multi-model envelope: results are not summable, but the API doesn't
say so
**major** — `stl-verify/python/app/api/v1/risk.py:174-179`

`RrcEnvelope.results: list[RrcResult]` lets a caller iterate models for
the same `(asset_id, prime_id)`. SURAF RRC is a *capital charge against
exposure*; gap-sweep RRC is *expected loss given a price gap*. They
overlap economically — gap-sweep loss is part of what SURAF's CRR is
meant to cover. Naively summing the two double-counts. The plan
(CONTRIBUTING §10 / T9) does not specify how to aggregate, and the
envelope offers no `aggregated_rrc_usd` or "models are alternative
views; do not sum" note. Add a top-level field — `max(rrc_usd)` is a
defensible aggregator and gives callers a single number to use without
having to understand model semantics — and document in the response
schema that summing across `results[].rrc_usd` is not meaningful.

### `bad_debt_usd` field name encodes a sign convention that's been
inverted
**minor** — `stl-verify/python/app/domain/entities/risk.py:41`,
`stl-verify/python/app/services/crypto_lending_risk_service.py:58-65`

`gap_sweep.total_bad_debt` returns `≤ 0` (loss as negative USD, the
math-honest convention). The service does `abs(raw)` and stores it as
`bad_debt_usd`. So a field called "bad debt" now ships a *positive*
number that callers will use as `rrc_usd`. Two issues: (1) the same
positive value is stored on both `rrc_usd` and `details.bad_debt_usd`,
which is redundant; (2) anyone reading the gap-sweep engine source will
expect a non-positive number. Either drop the field (it's already
`rrc_usd`) or rename to e.g. `bad_debt_usd_magnitude` / document the
sign flip explicitly. Also worth surfacing the per-collateral
breakdown in `details.items` per the plan (T7) — currently the unified
endpoint loses the per-collateral structure that the legacy
`/breakdown` route provided, which is a regression for risk debugging.

### Stale-share 503 codes: no documentation in the unified endpoint
**minor** — `stl-verify/python/app/api/v1/risk.py:74-82,230-231`

The `share_data_stale` / `share_data_missing` / `share_data_unavailable`
503 codes are a meaningful Web3-specific signal (allocation share is
the prime's fractional ownership of the receipt-token wrapper, computed
from on-chain events whose freshness depends on the indexer). The
unified handler propagates them but the endpoint docstring and the
response model say nothing about 503 semantics. Document this in the
GET/POST docstrings so callers know to retry rather than treat it as a
hard error.

## API ergonomics for blockchain consumers
- Response should echo `chain_id` and `receipt_token_address`. Without
  them, a caller storing RRC results cannot reconstruct the on-chain
  asset they correspond to without a second roundtrip.
- Consider returning `applicable_models` (just the names) when 0 models
  apply, so a caller knows whether the asset is unmapped vs the prime
  has no position. Today both yield 404.
- Address echo should be lowercased or EIP-55-checksummed deterministically.
- Per-model `details` should include a unit-bearing scale (`scale: "0..1"`
  vs `scale: "0..100"`) or rename fields so the convention is in the
  field name (e.g. `gap_fraction` vs `crr_pct`).
- Consider returning a single aggregated `rrc_usd` at the envelope level
  so consumers don't have to decide between sum / max.

## Open questions on semantics
- For an asset SURAF and gap-sweep both cover, what's the canonical RRC
  the caller should use? The contract is silent.
- Does `prime_id` validation need to verify the address is a known prime
  in the DB before computing? Today an unknown prime returns 404 from
  the SURAF exposure lookup, which is reasonable, but for gap-sweep with
  a non-prime address you'll silently get an empty `share` (or the 503
  path), depending on indexer state. Behaviour should be uniform.
- Should `prime_id` ever resolve to an L2 / non-mainnet prime? The
  endpoint takes no `chain_id` for the prime side — implicitly assuming
  Ethereum mainnet — but `receipt_token_id` already encodes a chain. A
  prime address on Ethereum and on Base are the same hex string but
  different positions; the endpoint cannot distinguish them today.
- For gap-sweep: is `gap_pct = 1.0` (100% collateral wipe) actually a
  meaningful scenario, or should the upper bound be tighter? The math
  accepts it; reviewing whether `loss_given_default(slippage=1.0)` is
  defined the way the product wants is worth a follow-up.
- Will `unadjusted_crr_pct + penalty_pp` ever differ from `crr_pct` by
  more than the documented ~1e-10? If a future scorer change widens the
  drift, downstream consumers reconstructing CRR from parts will silently
  diverge from `crr_pct`. Worth pinning a tolerance test.
