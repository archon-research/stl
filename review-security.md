# Security review — VEC-183 (T9)

## Summary
The new `GET/POST /v1/risk/rrc` endpoints are largely safe — input shapes are pinned by Pydantic, the EthAddress regex is anchored and ReDoS-free, no sensitive values are logged, and there is no obvious authn regression on top of the legacy routes. There are, however, three issues worth fixing before ship: the gap-sweep override path can be coerced into a `TypeError` by sending `gap_pct` as a JSON string, the SURAF override path accepts unbounded-precision strings as `usd_exposure`, and `asset_id`/`overrides` lack basic bounds. None give code execution; the worst is reflected DoS / unfiltered 500.

## Findings

### gap_sweep `gap_pct` override raises TypeError → uncaught 500
high
`stl-verify/python/app/services/crypto_lending_risk_service.py:74-77`
`POST /v1/risk/rrc` with body `{"asset_id": <known>, "prime_id": "0x...", "overrides": {"gap_sweep": {"gap_pct": "0.5"}}}` reaches `_resolve_gap_pct`, which executes `Decimal("0") <= "0.5" <= Decimal("1")`. Comparing `Decimal` to `str` raises `TypeError`, which `_compute_envelope` (`risk.py:228-233`) does not catch — only `ValueError` is mapped to 422. The exception escapes to FastAPI's default 500 handler. Trivially repeatable, so an unauthenticated caller can flip a 422 contract violation into a 500 + traceback in logs at will. Note the new tests (`test_post_forwards_per_model_overrides`) actually send `"0.25"` as a string but use a fake model that never validates, which is why this slipped through. Mitigation: coerce in the same defensive shape SURAF does (`Decimal(str(raw))` with `is_finite()` check) before the bounds compare; or catch `(TypeError, decimal.InvalidOperation)` and rewrap as `ValueError`.

### SURAF `usd_exposure` accepts unbounded-precision input
medium
`stl-verify/python/app/services/suraf_rrc_service.py:97-109`
`Decimal(str(raw))` is called on the raw override value with no length cap. CPython 3.12 caps int-string parsing at 4300 digits via `set_int_max_str_digits`, but `Decimal(...)` bypasses that limit — I confirmed `Decimal("1" + "0" * 10_000_000)` parses in ~26ms; the multiply `usd_exposure * crr / 100` then materialises the long mantissa back through Decimal context (capped at prec=28, so the math itself is fine, but the parse cost is per-request and amplifies under concurrency). An attacker can post a few-MB JSON body with a 10⁷-digit `usd_exposure` and burn CPU + memory parsing it. Mitigation: cap raw string length (e.g. ≤32 chars) before `Decimal(str(raw))`, or reject non-`(int|float|str)` types up front and bound the resulting Decimal exponent/digit count.

### Information leakage in 404 detail
low
`stl-verify/python/app/api/v1/risk.py:222-224`
`detail=f"no risk models apply for asset_id={asset_id}, prime_id={prime_id}"` reflects the caller's input verbatim. Not a real injection vector (FastAPI JSON-encodes the body), but `prime_id` echoes through `str(EthAddress)` — and unsupported asset_ids (negative, zero, very large) get round-tripped in the error. Combined with the lack of authn (next finding), this is a cheap probe primitive: a caller can confirm asset_id existence with a single request. Mitigation: drop the values from the 404 detail (`"no risk models apply"`), or at minimum drop `prime_id`.

### No authentication on a public-facing API
medium
`stl-verify/python/app/main.py:170-176`, `stl-verify/python/app/api/v1/risk.py:182-203`
Neither the new GET/POST nor the legacy routes (`/bad-debt`, `/breakdown`, `/rrc/scenario`) carry any `Depends(...)` for an auth check; the only middleware is `RequestIdMiddleware`. If this service is reachable from the public internet (per the prompt), the unified endpoints inherit the legacy posture — and on top of that, the new routes accept an attacker-controlled JSON body, which is a wider surface than the legacy GETs. This is in scope per "DO flag if the legacy paths have security issues that propagate to the new ones." Mitigation: add an authn dependency (API key, JWT, mTLS — whichever matches the deployment) at the router level before the cutover deletes the old endpoints in T9.

### Unbounded `asset_id` and `overrides` shape
low
`stl-verify/python/app/api/v1/risk.py:169-171, 184`
`asset_id: int` has no `Field(ge=...)` / upper bound; negatives and zero just fall through to `applicable() == []` and reflect into the 404. `overrides: dict[str, dict[str, Any]]` has no key/value count limits — a payload like `{"suraf": {}, "gap_sweep": {}, ...20k known keys...}` is bounded by the known-model frozenset diff anyway, but inside a known model the per-model dict is forwarded raw to `compute()`. SURAF iterates a 1-element allowlist so extra keys are O(n) on the override dict. Not exploitable beyond CPU-burn on big bodies; recommend FastAPI body-size limit at the ingress and `Field(ge=1)` on `asset_id`.

## Non-issues considered
- EthAddress regex `^0x[0-9a-fA-F]{40}$` is anchored and fixed-length — ReDoS-safe (verified, ~150ns/match).
- `crr_pct` Decimal pattern in OpenAPI is response-side only; never user-controlled at request time, no ReDoS exposure.
- `protected_namespaces=()` on the Pydantic models exists only to suppress the `model_*` warning for the `model` discriminator field. No vector — it does not relax validation, only disables a naming-collision check against `BaseModel.model_*` methods.
- Discriminated-union dispatch in `RrcResult` is server-built, never deserialised from request input, so a hostile `details` payload cannot be smuggled.
- `EthAddressParam`'s `AfterValidator` raises `ValueError`, which FastAPI maps to 422 cleanly — no traceback leak on malformed `prime_id`.
- No `logger.*` calls in the new code paths — no PII / address logging regression.
- `applies_to` ignores `prime_id` (asset-only check); not a security issue but means the 404 only fires on unknown `asset_id`, never on unknown `prime_id`.
