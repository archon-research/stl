# Tech-lead review — VEC-183 (T9)

## Summary
Solid landing of the unified `GET/POST /v1/risk/rrc` contract. The registry-dispatch handler is short, the discriminated union renders correctly, and the router-level test set hits the acceptance list cleanly. A few sharp edges around error translation, observability, and the `EthAddressParam` type-vs-runtime mismatch should be tidied before this is merged into prod.

## What's good
- Handlers stay flat and model-agnostic — `_compute_envelope` is the only place that knows about both models, exactly as the plan asks (`risk.py:206-236`).
- Discriminated union is wired correctly: `Annotated[Union[...], Field(discriminator="model")]` plus `Literal["..."]` defaults on each variant gives clean OpenAPI and a fast-path validator (`risk.py` entities, `risk.py:44`).
- `ModelRegistry.model_names` is precomputed once in `__init__`, so the per-request unknown-key check is O(1) (`model_registry.py:10`, `risk.py:212`).
- Existing legacy endpoints kept intact and `LegacyRrcResult` is genuinely separate from the new `RrcResult`, so old `/scenario` callers are unaffected (`suraf_rrc_service.py:27-36, 127-145`).
- Tests use `app.dependency_overrides[get_model_registry]` rather than monkey-patching `app.state` — the FastAPI-idiomatic way (`test_risk_rrc_endpoints.py:27, 101-112`).
- 422-on-malformed-`prime_id` is asserted (`test_risk_rrc_endpoints.py:270`), which proves the `AfterValidator` path works.

## What I'd change before merge

### `EthAddressParam` annotated as `str` but yields `EthAddress`
**major** — `risk.py:34`, `allocations.py:27`
`Annotated[str, AfterValidator(_validate_eth_address)]` returns an `EthAddress` instance at runtime but the static type says `str`. In `RrcRequest`, `prime_id: EthAddressParam` means the `body.prime_id` you forward at `risk.py:200` is *actually* an `EthAddress`, not a `str`, despite the comment. This is the kind of type lie that bites later when somebody adds a `body.prime_id.lower()` and gets an attribute error in prod. Either:
- type the alias as `Annotated[EthAddress, BeforeValidator(EthAddress)]` (truthful), or
- have the validator return `str(EthAddress(value))` so the runtime matches the annotation.
Same fix should land in `allocations.py` so the two stay aligned. Also note the duplication itself — pull the alias into `app/api/v1/_validators.py` (or onto `EthAddress` as a classmethod) and import from one place.

### Broad `except ValueError → 422` is dangerous
**major** — `risk.py:232-233`, also `risk.py:99-100, 120-121`
Catching every `ValueError` from `model.compute(...)` and surfacing it as 422 means any incidental `ValueError` raised deep in adapters (bad cast, malformed DB row, programmer error) becomes "client bad request". Compare `crypto_lending_risk_service.py:54` ("unsupported asset_id") — that's *our* invariant violation, not a client problem, yet today it's a 422 without `applies_to` first being false.
Two cleaner options:
- Define a narrow `OverrideValidationError(ValueError)` (or `RiskInputError`) and map only that to 422; let everything else 500.
- Or have services raise distinct typed errors (`UnknownOverrideKey`, `OverrideOutOfRange`, `PositionNotFound`) and map per-type in the handler.
Either way the boundary becomes intentional rather than "whatever happened to be a `ValueError`".

### No structured logging or span enrichment in the new handler
**major** — `risk.py:182-236`
Operability is thin. There is no `logger` imported in `risk.py` at all, so a 503 / 404 / 422 leaves nothing in the logs except the access record. At minimum I would want:
- A debug/info log on success: `request_id`, `asset_id`, `prime_id`, applicable model names, total `rrc_usd`.
- A `logger.warning` on the 503 paths with the share-error code (today the operator sees a 503 and has to grep adapter logs to find which model raised).
- A span attribute pass through OTEL: `span.set_attribute("risk.asset_id", asset_id)`, `risk.models", ",".join(m.model for m in applicable))`. The codebase already wires telemetry in `main.py:171`; this handler should participate.

No counter/histogram on per-model latency either. The whole point of the registry is that adding model #3 is cheap — please make sure the operator can answer "is gap_sweep slow today?" without code changes.

### `_compute_envelope` runs models serially
**minor** — `risk.py:227-234`
`for model in applicable:` `await`s sequentially. With two models that's fine; with five it'll be the slowest model's latency × N. Either document the choice (e.g. "ordering matters for response stability") or use `asyncio.gather` and let exceptions surface in deterministic order. Not a blocker today but worth a TODO.

### Per-model contract isn't tightened on the Pydantic side
**minor** — `risk.py:171`
`overrides: dict[str, dict[str, Any]]` is intentionally loose, but you could still get the `pydantic` win for *known* models by using a `RootModel` / `TypedDict` per model and falling back to `Any` for unknown. As-is, the only validation of `overrides["suraf"]` shape happens inside `SurafRrcService._resolve_usd_exposure` at `suraf_rrc_service.py:93-109`, which means OpenAPI advertises `dict[str, Any]` to clients. A discriminated `RrcOverrides` type would generate ergonomic clients in T10. Defer if you want, but the longer this sits as `dict[str, Any]` the more clients hard-code shapes.

### `RrcEnvelope.prime_id: str` loses the type round-trip
**minor** — `risk.py:178, 236`
You stringify the `EthAddress` on the way out (`str(prime_id)`). Fine, but the response schema then advertises `prime_id: str` with no format hint. Add `prime_id: str = Field(pattern=r"^0x[0-9a-fA-F]{40}$", examples=["0x" + "ab"*20])` or a typed `EthAddressStr` so the OpenAPI doc is self-documenting and generated TS clients narrow it.

### Missing tests / examples
**minor** — `tests/unit/test_risk_rrc_endpoints.py`
- No assertion that a non-`AllocationShareError`/`ValueError` exception bubbles to a 500 (i.e. the handler doesn't catch too broadly). Worth a test that throws `RuntimeError` and asserts 500, to lock the contract.
- No test for `details` discriminator round-trip (post the body, confirm `response.json()["results"][0]["details"]["rating_id"]` survives — implicit but not asserted).
- `RrcEnvelope` / `RrcRequest` carry no docstring `examples=` for OpenAPI; add at least one canonical example payload per model.
- The fake registry is fine, but consider also a test where `applicable` returns a model whose `compute` returns a `RrcResult` whose `model` field doesn't match the registry key — does the handler care? (Today it doesn't; document that or assert.)

### Nit: unused `service: SurafRrcService = ...` at the legacy endpoint imports
**nit** — `risk.py:13-16`
`get_suraf_rrc_service` and `get_crypto_lending_risk_service` are still both imported and used by the legacy routes, so this is fine — just flag for the eventual cleanup PR that removes those routes.

## Things I'd want in follow-ups
- Pull `EthAddressParam` / `EthAddressPath` into a single shared module; today it lives in two places with the same body.
- Type the per-model overrides with proper Pydantic models so OpenAPI clients get something better than `dict[str, Any]`.
- Add `asyncio.gather` over `applicable` once a third model lands.
- Wire structured logs + OTEL attributes + a per-model latency histogram in this handler before T10 frontend cutover — that's where you'll need the data.
- Add an integration test (T12) that exercises the discriminated-union round-trip through the real router; the unit suite mocks the registry, so nothing today proves the wiring in `main.py:152` actually populates `app.state.model_registry`.

## Open questions
- Should "no applicable models" really be 404, or 200 with `results: []`? A caller listing every prime allocation will hit assets that no model covers; turning every one of those into a 404 forces clients to special-case it. The plan says 404, but worth a second look once the frontend wires up.
- `RrcResult.prime_id` is `str` with a custom `before` validator that accepts `EthAddress`; `RrcEnvelope.prime_id` is plain `str`. Inconsistent. Pick one (`EthAddress`-aware everywhere, or `str` everywhere) and make `EthAddress.__get_pydantic_core_schema__` carry the validation.
- `protected_namespaces=()` is set on every model that has a `model: Literal[...]` field. That silences the warning, but it also disables the namespace check globally for those classes. Worth a comment explaining why — future maintainers will wonder.
