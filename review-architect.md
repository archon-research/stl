# Architecture review — VEC-183 (T9)

## Summary
The shape is right: a `ModelRegistry` in front of a `RiskModel` port, a discriminated-union `RrcDetails`, and a thin handler that loops over applicable models. The new endpoint earns its keep — handler dispatch is genuinely model-agnostic and adding a third model would be a narrow change. Two real issues: (1) the inbound handler imports an exception type from a postgres adapter, breaking the dependency rule; (2) `RrcResult` carries both a discriminator-based union and a redundant `model_validator`, doing the same job twice. Everything else is minor.

## Strengths
- `ModelRegistry.applicable` + `RiskModel.applies_to` cleanly separate routing from compute. Handler at `risk.py:212-234` reads as prose.
- `model_names` precomputed in the constructor (`model_registry.py:10`) and cached in a `frozenset` — small, correct, kept inside the registry rather than leaking into the handler.
- Discriminator field colocated on each variant (`SurafDetails.model`, `GapSweepDetails.model`) and surfaced as `Field(discriminator="model")` — Pydantic now handles deserialisation, OpenAPI rendering, and tag-checking in one shot.
- Per-model override validation is correctly delegated to the model itself (`suraf_rrc_service.py:93-95`, `crypto_lending_risk_service.py:71-73`), with only top-level model-key validation in the handler. ISP-respecting.
- Test double `_FakeRiskModel` (`test_risk_rrc_endpoints.py:38-68`) is duck-typed against the `Protocol`, which is the right way to test a `Protocol` port.

## Concerns

### Inbound handler imports an adapter-layer exception
**major** — `risk.py:7-11`, `risk.py:74-82`, `risk.py:230-231`
The handler imports `AllocationShareError`/`MissingShareError`/`StaleShareError` directly from `app.adapters.postgres.allocation_share_repository`. That makes the API layer depend on a concrete adapter (and any caller of `RiskModel.compute` is implicitly committed to postgres). It also forces every `RiskModel` implementation to raise that specific postgres exception type to get the 503-with-code translation — a Liskov trap waiting for the next non-postgres model. Lift the error hierarchy into a port/domain module (e.g. `app/ports/allocation_share.py` or `app/domain/errors.py`) and re-export from the adapter for backward compatibility. Same fix unblocks anyone wanting to swap the share repo for a non-DB source later.

### `RrcResult` validates the model/details pairing twice
**minor** — `domain/entities/risk.py:47-50`, `domain/entities/risk.py:81-87`
With `RrcDetails` now a `Field(discriminator="model")` union and a typed `model: Literal["suraf"]` on each variant, Pydantic already enforces that `RrcResult.details` matches `RrcResult.model` (provided the outer `model` field is also part of the discriminator pathway — and even if it isn't, the variant's own literal pins it). The `_MODEL_TO_DETAILS` lookup table plus `_check_model_details_pairing` model_validator are now redundant belt-and-braces and a second source of truth for the model name list. Drop them; rely on the discriminator. If you want defence-in-depth, make `RrcResult.model` a computed/derived field from `details.model` instead of a separately-supplied string.

### Two definitions of "the set of model names"
**minor** — `domain/entities/risk.py:13`, `services/model_registry.py:10`
`ModelName = Literal["suraf", "gap_sweep"]` in the domain entity hard-codes the set, while `ModelRegistry.model_names` derives it from the runtime registry. They will drift the moment a third model is added: the registry will accept it, the response schema will refuse to serialize it. Either drop `ModelName` and let the discriminated union define the literal set implicitly, or generate the response model dynamically from registered models. The plan's whole point is "adding a model is one ticket" — this widens that ticket.

### `RiskModel` uses `model:` as both a Protocol attribute and a method name space
**nit** — `ports/risk_model.py:20`, `services/suraf_rrc_service.py:46`, `services/crypto_lending_risk_service.py:26`
Pydantic v2 uses the `model_` prefix as a namespace and you've already had to suppress it via `protected_namespaces=()` in three places (`risk.py:23,37,62`). It works, but every new BaseModel that touches this surface inherits the suppression. Naming the discriminator `kind` (or `model_name`) avoids the workaround entirely and reads slightly more honestly — the field is a tag, not a model. Not blocking; just paying interest forever for a name choice.

### `ModelRegistry` placement
**nit** — `services/model_registry.py`
The registry is dispatch infrastructure, not a use case — it has no business logic and no policy. `app/services/` is the use-case layer in the hexagonal layout described in the parent CLAUDE.md. Consider `app/registry/` or `app/services/registry/`. Not load-bearing, but services/ is where readers will look for "what does the app *do*", not "how does it wire ports together".

### `EthAddressParam`'s declared type is a lie
**nit** — `risk.py:34`, `risk.py:200`
The `Annotated[str, AfterValidator(...)]` returns an `EthAddress` at runtime but its type annotation is `str`. The comment at `risk.py:200` explicitly papers over this. Type the alias as `Annotated[EthAddress, ...]` or use a `BeforeValidator` that keeps the `str` contract honest.

## Questions for the author
- Was lifting `AllocationShareError` out of the postgres adapter considered and rejected, or is that just deferred? It's a small move and removes a real layering violation.
- Given the discriminated union now exists, what's the role of `RrcResult.model` as a separate top-level field? Could it become a `@computed_field` from `details.model` so there's one source of truth?
- Is `ModelName = Literal[...]` used anywhere outside `RrcResult`? If not, removing it tightens the "register a new model in one place" property the design is aiming at.
