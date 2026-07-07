# stl-verify/python

FastAPI service that serves data out and hosts quantitative risk models. Go service
conventions: [../CLAUDE.md](../CLAUDE.md). Root: [../../CLAUDE.md](../../CLAUDE.md).

## Architecture (mirrors the Go hexagonal layout)

- `app/domain/`, `app/ports/`, `app/services/`, `app/adapters/` — same dependency-inward rule as Go.
- Risk models are **pure math** in `app/risk_engine/<model>/` with no I/O. `app/risk_engine/_vendored_synome/` is a vendored mirror of the upstream `synome` package — don't hand-edit it as if it were first-party.
- `app/api/v1/*.py` never imports `risk_engine/` directly — it goes through services and a registry.
- Each model has a unique `risk_model` discriminator (`suraf`, `gap_sweep`); `ModelRegistry` rejects duplicates at startup. `/v1/risk/rrc` dispatches to **every applicable model** and returns one result per model plus conservative `max_rrc_usd`/`max_crr_pct`. Per-model results are **not additive** (SURAF capital and gap-sweep expected-loss overlap economically) — never sum them; pick one or use the `max_*` fields.

## Conventions & gotchas

- **Money is `Decimal` serialized as JSON strings**, never floats — preserves precision. Match this on new response fields.
- **Startup state is a snapshot.** `create_app` loads SURAF ratings + asset→rating mapping eagerly (fails before any DB/telemetry if a `rating_id` is unmapped), then builds engine/repos/services on `app.state` in `lifespan`. Receipt tokens added after boot need a restart to appear.
- `Settings.async_database_url` normalizes `postgres[ql]://` → `postgresql+asyncpg://` and drops `sslmode` (asyncpg uses `ssl` instead).
- Integration tests give each **module** its own isolated DB with the real `../db/migrations` applied (`CONCURRENTLY` stripped); `asyncio_mode=auto`.

## Tooling & commands

- **Dependencies: `uv` only** — never pip/poetry. `uv add <pkg>`, `uv run <cmd>`. Tools: `uv sync --all-extras`.
- Hooks (lefthook): ruff lint, ruff format.
- Type checking: `make typecheck` (`ty`). Fix the root cause; don't paper over with `# ty: ignore`.
- CI (`python-ci.yml`): `make lint` + `make test-unit` + `make test-integration` — **source of truth**.

```bash
cd stl-verify/python
make lint             # ruff (lint-fix to autofix)
make typecheck
make test-unit        # uv run pytest tests/unit
make test-integration # needs Docker
make run              # FastAPI server locally
uv run pytest tests/unit/path/test_file.py::test_name   # single test
```

Don't bypass hooks.
