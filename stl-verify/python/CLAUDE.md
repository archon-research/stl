# stl-verify/python

Python sub-service. Go service conventions: [../CLAUDE.md](../CLAUDE.md). Root: [../../CLAUDE.md](../../CLAUDE.md).

## Linting & tooling

- Hooks (lefthook): ruff lint, ruff format
- CI (`python-ci.yml`): `make lint` + `make test-unit` + `make test-integration` — **source of truth**
- Tools: `uv sync --all-extras` (ruff, pytest, ty, etc.)
- Type checking: `ty check`. Fix the root cause; don't paper over with `# ty: ignore`.
- Don't bypass hooks.
