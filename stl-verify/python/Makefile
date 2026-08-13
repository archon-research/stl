
.PHONY: lint lint-fix typecheck test-unit test-integration build-ui-static export-openapi-schema export-axis-synome-contract run run-static

# Ensure `uv run` targets the project environment even if an unrelated venv is active.
UV := env -u VIRTUAL_ENV uv
TS_DIR := ../ts
STATIC_DIR := app/static
OPENAPI_SCHEMA := $(TS_DIR)/ui/src/generated/openapi-schema.json
AXIS_SYNOME_CONTRACT_DIR := ../contracts/axis-synome
AXIS_SYNOME_CONTRACT_JSON := $(AXIS_SYNOME_CONTRACT_DIR)/axis_synome_entities.json
AXIS_SYNOME_CONTRACT_SCHEMA := $(AXIS_SYNOME_CONTRACT_DIR)/axis_synome_entities.schema.json
# Provenance commit stamped into the exported contract. axis-synome's published dev
# wheels set SETUPTOOLS_SCM_PRETEND_VERSION, so the build commit is unset
# (_version.commit_id is None). The export tool (>= dev76) guards its own git fallback
# so it never resolves THIS repo's HEAD. We read the commit from the wheel when present
# and otherwise default to "unknown"; pass AXIS_SYNOME_GIT_COMMIT=<sha> on a release
# bump to stamp the published commit.
AXIS_SYNOME_GIT_COMMIT ?= $(shell $(UV) run python -c "from axis_synome._version import commit_id; print(commit_id or 'unknown')" 2>/dev/null || echo unknown)

typecheck:
	$(UV) run --extra dev ty check

lint:
	$(UV) run --extra dev ruff check .
	$(UV) run --extra dev ruff format --check .

lint-fix:
	$(UV) run --extra dev ruff check --fix .
	$(UV) run --extra dev ruff format .

test-unit:
	$(UV) run --extra dev pytest tests/unit

test-integration:
	$(UV) run --extra dev pytest tests/integration

build-ui-static:
	rm -rf $(STATIC_DIR)
	cd $(TS_DIR) && npm run build --workspace=@stl-verify/ui
	mkdir -p $(STATIC_DIR)
	cp -R $(TS_DIR)/ui/dist/. $(STATIC_DIR)/

export-openapi-schema:
	$(UV) run python scripts/export_openapi_schema.py --output $(OPENAPI_SCHEMA)

export-axis-synome-contract:
	$(UV) run export_entities --out $(AXIS_SYNOME_CONTRACT_JSON) --schema-out $(AXIS_SYNOME_CONTRACT_SCHEMA) --git-commit "$(AXIS_SYNOME_GIT_COMMIT)"

# Run the FastAPI dev server (requires dev-up in stl-verify/ first)
run:
	$(UV) run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

run-static: build-ui-static
	$(UV) run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000