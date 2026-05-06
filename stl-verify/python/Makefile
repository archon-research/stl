
.PHONY: lint lint-fix test-unit test-integration build-ui-static export-openapi-schema run run-static

# Ensure `uv run` targets the project environment even if an unrelated venv is active.
UV := env -u VIRTUAL_ENV uv
TS_DIR := ../ts
STATIC_DIR := app/static
OPENAPI_SCHEMA := $(TS_DIR)/ui/src/generated/openapi-schema.json

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

# Run the FastAPI dev server (requires dev-up in stl-verify/ first)
run:
	$(UV) run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

run-static: build-ui-static
	$(UV) run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000