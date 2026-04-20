
.PHONY: lint lint-fix test-unit test-integration build-ui-static run run-static

# Ensure `uv run` targets the project environment even if an unrelated venv is active.
UV := env -u VIRTUAL_ENV uv
TS_DIR := ../ts
STATIC_DIR := app/static

lint:
	$(UV) run ruff check .
	$(UV) run ruff format --check .

lint-fix:
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

test-unit:
	$(UV) run pytest tests/unit

test-integration:
	$(UV) run pytest tests/integration

build-ui-static:
	rm -rf $(STATIC_DIR)
	cd $(TS_DIR) && npm run build --workspace=@stl-verify/ui
	mkdir -p $(STATIC_DIR)
	cp -R $(TS_DIR)/ui/dist/. $(STATIC_DIR)/

# Run the FastAPI dev server (requires dev-up in stl-verify/ first)
run:
	$(UV) run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

run-static: build-ui-static
	$(UV) run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000