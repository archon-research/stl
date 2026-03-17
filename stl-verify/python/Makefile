
.PHONY: lint lint-fix test-unit test-integration run

# Ensure `uv run` targets the project environment even if an unrelated venv is active.
UV := env -u VIRTUAL_ENV uv

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

# Run the FastAPI dev server (requires dev-up in stl-verify/ first)
run:
	$(UV) run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000