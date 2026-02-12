
.PHONY: lint test-unit test-integration

# Ensure `uv run` targets the project environment even if an unrelated venv is active.
UV := env -u VIRTUAL_ENV uv

lint:
	$(UV) run ruff check .
	$(UV) run ruff format --check .

test-unit:
	$(UV) run pytest tests/unit

test-integration:
	$(UV) run pytest tests/integration
