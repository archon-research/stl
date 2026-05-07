# Linting and formatting targets for stl-verify (local development)
# 
# For CI: See .github/workflows/ (go-ci.yml, python-ci.yml, ts-ci.yml)
# Git hooks: See lefthook.yml, python/lefthook.yml, ts/lefthook.yml
#
# Note: Lefthook automatically handles changed-file workflow on git commit/push

.PHONY: install-hooks format lint help

# Install lefthook git hooks
install-hooks: ## Install lefthook pre-commit/push hooks
	@echo "==> Installing lefthook git hooks..."
	@command -v lefthook >/dev/null 2>&1 || go install github.com/evilmartians/lefthook@latest
	@lefthook install

# Local development helpers — just delegate to language-specific tooling

format: ## Auto-format code (Go, Python, TypeScript)
	@echo "==> Formatting Go..."
	@go fmt ./...
	@gofmt -s -w .
	@echo "==> Formatting Python..."
	@$(MAKE) -C python lint-fix
	@echo "==> Formatting TypeScript..."
	@npm run format --workspace=@stl-verify/ui

lint: ## Run linters (delegates to language pipelines)
	@echo "==> Linting Go (make ci-checks)..."
	@$(MAKE) ci-checks 2>/dev/null || true
	@echo "==> Linting Python..."
	@$(MAKE) -C python lint
	@echo "==> Linting TypeScript..."
	@npm run lint --workspace=@stl-verify/ui

# Help target
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' lint.mk | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'
