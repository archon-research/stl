# Linting and formatting targets for stl-verify (local development)
#
# For CI: See .github/workflows/ (go-ci.yml, python-ci.yml, ts-ci.yml)
# Git hooks: See lefthook.yml, python/lefthook.yml, ts/lefthook.yml
#
# Note: Lefthook automatically handles changed-file workflow on git commit/push

.PHONY: install-hooks format lint help lefthook-update-cli

LEFTHOOK_BIN := ./bin/lefthook
LEFTHOOK_VERSION ?= v2.1.6
LEFTHOOK_VERSION_NO_V := $(patsubst v%,%,$(LEFTHOOK_VERSION))

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

ifeq ($(UNAME_S),Darwin)
LEFTHOOK_OS := MacOS
else ifeq ($(UNAME_S),Linux)
LEFTHOOK_OS := Linux
else
LEFTHOOK_OS := unsupported
endif

ifeq ($(UNAME_M),arm64)
LEFTHOOK_ARCH := arm64
else ifeq ($(UNAME_M),aarch64)
LEFTHOOK_ARCH := arm64
else ifeq ($(UNAME_M),x86_64)
LEFTHOOK_ARCH := x86_64
else ifeq ($(UNAME_M),amd64)
LEFTHOOK_ARCH := x86_64
else
LEFTHOOK_ARCH := unsupported
endif

$(LEFTHOOK_BIN): ## Download lefthook CLI to ./bin from GitHub Releases
	@set -e; \
	if [ "$(LEFTHOOK_OS)" = "unsupported" ] || [ "$(LEFTHOOK_ARCH)" = "unsupported" ]; then \
		echo "Unsupported platform: $(UNAME_S)/$(UNAME_M)"; \
		exit 1; \
	fi; \
	mkdir -p ./bin; \
	ASSET="lefthook_$(LEFTHOOK_VERSION_NO_V)_$(LEFTHOOK_OS)_$(LEFTHOOK_ARCH)"; \
	URL="https://github.com/evilmartians/lefthook/releases/download/$(LEFTHOOK_VERSION)/$$ASSET"; \
	echo "Downloading $$URL"; \
	curl -fsSL "$$URL" -o "$(LEFTHOOK_BIN)"; \
	chmod +x "$(LEFTHOOK_BIN)"; \
	echo "Installed $(LEFTHOOK_BIN)"

lefthook-update-cli: ## Re-download pinned lefthook CLI into ./bin
	@rm -f "$(LEFTHOOK_BIN)"
	@$(MAKE) "$(LEFTHOOK_BIN)"

# Install lefthook git hooks
install-hooks: $(LEFTHOOK_BIN) ## Install lefthook pre-commit/push hooks
	@echo "==> Installing lefthook git hooks..."
	@$(LEFTHOOK_BIN) install

# Local development helpers — just delegate to language-specific tooling

format: ## Auto-format code (Go, Python, TypeScript)
	@echo "==> Formatting Go..."
	@go fmt ./...
	@gofmt -s -w .
	@echo "==> Formatting Python..."
	@$(MAKE) -C python lint-fix
	@echo "==> Formatting TypeScript..."
	@cd ts && npm run format

lint: ## Run linters (delegates to language pipelines)
	@echo "==> Linting Go (make ci-checks)..."
	@$(MAKE) ci-checks
	@echo "==> Linting Python..."
	@$(MAKE) -C python lint
	@echo "==> Linting TypeScript..."
	@cd ts && npm run lint

# Help target
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' lint.mk | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'
