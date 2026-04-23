# stl-verify Makefile
# Go 1.25+ best practices

.PHONY: all test test-race test-integration lint vet fmt fix fix-check goimports-check tidy check clean cover run run-watcher run-watcher-avax run-oracle-price-worker run-oracle-price-worker-avax run-morpho-indexer run-sparklend-position-tracker run-sparklend-position-tracker-avax run-allocation-tracker run-allocation-tracker-avax help ci ci-checks \
        dev-up dev-up-rebuild dev-up-workers dev-preflight dev-env dev-db dev-down dev-wipe dev-ips generate-er \
		ecr-login docker-build docker-push docker-release \
		_docker-release-internal _docker-release-backup-internal _docker-release-oracle-price-worker-internal _docker-release-sparklend-position-tracker-internal _docker-release-morpho-indexer-internal _docker-release-allocation-tracker-internal _docker-release-prime-debt-indexer-internal _docker-release-migrate-internal _docker-release-python-api-internal _docker-release-cronjob-%-internal \
        docker-build-watcher docker-push-watcher docker-release-watcher \
        docker-build-watcher-staging docker-push-watcher-staging docker-release-watcher-staging \
        docker-build-backup docker-push-backup docker-release-backup \
        docker-build-backup-staging docker-push-backup-staging docker-release-backup-staging \
        docker-build-oracle-price-worker docker-push-oracle-price-worker docker-release-oracle-price-worker \
        docker-build-oracle-price-worker-staging docker-push-oracle-price-worker-staging docker-release-oracle-price-worker-staging \
        docker-build-sparklend-position-tracker docker-push-sparklend-position-tracker docker-release-sparklend-position-tracker \
        docker-build-sparklend-position-tracker-staging docker-push-sparklend-position-tracker-staging docker-release-sparklend-position-tracker-staging \
        docker-build-allocation-tracker docker-push-allocation-tracker docker-release-allocation-tracker \
        docker-build-allocation-tracker-staging docker-push-allocation-tracker-staging docker-release-allocation-tracker-staging \
        docker-release-all docker-release-all-staging \
        \
        docker-build-morpho-indexer docker-push-morpho-indexer docker-release-morpho-indexer \
        docker-build-morpho-indexer-staging docker-push-morpho-indexer-staging docker-release-morpho-indexer-staging \
        docker-build-prime-debt-indexer docker-push-prime-debt-indexer docker-release-prime-debt-indexer \
        docker-build-prime-debt-indexer-staging docker-push-prime-debt-indexer-staging docker-release-prime-debt-indexer-staging \
        \
        ecs-deploy-watcher ecs-deploy-watcher-staging \
        ecs-deploy-oracle-price-worker ecs-deploy-oracle-price-worker-staging \
        bastion-connect bastion-connect-staging bastion-tailscale-status bastion-tailscale-status-staging \
        bastion-create-tailscale-secret bastion-create-tailscale-secret-staging tigerdata-tunnel \
        db-migrate db-set-passwords db-test-app-user \
        erigon-status erigon-logs erigon-logs-follow erigon-deploy-readonly erigon-switch-readonly erigon-switch-sync erigon-mode \
        erigon-run build-bulk-download-arm64 deploy-bulk-download erigon-bulk-download \
		build-oracle-pricing-backfill-arm64 deploy-oracle-pricing-backfill deploy-oracle-pricing-backfill-worker \
		build-sparklend-backfill-arm64 deploy-sparklend-backfill erigon-sparklend-backfill \
		kind-create kind-delete kind-infra kind-config kind-secrets kind-bootstrap-db kind-migrate \
		kind-load-migrate kind-load-watcher kind-load-workers kind-load-python-api kind-load-cronjobs \
		kind-deploy-watcher kind-deploy-workers kind-deploy-python-api kind-deploy-cronjobs generate-cronjob-manifests \
		kind-redeploy-watcher kind-redeploy-worker \
		docker-build-migrate \
		kind-infra-ci kind-ci-up kind-ci-down \
		docker-build-python-api docker-push-python-api docker-release-python-api \
		docker-release-python-api-staging \
		docker-build-mock-blockchain-server docker-push-mock-blockchain-server docker-release-mock-blockchain-server \
		docker-release-mock-blockchain-server-staging \
		kind-build-mock-blockchain-server kind-load-mock-blockchain-server kind-deploy-mock-blockchain-server kind-deploy-mock-blockchain-server-s3 \
		kind-use-mock kind-use-alchemy k6-stress-test stress-test-data stress-test kind-clean

# Go bin path for installed tools
GOBIN := $(shell go env GOPATH)/bin

# Local kind cluster name — single source of truth; KUBE_CONTEXT is derived from it.
# To target EKS: KUBE_CONTEXT=<arn> KUBE_NS=<ns>
KIND_CLUSTER ?= vector
KUBE_CONTEXT ?= kind-$(KIND_CLUSTER)
KUBE_NS      ?= vector

# Run the application
run:
	@echo "==> Running..."
	go run ./cmd/server

# Run the Ethereum watcher (requires dev-up first)
run-watcher:
	@echo "==> Running Ethereum watcher (chain_id=1)..."
	@set -a && . cmd/base/watcher/.env && [ -f cmd/base/watcher/.env.local ] && . cmd/base/watcher/.env.local; set +a && go run ./cmd/base/watcher

# Run the Avalanche watcher (requires dev-up first)
# Avalanche C-Chain does not support trace_block, so traces are disabled.
run-watcher-avax:
	@echo "==> Running Avalanche watcher (chain_id=43114)..."
	@set -a && . cmd/base/watcher/.env.avax && [ -f cmd/base/watcher/.env.avax.local ] && . cmd/base/watcher/.env.avax.local; set +a && go run ./cmd/base/watcher --enable-traces=false

# Run the oracle price worker (requires dev-up first)
run-oracle-price-worker:
	@echo "==> Running oracle price worker..."
	@set -a && . cmd/workers/oracle-price-indexer/.env && [ -f cmd/workers/oracle-price-indexer/.env.local ] && . cmd/workers/oracle-price-indexer/.env.local; set +a && go run ./cmd/workers/oracle-price-indexer

# Run the Avalanche oracle price worker (requires dev-up first)
run-oracle-price-worker-avax:
	@echo "==> Running Avalanche oracle price worker (chain_id=43114)..."
	@set -a && . cmd/workers/oracle-price-indexer/.env.avax && [ -f cmd/workers/oracle-price-indexer/.env.avax.local ] && . cmd/workers/oracle-price-indexer/.env.avax.local; set +a && go run ./cmd/workers/oracle-price-indexer

# Run the Morpho indexer (requires dev-up first)
run-morpho-indexer:
	@echo "==> Running Morpho indexer..."
	@set -a && . cmd/workers/morpho-indexer/.env && [ -f cmd/workers/morpho-indexer/.env.local ] && . cmd/workers/morpho-indexer/.env.local; set +a && go run ./cmd/workers/morpho-indexer

# Run the allocation tracker (requires dev-up first)
run-allocation-tracker:
	@echo "==> Running allocation tracker..."
	@set -a && . cmd/workers/prime-allocation-indexer/.env && [ -f cmd/workers/prime-allocation-indexer/.env.local ] && . cmd/workers/prime-allocation-indexer/.env.local; set +a && go run ./cmd/workers/prime-allocation-indexer

# Run the Avalanche allocation tracker (requires dev-up first)
run-allocation-tracker-avax:
	@echo "==> Running Avalanche allocation tracker (chain_id=43114)..."
	@set -a && . cmd/workers/prime-allocation-indexer/.env.avax && [ -f cmd/workers/prime-allocation-indexer/.env.avax.local ] && . cmd/workers/prime-allocation-indexer/.env.avax.local; set +a && go run ./cmd/workers/prime-allocation-indexer

# Run the SparkLend position tracker (requires dev-up first)
run-sparklend-position-tracker:
	@echo "==> Running SparkLend position tracker..."
	@set -a && . cmd/workers/sparklend-indexer/.env && [ -f cmd/workers/sparklend-indexer/.env.local ] && . cmd/workers/sparklend-indexer/.env.local; set +a && go run ./cmd/workers/sparklend-indexer

# Run the Avalanche SparkLend position tracker (requires dev-up first)
run-sparklend-position-tracker-avax:
	@echo "==> Running Avalanche SparkLend position tracker (chain_id=43114)..."
	@set -a && . cmd/workers/sparklend-indexer/.env.avax && [ -f cmd/workers/sparklend-indexer/.env.avax.local ] && . cmd/workers/sparklend-indexer/.env.avax.local; set +a && go run ./cmd/workers/sparklend-indexer

# Run tests with race detector
test-race:
	@echo "==> Running tests with race detector..."
	go test -race -v ./...

# Run unit tests only (default, no build tags)
test:
	@echo "==> Running unit tests..."
	go test -v ./...

test-integration: 
	@echo "==> Running integration tests..."
	GOEXPERIMENT=goroutineleakprofile go test -tags=integration -v -timeout=10m -p 2 ./...

# Run tests with coverage
cover:
	@echo "==> Running tests with coverage..."
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Run all tests (including integration) with coverage
cover-all:
	@echo "==> Running all tests with coverage (including integration)..."
	go test -tags=integration -coverprofile=coverage.out -coverpkg=./... ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Run go vet
vet:
	@echo "==> Running go vet..."
	go vet ./...

# Apply go fix modernizations
fix:
	@echo "==> Applying go fix..."
	go fix ./...

# Check if go fix has been applied (CI-friendly, fails if not)
fix-check:
	@echo "==> Checking go fix..."
	@diff=$$(go fix -diff ./... 2>/dev/null); \
	if [ -n "$$diff" ]; then \
		echo "Code needs modernizing. Run 'go fix ./...'"; \
		echo "$$diff"; \
		exit 1; \
	fi

# Format code
fmt:
	@echo "==> Formatting code..."
	go fmt ./...
	gofmt -s -w .

# Check formatting (CI-friendly, fails if not formatted)
fmt-check:
	@echo "==> Checking format..."
	@test -z "$$(gofmt -l .)" || (echo "Files not formatted:"; gofmt -l .; exit 1)

# Check import formatting (CI-friendly, fails if imports not organized)
goimports-check:
	@echo "==> Checking imports..."
	@test -z "$$($(GOBIN)/goimports -l .)" || (echo "Imports not formatted:"; $(GOBIN)/goimports -l .; exit 1)

# Tidy and verify dependencies
tidy:
	@echo "==> Tidying modules..."
	go mod tidy
	go mod verify

# Check if go.mod is tidy (CI-friendly, fails if not tidy)
tidy-check:
	@echo "==> Checking go.mod is tidy..."
	@cp go.mod go.mod.bak && cp go.sum go.sum.bak
	@go mod tidy
	@diff -q go.mod go.mod.bak > /dev/null || (echo "go.mod is not tidy. Run 'go mod tidy'"; mv go.mod.bak go.mod; mv go.sum.bak go.sum; exit 1)
	@diff -q go.sum go.sum.bak > /dev/null || (echo "go.sum is not tidy. Run 'go mod tidy'"; mv go.mod.bak go.mod; mv go.sum.bak go.sum; exit 1)
	@mv go.mod.bak go.mod && mv go.sum.bak go.sum
	@echo "go.mod is tidy"

# Run staticcheck with all checks (install if needed)
staticcheck:
	@echo "==> Running staticcheck..."
	$(GOBIN)/staticcheck ./...

# Run golangci-lint with all linters (install if needed)
golangci-lint:
	@echo "==> Running golangci-lint..."
	$(GOBIN)/golangci-lint run

# Run govulncheck for security vulnerabilities (install if needed)
# GO-2026-4887, GO-2026-4883: docker/docker vulns with no upstream fix — only affects test code (testcontainers)
GOVULN_IGNORE := GO-2026-4887|GO-2026-4883
vulncheck:
	@echo "==> Running govulncheck..."
	@$(GOBIN)/govulncheck ./... 2>&1; rc=$$?; \
	if [ $$rc -eq 0 ]; then exit 0; fi; \
	unmatched=$$($(GOBIN)/govulncheck ./... 2>&1 | grep '^Vulnerability' | grep -v -E '$(GOVULN_IGNORE)'); \
	if [ -n "$$unmatched" ]; then echo "ERROR: unignored vulnerabilities found"; exit 1; fi; \
	echo "WARNING: only ignored vulnerabilities found ($(GOVULN_IGNORE)) — passing"

# Clean build artifacts
clean:
	@echo "==> Cleaning..."
	rm -rf bin/
	rm -f coverage.out coverage.html blockstate.out blockstate_coverage.html

# Run PostgreSQL performance benchmarks (10M rows)
bench-postgres:
	@echo "==> Running PostgreSQL performance benchmarks..."
	@echo "    This will insert 10 million rows and test query performance."
	@echo "    Expected runtime: 2-3 minutes"
	go test -tags=benchmark -v -timeout=5m -run TestLargeDataset_QueryPerformance ./internal/adapters/outbound/postgres/...

# Run PostgreSQL EXPLAIN ANALYZE (for query plan inspection)
bench-postgres-explain:
	@echo "==> Running PostgreSQL EXPLAIN ANALYZE tests..."
	go test -tags=benchmark -v -timeout=1m -run TestExplainAnalyze ./internal/adapters/outbound/postgres/...

# Run end-to-end tests (requires Docker)
e2e:
	@echo "==> Running end-to-end tests..."
	@echo "    This uses testcontainers to spin up PostgreSQL, Redis, and LocalStack."
	go test -tags=e2e -v -timeout=5m ./cmd/base/watcher/...

# Install development tools
tools:
	@echo "==> Installing development tools..."
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/go-delve/delve/cmd/dlv@latest
	go install golang.org/x/tools/gopls@latest

# CI target for lint/format/static/security checks
ci-checks: fix-check fmt-check goimports-check tidy-check vet staticcheck golangci-lint vulncheck

# CI target: everything with strict checks
# Order: fast checks first (fail fast), then expensive checks
ci: ci-checks test-race

# =============================================================================
# Local Development
# =============================================================================

# Build a Docker image only if COLD=1 or the image does not exist locally.
# Usage: $(call build-if-needed,<image:tag>,<make-target> [ARGS])
define build-if-needed
@if [ "$(COLD)" = "1" ] || ! docker image inspect $(1) >/dev/null 2>&1; then \
  $(MAKE) $(2); \
else \
  echo "  $(1) already present — skipping build (use COLD=1 to force rebuild)"; \
fi
endef

# Verify required tools are installed before starting the cluster.
dev-preflight:
	@command -v kind    >/dev/null 2>&1 || { echo "ERROR: kind not found. Install from https://kind.sigs.k8s.io/docs/user/quick-start/#installation"; exit 1; }
	@command -v kubectl >/dev/null 2>&1 || { echo "ERROR: kubectl not found. Install from https://kubernetes.io/docs/tasks/tools/"; exit 1; }
	@docker info        >/dev/null 2>&1 || { echo "ERROR: Docker is not running. Start Docker and try again."; exit 1; }
	@if [ ! -f ../.env.secrets ]; then \
	  printf 'ALCHEMY_API_KEY=\nCOINGECKO_API_KEY=\nETHERSCAN_API_KEY=\nANCHORAGE_API_KEY=\n' > ../.env.secrets && chmod 600 ../.env.secrets; \
	  echo ""; \
	  echo "ℹ️  .env.secrets created at the repo root with empty values."; \
	  echo "   The watcher will use the mock blockchain server by default."; \
	  echo "   To switch to real Alchemy: set ALCHEMY_API_KEY in .env.secrets, then run:"; \
	  echo "   make kind-secrets && make kind-use-alchemy"; \
	  echo ""; \
	elif ! grep -q 'ALCHEMY_API_KEY=.\+' ../.env.secrets; then \
	  echo ""; \
	  echo "ℹ️  ALCHEMY_API_KEY is not set in .env.secrets — watcher will use the mock blockchain server."; \
	  echo "   To switch to real Alchemy at any time: set ALCHEMY_API_KEY, then run:"; \
	  echo "   make kind-secrets && make kind-use-alchemy"; \
	  echo ""; \
	fi

# Start the local kind cluster and deploy the full pipeline.
# Warm start (default): reuses existing Docker images if present.
# Cold start: COLD=1 make dev-up — rebuilds all images from scratch.
dev-up:
	@$(MAKE) dev-preflight
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  Cluster"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	$(MAKE) kind-create
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  Infrastructure"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	kubectl --context=$(KUBE_CONTEXT) create namespace $(KUBE_NS) --dry-run=client -o yaml | kubectl --context=$(KUBE_CONTEXT) apply -f -
	$(MAKE) kind-config kind-secrets kind-infra
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  Database migrations"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	$(call build-if-needed,stl-migrate:local,docker-build-migrate)
	$(MAKE) kind-load-migrate kind-migrate
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  Mock Blockchain Server"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	$(call build-if-needed,stl-mock-blockchain-server:local,kind-build-mock-blockchain-server)
	$(MAKE) kind-load-mock-blockchain-server
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/apps/mock-blockchain-server.yaml -n $(KUBE_NS)
	@echo "==> Waiting for mock-blockchain-server to be ready..."
	kubectl --context=$(KUBE_CONTEXT) rollout status deployment/mock-blockchain-server -n $(KUBE_NS) --timeout=120s
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  Watcher"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	$(call build-if-needed,stl-watcher:local,docker-build-watcher LOCAL=1)
	$(MAKE) kind-load-watcher kind-deploy-watcher
	$(MAKE) kind-use-mock
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  Workers"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@set -a; . ../.env.secrets 2>/dev/null; set +a; \
	if [ -n "$$ALCHEMY_API_KEY" ]; then \
	  echo "==> API keys found — deploying workers..."; \
	  $(MAKE) dev-up-workers; \
	else \
	  echo "==> API keys not set — skipping workers."; \
	fi
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  Python API"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	$(call build-if-needed,stl-python-api:local,docker-build-python-api LOCAL=1)
	$(MAKE) kind-load-python-api kind-deploy-python-api
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  Cronjobs (Temporal)"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@for cj in $(CRONJOBS); do \
	  if [ "$(COLD)" = "1" ] || ! docker image inspect stl-$$cj:local >/dev/null 2>&1; then \
	    $(MAKE) docker-build-cronjob-$$cj; \
	  else \
	    echo "  stl-$$cj:local already present — skipping build (use COLD=1 to force rebuild)"; \
	  fi; \
	done
	$(MAKE) kind-load-cronjobs kind-deploy-cronjobs
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  Ready"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "==> Waiting for all deployments to roll out..."
	@set -a; . ../.env.secrets 2>/dev/null; set +a; \
	if [ -n "$$ALCHEMY_API_KEY" ]; then \
	  DEPS="mock-blockchain-server watcher morpho-indexer oracle-price-worker sparklend-position-tracker prime-debt-indexer temporal-worker python-api $(CRONJOBS)"; \
	else \
	  DEPS="mock-blockchain-server watcher python-api $(CRONJOBS)"; \
	fi; \
	for dep in $$DEPS; do \
	  kubectl --context=$(KUBE_CONTEXT) rollout status deployment/$$dep -n $(KUBE_NS) --timeout=120s || \
	    { echo "ERROR: deployment $$dep not ready after 120s"; kubectl --context=$(KUBE_CONTEXT) get pods -n $(KUBE_NS); exit 1; }; \
	done
	@kubectl --context=$(KUBE_CONTEXT) get pods -n $(KUBE_NS)
	@echo ""
	@set -a; . ../.env.secrets 2>/dev/null; set +a; \
	if [ -z "$$ALCHEMY_API_KEY" ]; then \
	  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; \
	  echo "  ℹ️  Workers not deployed (missing API keys)"; \
	  echo ""; \
	  echo "  The watcher pipeline is running with the mock blockchain server."; \
	  echo "  Workers require the following keys in .env.secrets:"; \
	  echo ""; \
	  echo "    ALCHEMY_API_KEY    — oracle-price-worker, morpho-indexer,"; \
	  echo "                         sparklend-position-tracker, prime-debt-indexer"; \
	  echo "    COINGECKO_API_KEY  — temporal-worker"; \
	  echo "    ETHERSCAN_API_KEY  — temporal-worker (data validation)"; \
	  echo ""; \
	  echo "  Once set, run: make kind-secrets && make dev-up-workers"; \
	  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; \
	fi

# Build and deploy workers that require API keys.
# Checks which keys are available and only deploys workers whose dependencies are met.
# Usage: make kind-secrets && make dev-up-workers
dev-up-workers:
	@set -a; . ../.env.secrets 2>/dev/null; set +a; \
	MISSING=""; \
	if [ -z "$$ALCHEMY_API_KEY" ]; then \
	  MISSING="$$MISSING\n    ALCHEMY_API_KEY    — oracle-price-worker, morpho-indexer, sparklend-position-tracker, prime-debt-indexer"; \
	fi; \
	if [ -z "$$COINGECKO_API_KEY" ]; then \
	  MISSING="$$MISSING\n    COINGECKO_API_KEY  — temporal-worker"; \
	fi; \
	if [ -z "$$ETHERSCAN_API_KEY" ]; then \
	  MISSING="$$MISSING\n    ETHERSCAN_API_KEY  — temporal-worker (data validation)"; \
	fi; \
	if [ -n "$$MISSING" ]; then \
	  echo "==> Missing API keys:"; \
	  printf "$$MISSING\n\n"; \
	  echo "    Set them in .env.secrets and run: make kind-secrets && make dev-up-workers"; \
	  echo ""; \
	fi; \
	if [ -n "$$ALCHEMY_API_KEY" ]; then \
	  echo "==> Deploying Alchemy workers (oracle-price-worker, morpho-indexer, sparklend-position-tracker, prime-debt-indexer)..."; \
	  $(MAKE) _dev-up-alchemy-workers; \
	else \
	  echo "==> Skipping Alchemy workers (no ALCHEMY_API_KEY)"; \
	fi; \
	if [ -n "$$COINGECKO_API_KEY" ]; then \
	  echo "==> Deploying temporal-worker..."; \
	  $(MAKE) _dev-up-temporal-worker; \
	else \
	  echo "==> Skipping temporal-worker (no COINGECKO_API_KEY)"; \
	fi

_dev-up-alchemy-workers:
	$(call build-if-needed,stl-oracle-price-worker:local,docker-build-oracle-price-worker LOCAL=1)
	$(call build-if-needed,stl-morpho-indexer:local,docker-build-morpho-indexer LOCAL=1)
	$(call build-if-needed,stl-sparklend-position-tracker:local,docker-build-sparklend-position-tracker LOCAL=1)
	$(call build-if-needed,stl-prime-debt-indexer:local,docker-build-prime-debt-indexer LOCAL=1)
	$(MAKE) kind-load-workers kind-deploy-workers
	@for dep in morpho-indexer oracle-price-worker sparklend-position-tracker prime-debt-indexer; do \
	  kubectl --context=$(KUBE_CONTEXT) rollout status deployment/$$dep -n $(KUBE_NS) --timeout=120s || \
	    { echo "ERROR: deployment $$dep not ready after 120s"; exit 1; }; \
	done

_dev-up-temporal-worker:
	$(call build-if-needed,stl-temporal-worker:local,docker-build-temporal-worker LOCAL=1)
	$(MAKE) kind-load-temporal-worker kind-deploy-temporal-worker
	@kubectl --context=$(KUBE_CONTEXT) rollout status deployment/temporal-worker -n $(KUBE_NS) --timeout=120s || \
	  { echo "ERROR: deployment temporal-worker not ready after 120s"; exit 1; }

# Start only the database and run migrations (no app services).
dev-db:
	@$(MAKE) dev-preflight
	$(MAKE) kind-create
	kubectl --context=$(KUBE_CONTEXT) create namespace $(KUBE_NS) --dry-run=client -o yaml | kubectl --context=$(KUBE_CONTEXT) apply -f -
	$(MAKE) kind-config kind-secrets
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/infra/timescaledb.yaml -n $(KUBE_NS)
	@echo "==> Waiting for timescaledb to be ready..."
	kubectl --context=$(KUBE_CONTEXT) rollout status statefulset/timescaledb -n $(KUBE_NS) --timeout=120s
	$(call build-if-needed,stl-migrate:local,docker-build-migrate)
	$(MAKE) kind-load-migrate kind-migrate
	@echo "==> Database ready."
	@echo "    Connection: postgres://postgres:postgres@127.0.0.1:30432/stl_verify?sslmode=disable"

# Generate .env files for all services (fetches secrets from AWS)
# Can be re-run independently to refresh secrets without restarting services.
dev-env:
	@ALCHEMY_API_KEY=$$(aws secretsmanager get-secret-value --secret-id stl-sentinelstaging-watcher-config --query SecretString --output text | jq -r '.alchemy_api_key // empty') || \
		{ echo "WARNING: Failed to fetch ALCHEMY_API_KEY from AWS Secrets Manager. Set it manually in .env.secrets"; ALCHEMY_API_KEY=''; }; \
	printf '%s\n' "ALCHEMY_API_KEY=$$ALCHEMY_API_KEY" > ../.env.secrets && chmod 600 ../.env.secrets; \
	printf '%s\n' \
		"ENVIRONMENT=development" \
		"ALCHEMY_API_KEY=$$ALCHEMY_API_KEY" \
		"ALCHEMY_HTTP_URL=https://eth-mainnet.g.alchemy.com/v2" \
		"ALCHEMY_WS_URL=wss://eth-mainnet.g.alchemy.com/v2" \
		"CHAIN_ID=1" \
		"AWS_SNS_ENDPOINT=http://127.0.0.1:4566" \
		"AWS_ACCESS_KEY_ID=test" \
		"AWS_SECRET_ACCESS_KEY=test" \
		"AWS_REGION=us-east-1" \
		"AWS_SNS_TOPIC_ARN=arn:aws:sns:us-east-1:000000000000:stl-ethereum-blocks.fifo" \
		"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" \
		"REDIS_ADDR=127.0.0.1:6379" \
		"REDIS_PASSWORD=" \
		"JAEGER_ENDPOINT=127.0.0.1:4317" \
		"OTEL_EXPORTER_OTLP_ENDPOINT=" \
		"ENABLE_BACKFILL=false" \
		> cmd/base/watcher/.env && \
	printf '%s\n' \
		"ENVIRONMENT=development" \
		"ALCHEMY_API_KEY=$$ALCHEMY_API_KEY" \
		"ALCHEMY_HTTP_URL=https://avax-mainnet.g.alchemy.com/v2" \
		"ALCHEMY_WS_URL=wss://avax-mainnet.g.alchemy.com/v2" \
		"CHAIN_ID=43114" \
		"AWS_SNS_ENDPOINT=http://127.0.0.1:4566" \
		"AWS_ACCESS_KEY_ID=test" \
		"AWS_SECRET_ACCESS_KEY=test" \
		"AWS_REGION=us-east-1" \
		"AWS_SNS_TOPIC_ARN=arn:aws:sns:us-east-1:000000000000:stl-avalanche-blocks.fifo" \
		"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" \
		"REDIS_ADDR=127.0.0.1:6379" \
		"REDIS_PASSWORD=" \
		"JAEGER_ENDPOINT=127.0.0.1:4317" \
		"OTEL_EXPORTER_OTLP_ENDPOINT=" \
		"ENABLE_BACKFILL=false" \
		> cmd/base/watcher/.env.avax && \
	printf '%s\n' \
		"ENVIRONMENT=development" \
		"ALCHEMY_API_KEY=$$ALCHEMY_API_KEY" \
		"ALCHEMY_HTTP_URL=https://eth-mainnet.g.alchemy.com/v2" \
		"AWS_SQS_QUEUE_URL=http://127.0.0.1:4566/000000000000/stl-ethereum-oracle-price.fifo" \
		"AWS_SQS_ENDPOINT=http://127.0.0.1:4566" \
		"AWS_ACCESS_KEY_ID=test" \
		"AWS_SECRET_ACCESS_KEY=test" \
		"AWS_REGION=us-east-1" \
		"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" \
		"JAEGER_ENDPOINT=127.0.0.1:4317" \
		"OTEL_EXPORTER_OTLP_ENDPOINT=" \
		"LOG_LEVEL=debug" \
		> cmd/workers/oracle-price-indexer/.env && \
	printf '%s\n' \
		"ENVIRONMENT=development" \
		"ALCHEMY_API_KEY=$$ALCHEMY_API_KEY" \
		"CHAIN_ID=43114" \
		"ALCHEMY_HTTP_URL=https://avax-mainnet.g.alchemy.com/v2" \
		"AWS_SQS_QUEUE_URL=http://127.0.0.1:4566/000000000000/stl-avalanche-oracle-price.fifo" \
		"AWS_SQS_ENDPOINT=http://127.0.0.1:4566" \
		"AWS_ACCESS_KEY_ID=test" \
		"AWS_SECRET_ACCESS_KEY=test" \
		"AWS_REGION=us-east-1" \
		"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" \
		"JAEGER_ENDPOINT=127.0.0.1:4317" \
		"OTEL_EXPORTER_OTLP_ENDPOINT=" \
		"LOG_LEVEL=debug" \
		> cmd/workers/oracle-price-indexer/.env.avax && \
	printf '%s\n' \
		"ENVIRONMENT=development" \
		"ALCHEMY_API_KEY=$$ALCHEMY_API_KEY" \
		"ALCHEMY_HTTP_URL=https://eth-mainnet.g.alchemy.com/v2" \
		"AWS_SQS_QUEUE_URL=http://127.0.0.1:4566/000000000000/stl-ethereum-sparklend-position.fifo" \
		"AWS_SQS_ENDPOINT=http://127.0.0.1:4566" \
		"AWS_S3_ENDPOINT=http://127.0.0.1:4566" \
		"AWS_ACCESS_KEY_ID=test" \
		"AWS_SECRET_ACCESS_KEY=test" \
		"AWS_REGION=us-east-1" \
		"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" \
		"REDIS_ADDR=127.0.0.1:6379" \
		"REDIS_PASSWORD=" \
		"CHAIN_ID=1" \
		"S3_BUCKET=stl-sentineldevelopment-ethereum-raw" \
		"DEPLOY_ENV=development" \
		"LOG_LEVEL=debug" \
		> cmd/workers/sparklend-indexer/.env && \
	printf '%s\n' \
		"ENVIRONMENT=development" \
		"ALCHEMY_API_KEY=$$ALCHEMY_API_KEY" \
		"ALCHEMY_HTTP_URL=https://avax-mainnet.g.alchemy.com/v2" \
		"AWS_SQS_QUEUE_URL=http://127.0.0.1:4566/000000000000/stl-avalanche-sparklend-position.fifo" \
		"AWS_SQS_ENDPOINT=http://127.0.0.1:4566" \
		"AWS_S3_ENDPOINT=http://127.0.0.1:4566" \
		"AWS_ACCESS_KEY_ID=test" \
		"AWS_SECRET_ACCESS_KEY=test" \
		"AWS_REGION=us-east-1" \
		"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" \
		"REDIS_ADDR=127.0.0.1:6379" \
		"REDIS_PASSWORD=" \
		"CHAIN_ID=43114" \
		"S3_BUCKET=stl-sentineldevelopment-avalanche-raw" \
		"DEPLOY_ENV=development" \
		"LOG_LEVEL=debug" \
		> cmd/workers/sparklend-indexer/.env.avax && \
	printf '%s\n' \
		"ENVIRONMENT=development" \
		"ALCHEMY_API_KEY=$$ALCHEMY_API_KEY" \
		"ALCHEMY_HTTP_URL=https://eth-mainnet.g.alchemy.com/v2" \
		"AWS_SQS_QUEUE_URL=http://127.0.0.1:4566/000000000000/stl-ethereum-allocation-tracker.fifo" \
		"AWS_SQS_ENDPOINT=http://127.0.0.1:4566" \
		"AWS_S3_ENDPOINT=http://127.0.0.1:4566" \
		"AWS_ACCESS_KEY_ID=test" \
		"AWS_SECRET_ACCESS_KEY=test" \
		"AWS_REGION=us-east-1" \
		"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" \
		"REDIS_ADDR=127.0.0.1:6379" \
		"REDIS_PASSWORD=" \
		"CHAIN_ID=1" \
		"S3_BUCKET=stl-sentineldevelopment-ethereum-raw" \
		"DEPLOY_ENV=development" \
		"LOG_LEVEL=debug" \
		> cmd/workers/prime-allocation-indexer/.env && \
	printf '%s\n' \
		"ENVIRONMENT=development" \
		"ALCHEMY_API_KEY=$$ALCHEMY_API_KEY" \
		"ALCHEMY_HTTP_URL=https://avax-mainnet.g.alchemy.com/v2" \
		"AWS_SQS_QUEUE_URL=http://127.0.0.1:4566/000000000000/stl-avalanche-allocation-tracker.fifo" \
		"AWS_SQS_ENDPOINT=http://127.0.0.1:4566" \
		"AWS_S3_ENDPOINT=http://127.0.0.1:4566" \
		"AWS_ACCESS_KEY_ID=test" \
		"AWS_SECRET_ACCESS_KEY=test" \
		"AWS_REGION=us-east-1" \
		"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" \
		"REDIS_ADDR=127.0.0.1:6379" \
		"REDIS_PASSWORD=" \
		"CHAIN_ID=43114" \
		"S3_BUCKET=stl-sentineldevelopment-avalanche-raw" \
		"DEPLOY_ENV=development" \
		"LOG_LEVEL=debug" \
		> cmd/workers/prime-allocation-indexer/.env.avax && \
	printf '%s\n' \
		"ENVIRONMENT=development" \
		"ALCHEMY_API_KEY=$$ALCHEMY_API_KEY" \
		"ALCHEMY_HTTP_URL=https://eth-mainnet.g.alchemy.com/v2" \
		"AWS_SQS_QUEUE_URL=http://127.0.0.1:4566/000000000000/stl-ethereum-morpho-indexing.fifo" \
		"AWS_SQS_ENDPOINT=http://127.0.0.1:4566" \
		"AWS_ACCESS_KEY_ID=test" \
		"AWS_SECRET_ACCESS_KEY=test" \
		"AWS_REGION=us-east-1" \
		"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" \
		"REDIS_ADDR=127.0.0.1:6379" \
		"REDIS_PASSWORD=" \
		"CHAIN_ID=1" \
		"JAEGER_ENDPOINT=127.0.0.1:4317" \
		"OTEL_EXPORTER_OTLP_ENDPOINT=" \
		"SQS_WAIT_TIME=1" \
		"SQS_VISIBILITY_TIMEOUT=30" \
		"LOG_LEVEL=debug" \
		> cmd/workers/morpho-indexer/.env
	@COINGECKO_API_KEY=$$(aws secretsmanager get-secret-value --secret-id coingecko_api_key --query SecretString --output text | jq -r '.coingecko_api_key // empty') || \
		{ echo "WARNING: Failed to fetch COINGECKO_API_KEY from AWS Secrets Manager."; COINGECKO_API_KEY=''; }; \
	printf '%s\n' "COINGECKO_API_KEY=$$COINGECKO_API_KEY" >> ../.env.secrets
	@printf '%s\n' \
		"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" \
		"LOG_LEVEL=debug" \
		> cmd/backfillers/oracle-pricing-backfill/.env
	@ETHERSCAN_API_KEY=$$(aws secretsmanager get-secret-value --secret-id etherscan_api_key --query SecretString --output text) || \
		{ echo "WARNING: Failed to fetch ETHERSCAN_API_KEY from AWS Secrets Manager."; ETHERSCAN_API_KEY=''; }; \
	printf '%s\n' "ETHERSCAN_API_KEY=$$ETHERSCAN_API_KEY" >> ../.env.secrets
	@ANCHORAGE_API_KEY=$$(aws secretsmanager get-secret-value --secret-id stl-sentinelstaging-anchorage-api-key --query SecretString --output text 2>/dev/null) || \
		{ echo "WARNING: Failed to fetch ANCHORAGE_API_KEY from AWS Secrets Manager."; ANCHORAGE_API_KEY=''; }; \
	printf '%s\n' "ANCHORAGE_API_KEY=$$ANCHORAGE_API_KEY" >> ../.env.secrets
	@set -a; . ../.env.secrets; set +a; \
	printf '%s\n' \
		"CHAIN_ID=1" \
		"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" \
		"TEMPORAL_HOST_PORT=127.0.0.1:7233" \
		"TEMPORAL_NAMESPACE=vector" \
		"COINGECKO_API_KEY=$$COINGECKO_API_KEY" \
		"LOG_LEVEL=debug" \
		> cmd/cronjobs/offchain-price-indexer/.env; \
	printf '%s\n' \
		"CHAIN_ID=1" \
		"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" \
		"TEMPORAL_HOST_PORT=127.0.0.1:7233" \
		"TEMPORAL_NAMESPACE=vector" \
		"ETHERSCAN_API_KEY=$$ETHERSCAN_API_KEY" \
		"LOG_LEVEL=debug" \
		> cmd/cronjobs/watcher-data-validator/.env; \
	printf '%s\n' \
		"CHAIN_ID=1" \
		"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" \
		"TEMPORAL_HOST_PORT=127.0.0.1:7233" \
		"TEMPORAL_NAMESPACE=vector" \
		"ANCHORAGE_API_URL=" \
		"ANCHORAGE_API_KEY=$$ANCHORAGE_API_KEY" \
		"ANCHORAGE_PRIME=" \
		"LOG_LEVEL=debug" \
		> cmd/cronjobs/anchorage-indexer/.env
	@echo "Environment files generated (.env.secrets updated)."

# Cold start — rebuilds all images from scratch
dev-up-rebuild:
	$(MAKE) dev-up COLD=1

dev-down: kind-delete

# Wipe cluster and all persisted data. Cannot be undone.
dev-wipe:
	@echo "WARNING: This will tear down the cluster and permanently delete all local database data."
	@read -rp "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	$(MAKE) kind-delete
	rm -rf $$HOME/.$(KIND_CLUSTER)

# Wipe and recreate the local kind cluster
dev-reset: dev-down dev-up

# Show service IPs (useful for devcontainer)
dev-ips:
	@echo "=== Service IPs ==="
	@docker inspect stl-verify-postgres --format 'Postgres:    {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 2>/dev/null || echo "Postgres: not running"
	@docker inspect stl-verify-redis --format 'Redis:       {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 2>/dev/null || echo "Redis: not running"
	@docker inspect stl-verify-localstack --format 'LocalStack:  {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 2>/dev/null || echo "LocalStack: not running"
	@docker inspect stl-verify-jaeger --format 'Jaeger:      {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 2>/dev/null || echo "Jaeger: not running"

# Generate ER diagram from database schema (requires dev-up)
generate-er:
	@echo "==> Generating ER diagram..."
	@DATABASE_URL="postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" go run ./cmd/generate-er --output ../docs/entity_relation.md
	@echo "==> Generated: docs/entity_relation.md"

# =============================================================================
# Docker Build & Push
# =============================================================================

# AWS Account ID and Region (override via environment or command line)
AWS_ACCOUNT_ID ?= $(shell aws sts get-caller-identity --query Account --output text 2>/dev/null)
AWS_REGION ?= eu-west-1

# ECR repository URL
ECR_REPO = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/stl-$(ENV)-watcher

# Git info for image tagging and build metadata
GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_COMMIT_FULL ?= $(shell git rev-parse HEAD 2>/dev/null || echo "unknown")
GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
BUILD_TIME ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# Image tag (default to git sha, can override)
IMAGE_TAG ?= $(GIT_COMMIT)

# Login to ECR
ecr-login:
	@echo "==> Logging in to ECR..."
	aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com

# Build Docker image for ARM64 (Fargate Graviton)
docker-build:
	@echo "==> Building Docker image for ARM64..."
	@echo "    Repository: $(ECR_REPO)"
	@echo "    Tag: $(IMAGE_TAG)"
	@echo "    Commit: $(GIT_COMMIT_FULL)"
	@echo "    Branch: $(GIT_BRANCH)"
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$(GIT_COMMIT_FULL) \
		--build-arg GIT_BRANCH=$(GIT_BRANCH) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(ECR_REPO):$(IMAGE_TAG) \
		-t $(ECR_REPO):latest \
		--load .

# Push Docker image to ECR
docker-push: ecr-login
	@echo "==> Pushing Docker image to ECR..."
	docker push $(ECR_REPO):$(IMAGE_TAG)
	docker push $(ECR_REPO):latest

# Build and push in one step (single recipe to ensure consistent IMAGE_TAG)
docker-release: ecr-login _docker-release-internal

_docker-release-internal:
	@IMAGE_TAG=$(IMAGE_TAG) && \
	GIT_COMMIT_FULL=$$(git rev-parse HEAD) && \
	GIT_BRANCH=$$(git rev-parse --abbrev-ref HEAD) && \
	BUILD_TIME=$$(date -u +"%Y-%m-%dT%H:%M:%SZ") && \
	echo "==> Building Docker image for ARM64..." && \
	echo "    Repository: $(ECR_REPO)" && \
	echo "    Tag: $$IMAGE_TAG" && \
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$$GIT_COMMIT_FULL \
		--build-arg GIT_BRANCH=$$GIT_BRANCH \
		--build-arg BUILD_TIME=$$BUILD_TIME \
		-t $(ECR_REPO):$$IMAGE_TAG \
		-t $(ECR_REPO):latest \
		--load . && \
	echo "==> Pushing Docker image to ECR..." && \
	docker push $(ECR_REPO):$$IMAGE_TAG && \
	docker push $(ECR_REPO):latest && \
	echo "==> Docker image released: $(ECR_REPO):$$IMAGE_TAG"

# Clean up local Docker images
docker-clean:
	@echo "==> Cleaning up Docker images..."
	-docker rmi $$(docker images --filter=reference='*stl-*-watcher*' -q) 2>/dev/null || true
	-docker builder prune -f
	@echo "==> Docker cleanup complete"

# Force new deployment of watcher service (pulls latest image)
# Usage: make ecs-deploy-watcher ENV=sentineldev|sentinelstaging|sentinelprod
ecs-deploy-watcher:
	@echo "==> Forcing new deployment of watcher service..."
	aws ecs update-service --cluster stl-$(ENV)-cluster --service stl-$(ENV)-watcher --force-new-deployment --region $(AWS_REGION)
	@echo "==> Deployment initiated. Monitor with: aws ecs describe-services --cluster stl-$(ENV)-cluster --services stl-$(ENV)-watcher --region $(AWS_REGION)"

# Named aliases for consistency with other service targets
docker-build-watcher:
ifdef LOCAL
	docker build -t stl-watcher:local -f Dockerfile .
else
	@$(MAKE) docker-build
endif

docker-push-watcher:
	@$(MAKE) docker-push

docker-release-watcher:
	@$(MAKE) docker-release

# =============================================================================
# Sentinelstaging Watcher Docker Shortcuts
# =============================================================================

docker-build-watcher-staging:
	@$(MAKE) docker-build-watcher ENV=sentinelstaging

docker-push-watcher-staging:
	@$(MAKE) docker-push-watcher ENV=sentinelstaging

docker-release-watcher-staging:
	@$(MAKE) docker-release-watcher ENV=sentinelstaging

ecs-deploy-watcher-staging:
	@$(MAKE) ecs-deploy-watcher ENV=sentinelstaging

# =============================================================================
# Combined Docker Release (All Services)
# =============================================================================

# Release all services for a given environment
# Usage: make docker-release-all ENV=sentineldev|sentinelstaging|sentinelprod
docker-release-all:
	@if [ -z "$(ENV)" ]; then \
		echo "Error: ENV is required. Usage: make docker-release-all ENV=sentineldev|sentinelstaging|sentinelprod"; \
		exit 1; \
	fi
	@$(MAKE) ecr-login ENV=$(ENV)
	@echo "==> Releasing all Docker images for ENV=$(ENV)..."
	@echo ""
	@$(MAKE) _docker-release-migrate-internal ENV=$(ENV)
	@echo ""
	@$(MAKE) _docker-release-internal ENV=$(ENV)
	@echo ""
	@$(MAKE) _docker-release-backup-internal ENV=$(ENV)
	@echo ""
	@$(MAKE) _docker-release-oracle-price-worker-internal ENV=$(ENV)
	@echo ""
	@$(MAKE) _docker-release-sparklend-position-tracker-internal ENV=$(ENV)
	@echo ""
	@$(MAKE) _docker-release-morpho-indexer-internal ENV=$(ENV)
	@echo ""
	@$(MAKE) _docker-release-allocation-tracker-internal ENV=$(ENV)
	@echo ""
	@$(MAKE) _docker-release-prime-debt-indexer-internal ENV=$(ENV)
	@echo ""
	@$(MAKE) _docker-release-python-api-internal ENV=$(ENV)
	@echo ""
	@for cj in $(CRONJOBS); do \
		$(MAKE) _docker-release-cronjob-$$cj-internal ENV=$(ENV) || { echo "ERROR: Failed to release cronjob $$cj"; exit 1; }; \
		echo ""; \
	done
	@echo "==> All Docker images released for ENV=$(ENV)"

# Shortcut: Release all services for sentinelstaging
docker-release-all-staging:
	@$(MAKE) docker-release-all ENV=sentinelstaging

# =============================================================================
# Bastion Host Management
# =============================================================================

# SSH tunnel to TigerData via bastion (after Tailscale is connected)
# Auto-discovers bastion and TigerData details from AWS
# Usage: make tigerdata-tunnel ENV=sentineldev
# Or manual: make tigerdata-tunnel BASTION_IP=<tailscale-ip> TIGERDATA_HOST=<tigerdata-host>
# Then connect locally: psql "postgres://user:pass@localhost:5432/dbname"
tigerdata-tunnel:
	@if [ -z "$(BASTION_IP)" ] || [ -z "$(TIGERDATA_HOST)" ]; then \
		echo "Error: BASTION_IP and TIGERDATA_HOST are required"; \
		echo "Usage: make $@ BASTION_IP=<ip> TIGERDATA_HOST=<tigerdata-host>"; \
		exit 1; \
	fi
	ssh -N -L 5432:$(TIGERDATA_HOST):5432 ec2-user@$(BASTION_IP)

# =============================================================================
# Database Migrations
# =============================================================================

# Run database migrations
# Usage: make db-migrate DATABASE_URL="postgresql://user:pass@localhost:5432/dbname?sslmode=require"
# Requires tunnel to be running first: make tigerdata-tunnel BASTION_IP=... TIGERDATA_HOST=...
db-migrate:
	@if [ -z "$(DATABASE_URL)" ]; then \
		echo "Error: DATABASE_URL is required"; \
		echo "Usage: make $@ DATABASE_URL=\"postgresql://user:pass@localhost:5432/dbname?sslmode=require\""; \
		echo ""; \
		echo "First start the tunnel in another terminal:"; \
		echo "  make tigerdata-tunnel BASTION_IP=stl-sentinelstaging-bastion TIGERDATA_HOST=<pooler-host>"; \
		exit 1; \
	fi
	@echo "==> Running database migrations..."
	DATABASE_URL="$(DATABASE_URL)" go run ./cmd/migrate
	@echo "==> Migrations complete"

# Set database user passwords from Secrets Manager
# Usage: make db-set-passwords DATABASE_URL="postgresql://tsdbadmin:pass@localhost:5432/tsdb?sslmode=require" ENV=sentineldev|sentinelstaging|sentinelprod
# Requires:
#   1. Tunnel running: make tigerdata-tunnel BASTION_IP=... TIGERDATA_HOST=...
#   2. Terraform applied (from infrastructure repo): make tf-apply ENV=sentineldev|sentinelstaging|sentinelprod (creates secrets with passwords)
#   3. Migrations run: make db-migrate DATABASE_URL=... (creates users with placeholder passwords)
db-set-passwords:
	@if [ -z "$(DATABASE_URL)" ] || [ -z "$(ENV)" ]; then \
		echo "Error: DATABASE_URL and ENV are required"; \
		echo "Usage: make $@ DATABASE_URL=\"postgresql://tsdbadmin:pass@localhost:5432/tsdb?sslmode=require\" ENV=sentineldev|sentinelstaging|sentinelprod"; \
		exit 1; \
	fi
	@echo "==> Setting database user passwords from Secrets Manager..."
	@STL_RW_PASS=$$(aws secretsmanager get-secret-value --secret-id stl-$(ENV)-tigerdata-app --query SecretString --output text | jq -r .password) && \
	STL_RO_PASS=$$(aws secretsmanager get-secret-value --secret-id stl-$(ENV)-tigerdata-readonly --query SecretString --output text | jq -r .password) && \
	echo "Updating stl_read_write password..." && \
	psql "$(DATABASE_URL)" -c "ALTER USER stl_read_write WITH PASSWORD '$$STL_RW_PASS';" && \
	echo "Updating stl_read_only password..." && \
	psql "$(DATABASE_URL)" -c "ALTER USER stl_read_only WITH PASSWORD '$$STL_RO_PASS';" && \
	echo "==> Database user passwords configured successfully"

# Test database connection with application user
# Usage: make db-test-app-user ENV=sentineldev|sentinelstaging|sentinelprod
# Requires tunnel to be running first: make tigerdata-tunnel BASTION_IP=... TIGERDATA_HOST=...
db-test-app-user:
	@if [ -z "$(ENV)" ]; then \
		echo "Error: ENV is required"; \
		echo "Usage: make $@ ENV=sentineldev|sentinelstaging|sentinelprod"; \
		exit 1; \
	fi
	@echo "==> Testing stl_read_write connection..."
	@CONNECTION_URL=$$(aws secretsmanager get-secret-value --secret-id stl-$(ENV)-tigerdata-app --query SecretString --output text | jq -r .connection_url | sed 's|@[^:]*:|@localhost:|') && \
	psql "$$CONNECTION_URL" -c "SELECT current_user, current_database();" && \
	echo "" && \
	echo "==> Testing SELECT permission..." && \
	psql "$$CONNECTION_URL" -c "SELECT COUNT(*) FROM block_states;" && \
	echo "" && \
	echo "==> Testing INSERT permission (will rollback)..." && \
	psql "$$CONNECTION_URL" -c "BEGIN; INSERT INTO reorg_events (block_number, old_hash, new_hash, depth) VALUES (0, 'test', 'test', 1); ROLLBACK;" && \
	echo "==> All tests passed!"

# =============================================================================
# Docker Build & Push - Backup Worker
# =============================================================================

# ECR repository URL for backup worker
ECR_REPO_BACKUP = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/stl-$(ENV)-backup-worker

# Build Docker image for backup worker (ARM64 for Fargate Graviton)
docker-build-backup:
	@echo "==> Building Backup Worker Docker image for ARM64..."
	@echo "    Repository: $(ECR_REPO_BACKUP)"
	@echo "    Tag: $(IMAGE_TAG)"
	@echo "    Commit: $(GIT_COMMIT_FULL)"
	@echo "    Branch: $(GIT_BRANCH)"
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$(GIT_COMMIT_FULL) \
		--build-arg GIT_BRANCH=$(GIT_BRANCH) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-f Dockerfile.backup \
		-t $(ECR_REPO_BACKUP):$(IMAGE_TAG) \
		-t $(ECR_REPO_BACKUP):latest \
		--load .

# Push backup worker Docker image to ECR
docker-push-backup: ecr-login
	@echo "==> Pushing Backup Worker Docker image to ECR..."
	docker push $(ECR_REPO_BACKUP):$(IMAGE_TAG)
	docker push $(ECR_REPO_BACKUP):latest

# Build and push backup worker in one step (captures tag at start to ensure consistency)
docker-release-backup: ecr-login _docker-release-backup-internal

_docker-release-backup-internal:
	@TAG=$(IMAGE_TAG) && \
	COMMIT=$$(git rev-parse HEAD) && \
	BRANCH=$$(git rev-parse --abbrev-ref HEAD) && \
	TIME=$$(date -u +"%Y-%m-%dT%H:%M:%SZ") && \
	echo "==> Building Backup Worker Docker image for ARM64..." && \
	echo "    Repository: $(ECR_REPO_BACKUP)" && \
	echo "    Tag: $$TAG" && \
	echo "    Commit: $$COMMIT" && \
	echo "    Branch: $$BRANCH" && \
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$$COMMIT \
		--build-arg GIT_BRANCH=$$BRANCH \
		--build-arg BUILD_TIME=$$TIME \
		-f Dockerfile.backup \
		-t $(ECR_REPO_BACKUP):$$TAG \
		-t $(ECR_REPO_BACKUP):latest \
		--load . && \
	echo "==> Pushing Backup Worker Docker image to ECR..." && \
	docker push $(ECR_REPO_BACKUP):$$TAG && \
	docker push $(ECR_REPO_BACKUP):latest && \
	echo "==> Backup Worker Docker image released: $(ECR_REPO_BACKUP):$$TAG"

# =============================================================================
# Sentinelstaging Backup Worker Docker Shortcuts
# =============================================================================

docker-build-backup-staging:
	@$(MAKE) docker-build-backup ENV=sentinelstaging

docker-push-backup-staging:
	@$(MAKE) docker-push-backup ENV=sentinelstaging

docker-release-backup-staging:
	@$(MAKE) docker-release-backup ENV=sentinelstaging

# =============================================================================
# Docker Build & Push - Oracle Price Worker
# =============================================================================

# ECR repository URL for oracle price worker
ECR_REPO_ORACLE_PRICE_WORKER = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/stl-$(ENV)-oracle-price-worker

# Build Docker image for oracle price worker (ARM64 for Fargate Graviton)
docker-build-oracle-price-worker:
ifdef LOCAL
	docker build -t stl-oracle-price-worker:local -f Dockerfile.oracle-price-worker .
else
	@echo "==> Building Oracle Price Worker Docker image for ARM64..."
	@echo "    Repository: $(ECR_REPO_ORACLE_PRICE_WORKER)"
	@echo "    Tag: $(IMAGE_TAG)"
	@echo "    Commit: $(GIT_COMMIT_FULL)"
	@echo "    Branch: $(GIT_BRANCH)"
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$(GIT_COMMIT_FULL) \
		--build-arg GIT_BRANCH=$(GIT_BRANCH) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-f Dockerfile.oracle-price-worker \
		-t $(ECR_REPO_ORACLE_PRICE_WORKER):$(IMAGE_TAG) \
		-t $(ECR_REPO_ORACLE_PRICE_WORKER):latest \
		--load .
endif

# Push oracle price worker Docker image to ECR
docker-push-oracle-price-worker: ecr-login
	@echo "==> Pushing Oracle Price Worker Docker image to ECR..."
	docker push $(ECR_REPO_ORACLE_PRICE_WORKER):$(IMAGE_TAG)
	docker push $(ECR_REPO_ORACLE_PRICE_WORKER):latest

# Build and push oracle price worker in one step (captures tag at start to ensure consistency)
docker-release-oracle-price-worker: ecr-login _docker-release-oracle-price-worker-internal

_docker-release-oracle-price-worker-internal:
	@TAG=$(IMAGE_TAG) && \
	COMMIT=$$(git rev-parse HEAD) && \
	BRANCH=$$(git rev-parse --abbrev-ref HEAD) && \
	TIME=$$(date -u +"%Y-%m-%dT%H:%M:%SZ") && \
	echo "==> Building Oracle Price Worker Docker image for ARM64..." && \
	echo "    Repository: $(ECR_REPO_ORACLE_PRICE_WORKER)" && \
	echo "    Tag: $$TAG" && \
	echo "    Commit: $$COMMIT" && \
	echo "    Branch: $$BRANCH" && \
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$$COMMIT \
		--build-arg GIT_BRANCH=$$BRANCH \
		--build-arg BUILD_TIME=$$TIME \
		-f Dockerfile.oracle-price-worker \
		-t $(ECR_REPO_ORACLE_PRICE_WORKER):$$TAG \
		-t $(ECR_REPO_ORACLE_PRICE_WORKER):latest \
		--load . && \
	echo "==> Pushing Oracle Price Worker Docker image to ECR..." && \
	docker push $(ECR_REPO_ORACLE_PRICE_WORKER):$$TAG && \
	docker push $(ECR_REPO_ORACLE_PRICE_WORKER):latest && \
	echo "==> Oracle Price Worker Docker image released: $(ECR_REPO_ORACLE_PRICE_WORKER):$$TAG"

# =============================================================================
# Sentinelstaging Oracle Price Worker Docker Shortcuts
# =============================================================================

docker-build-oracle-price-worker-staging:
	@$(MAKE) docker-build-oracle-price-worker ENV=sentinelstaging

docker-push-oracle-price-worker-staging:
	@$(MAKE) docker-push-oracle-price-worker ENV=sentinelstaging

docker-release-oracle-price-worker-staging:
	@$(MAKE) docker-release-oracle-price-worker ENV=sentinelstaging

# Force new deployment of oracle price worker service (pulls latest image)
# Usage: make ecs-deploy-oracle-price-worker ENV=sentineldev|sentinelstaging|sentinelprod
ecs-deploy-oracle-price-worker:
	@echo "==> Forcing new deployment of oracle price worker service..."
	aws ecs update-service --cluster stl-$(ENV)-cluster --service stl-$(ENV)-oracle-price-worker --force-new-deployment --region $(AWS_REGION)
	@echo "==> Deployment initiated. Monitor with: aws ecs describe-services --cluster stl-$(ENV)-cluster --services stl-$(ENV)-oracle-price-worker --region $(AWS_REGION)"

ecs-deploy-oracle-price-worker-staging:
	@$(MAKE) ecs-deploy-oracle-price-worker ENV=sentinelstaging

# =============================================================================
# Docker Build & Push - SparkLend Position Tracker
# =============================================================================

# ECR repository URL for sparklend position tracker
ECR_REPO_SPARKLEND_POSITION_TRACKER = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/stl-$(ENV)-sparklend-position-tracker

# Build Docker image for sparklend position tracker (ARM64 for Fargate Graviton)
docker-build-sparklend-position-tracker:
ifdef LOCAL
	docker build -t stl-sparklend-position-tracker:local -f Dockerfile.sparklend-position-tracker .
else
	@echo "==> Building SparkLend Position Tracker Docker image for ARM64..."
	@echo "    Repository: $(ECR_REPO_SPARKLEND_POSITION_TRACKER)"
	@echo "    Tag: $(IMAGE_TAG)"
	@echo "    Commit: $(GIT_COMMIT_FULL)"
	@echo "    Branch: $(GIT_BRANCH)"
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$(GIT_COMMIT_FULL) \
		--build-arg GIT_BRANCH=$(GIT_BRANCH) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-f Dockerfile.sparklend-position-tracker \
		-t $(ECR_REPO_SPARKLEND_POSITION_TRACKER):$(IMAGE_TAG) \
		-t $(ECR_REPO_SPARKLEND_POSITION_TRACKER):latest \
		--load .
endif

# Push sparklend position tracker Docker image to ECR
docker-push-sparklend-position-tracker: ecr-login
	@echo "==> Pushing SparkLend Position Tracker Docker image to ECR..."
	docker push $(ECR_REPO_SPARKLEND_POSITION_TRACKER):$(IMAGE_TAG)
	docker push $(ECR_REPO_SPARKLEND_POSITION_TRACKER):latest

# Build and push sparklend position tracker in one step (captures tag at start to ensure consistency)
docker-release-sparklend-position-tracker: ecr-login _docker-release-sparklend-position-tracker-internal

_docker-release-sparklend-position-tracker-internal:
	@TAG=$(IMAGE_TAG) && \
	COMMIT=$$(git rev-parse HEAD) && \
	BRANCH=$$(git rev-parse --abbrev-ref HEAD) && \
	TIME=$$(date -u +"%Y-%m-%dT%H:%M:%SZ") && \
	echo "==> Building SparkLend Position Tracker Docker image for ARM64..." && \
	echo "    Repository: $(ECR_REPO_SPARKLEND_POSITION_TRACKER)" && \
	echo "    Tag: $$TAG" && \
	echo "    Commit: $$COMMIT" && \
	echo "    Branch: $$BRANCH" && \
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$$COMMIT \
		--build-arg GIT_BRANCH=$$BRANCH \
		--build-arg BUILD_TIME=$$TIME \
		-f Dockerfile.sparklend-position-tracker \
		-t $(ECR_REPO_SPARKLEND_POSITION_TRACKER):$$TAG \
		-t $(ECR_REPO_SPARKLEND_POSITION_TRACKER):latest \
		--load . && \
	echo "==> Pushing SparkLend Position Tracker Docker image to ECR..." && \
	docker push $(ECR_REPO_SPARKLEND_POSITION_TRACKER):$$TAG && \
	docker push $(ECR_REPO_SPARKLEND_POSITION_TRACKER):latest && \
	echo "==> SparkLend Position Tracker Docker image released: $(ECR_REPO_SPARKLEND_POSITION_TRACKER):$$TAG"

# =============================================================================
# Sentinelstaging SparkLend Position Tracker Docker Shortcuts
# =============================================================================

docker-build-sparklend-position-tracker-staging:
	@$(MAKE) docker-build-sparklend-position-tracker ENV=sentinelstaging

docker-push-sparklend-position-tracker-staging:
	@$(MAKE) docker-push-sparklend-position-tracker ENV=sentinelstaging

docker-release-sparklend-position-tracker-staging:
	@$(MAKE) docker-release-sparklend-position-tracker ENV=sentinelstaging


# =============================================================================
# Docker Build & Push - Morpho Indexer
# =============================================================================

# ECR repository URL for morpho indexer
ECR_REPO_MORPHO_INDEXER = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/stl-$(ENV)-morpho-indexer

# Build Docker image for morpho indexer (ARM64 for Fargate Graviton)
docker-build-morpho-indexer:
ifdef LOCAL
	docker build -t stl-morpho-indexer:local -f Dockerfile.morpho-indexer .
else
	@echo "==> Building Morpho Indexer Docker image for ARM64..."
	@echo "    Repository: $(ECR_REPO_MORPHO_INDEXER)"
	@echo "    Tag: $(IMAGE_TAG)"
	@echo "    Commit: $(GIT_COMMIT_FULL)"
	@echo "    Branch: $(GIT_BRANCH)"
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$(GIT_COMMIT_FULL) \
		--build-arg GIT_BRANCH=$(GIT_BRANCH) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-f Dockerfile.morpho-indexer \
		-t $(ECR_REPO_MORPHO_INDEXER):$(IMAGE_TAG) \
		-t $(ECR_REPO_MORPHO_INDEXER):latest \
		--load .
endif

# Push morpho indexer Docker image to ECR
docker-push-morpho-indexer: ecr-login
	@echo "==> Pushing Morpho Indexer Docker image to ECR..."
	docker push $(ECR_REPO_MORPHO_INDEXER):$(IMAGE_TAG)
	docker push $(ECR_REPO_MORPHO_INDEXER):latest

# Build and push morpho indexer in one step (captures tag at start to ensure consistency)
docker-release-morpho-indexer: ecr-login _docker-release-morpho-indexer-internal

_docker-release-morpho-indexer-internal:
	@TAG=$(IMAGE_TAG) && \
	COMMIT=$$(git rev-parse HEAD) && \
	BRANCH=$$(git rev-parse --abbrev-ref HEAD) && \
	TIME=$$(date -u +"%Y-%m-%dT%H:%M:%SZ") && \
	echo "==> Building Morpho Indexer Docker image for ARM64..." && \
	echo "    Repository: $(ECR_REPO_MORPHO_INDEXER)" && \
	echo "    Tag: $$TAG" && \
	echo "    Commit: $$COMMIT" && \
	echo "    Branch: $$BRANCH" && \
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$$COMMIT \
		--build-arg GIT_BRANCH=$$BRANCH \
		--build-arg BUILD_TIME=$$TIME \
		-f Dockerfile.morpho-indexer \
		-t $(ECR_REPO_MORPHO_INDEXER):$$TAG \
		-t $(ECR_REPO_MORPHO_INDEXER):latest \
		--load . && \
	echo "==> Pushing Morpho Indexer Docker image to ECR..." && \
	docker push $(ECR_REPO_MORPHO_INDEXER):$$TAG && \
	docker push $(ECR_REPO_MORPHO_INDEXER):latest && \
	echo "==> Morpho Indexer Docker image released: $(ECR_REPO_MORPHO_INDEXER):$$TAG"

# =============================================================================
# Sentinelstaging Morpho Indexer Docker Shortcuts
# =============================================================================

docker-build-morpho-indexer-staging:
	@$(MAKE) docker-build-morpho-indexer ENV=sentinelstaging

docker-push-morpho-indexer-staging:
	@$(MAKE) docker-push-morpho-indexer ENV=sentinelstaging

docker-release-morpho-indexer-staging:
	@$(MAKE) docker-release-morpho-indexer ENV=sentinelstaging

# =============================================================================
# Docker Build & Push - Prime Debt Indexer
# =============================================================================

ECR_REPO_PRIME_DEBT_INDEXER = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/stl-$(ENV)-prime-debt-indexer

# Build Docker image for prime debt indexer (ARM64 for EKS Graviton)
docker-build-prime-debt-indexer:
ifdef LOCAL
	docker build -t stl-prime-debt-indexer:local -f Dockerfile.prime-debt-indexer .
else
	@echo "==> Building Prime Debt Indexer Docker image for ARM64..."
	@echo "    Repository: $(ECR_REPO_PRIME_DEBT_INDEXER)"
	@echo "    Tag: $(IMAGE_TAG)"
	@echo "    Commit: $(GIT_COMMIT_FULL)"
	@echo "    Branch: $(GIT_BRANCH)"
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$(GIT_COMMIT_FULL) \
		--build-arg GIT_BRANCH=$(GIT_BRANCH) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-f Dockerfile.prime-debt-indexer \
		-t $(ECR_REPO_PRIME_DEBT_INDEXER):$(IMAGE_TAG) \
		-t $(ECR_REPO_PRIME_DEBT_INDEXER):latest \
		--load .
endif

# Push prime debt indexer Docker image to ECR
docker-push-prime-debt-indexer: ecr-login
	@echo "==> Pushing Prime Debt Indexer Docker image to ECR..."
	docker push $(ECR_REPO_PRIME_DEBT_INDEXER):$(IMAGE_TAG)
	docker push $(ECR_REPO_PRIME_DEBT_INDEXER):latest

# Build and push prime debt indexer in one step (captures tag at start to ensure consistency)
docker-release-prime-debt-indexer: ecr-login _docker-release-prime-debt-indexer-internal

_docker-release-prime-debt-indexer-internal:
	@TAG=$(IMAGE_TAG) && \
	COMMIT=$$(git rev-parse HEAD) && \
	BRANCH=$$(git rev-parse --abbrev-ref HEAD) && \
	TIME=$$(date -u +"%Y-%m-%dT%H:%M:%SZ") && \
	echo "==> Building Prime Debt Indexer Docker image for ARM64..." && \
	echo "    Repository: $(ECR_REPO_PRIME_DEBT_INDEXER)" && \
	echo "    Tag: $$TAG" && \
	echo "    Commit: $$COMMIT" && \
	echo "    Branch: $$BRANCH" && \
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$$COMMIT \
		--build-arg GIT_BRANCH=$$BRANCH \
		--build-arg BUILD_TIME=$$TIME \
		-f Dockerfile.prime-debt-indexer \
		-t $(ECR_REPO_PRIME_DEBT_INDEXER):$$TAG \
		-t $(ECR_REPO_PRIME_DEBT_INDEXER):latest \
		--load . && \
	echo "==> Pushing Prime Debt Indexer Docker image to ECR..." && \
	docker push $(ECR_REPO_PRIME_DEBT_INDEXER):$$TAG && \
	docker push $(ECR_REPO_PRIME_DEBT_INDEXER):latest && \
	echo "==> Prime Debt Indexer Docker image released: $(ECR_REPO_PRIME_DEBT_INDEXER):$$TAG"

docker-build-prime-debt-indexer-staging:
	@$(MAKE) docker-build-prime-debt-indexer ENV=sentinelstaging

docker-push-prime-debt-indexer-staging:
	@$(MAKE) docker-push-prime-debt-indexer ENV=sentinelstaging

docker-release-prime-debt-indexer-staging:
	@$(MAKE) docker-release-prime-debt-indexer ENV=sentinelstaging

# =============================================================================
# Docker Build & Push - Allocation Tracker
# =============================================================================

# ECR repository URL for allocation tracker
ECR_REPO_ALLOCATION_TRACKER = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/stl-$(ENV)-allocation-tracker

# Build Docker image for allocation tracker (ARM64 for Fargate Graviton)
docker-build-allocation-tracker:
	@echo "==> Building Allocation Tracker Docker image for ARM64..."
	@echo "    Repository: $(ECR_REPO_ALLOCATION_TRACKER)"
	@echo "    Tag: $(IMAGE_TAG)"
	@echo "    Commit: $(GIT_COMMIT_FULL)"
	@echo "    Branch: $(GIT_BRANCH)"
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$(GIT_COMMIT_FULL) \
		--build-arg GIT_BRANCH=$(GIT_BRANCH) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-f Dockerfile.allocation-tracker \
		-t $(ECR_REPO_ALLOCATION_TRACKER):$(IMAGE_TAG) \
		-t $(ECR_REPO_ALLOCATION_TRACKER):latest \
		--load .

# Push allocation tracker Docker image to ECR
docker-push-allocation-tracker: ecr-login
	@echo "==> Pushing Allocation Tracker Docker image to ECR..."
	docker push $(ECR_REPO_ALLOCATION_TRACKER):$(IMAGE_TAG)
	docker push $(ECR_REPO_ALLOCATION_TRACKER):latest

# Build and push allocation tracker in one step (captures tag at start to ensure consistency)
docker-release-allocation-tracker: ecr-login _docker-release-allocation-tracker-internal

_docker-release-allocation-tracker-internal:
	@TAG=$(IMAGE_TAG) && \
	COMMIT=$$(git rev-parse HEAD) && \
	BRANCH=$$(git rev-parse --abbrev-ref HEAD) && \
	TIME=$$(date -u +"%Y-%m-%dT%H:%M:%SZ") && \
	echo "==> Building Allocation Tracker Docker image for ARM64..." && \
	echo "    Repository: $(ECR_REPO_ALLOCATION_TRACKER)" && \
	echo "    Tag: $$TAG" && \
	echo "    Commit: $$COMMIT" && \
	echo "    Branch: $$BRANCH" && \
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$$COMMIT \
		--build-arg GIT_BRANCH=$$BRANCH \
		--build-arg BUILD_TIME=$$TIME \
		-f Dockerfile.allocation-tracker \
		-t $(ECR_REPO_ALLOCATION_TRACKER):$$TAG \
		-t $(ECR_REPO_ALLOCATION_TRACKER):latest \
		--load . && \
	echo "==> Pushing Allocation Tracker Docker image to ECR..." && \
	docker push $(ECR_REPO_ALLOCATION_TRACKER):$$TAG && \
	docker push $(ECR_REPO_ALLOCATION_TRACKER):latest && \
	echo "==> Allocation Tracker Docker image released: $(ECR_REPO_ALLOCATION_TRACKER):$$TAG"

# =============================================================================
# Sentinelstaging Allocation Tracker Docker Shortcuts
# =============================================================================

docker-build-allocation-tracker-staging:
	@$(MAKE) docker-build-allocation-tracker ENV=sentinelstaging

docker-push-allocation-tracker-staging:
	@$(MAKE) docker-push-allocation-tracker ENV=sentinelstaging

docker-release-allocation-tracker-staging:
	@$(MAKE) docker-release-allocation-tracker ENV=sentinelstaging

# =============================================================================
# Docker Build & Push - Migrate
# =============================================================================

# ECR repository URL for migrate
ECR_REPO_MIGRATE = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/stl-$(ENV)-migrate

# Build and push migrate in one step (captures tag at start to ensure consistency)
docker-release-migrate: ecr-login _docker-release-migrate-internal

_docker-release-migrate-internal:
	@TAG=$(IMAGE_TAG) && \
	echo "==> Building Migrate Docker image for ARM64..." && \
	echo "    Repository: $(ECR_REPO_MIGRATE)" && \
	echo "    Tag: $$TAG" && \
	docker buildx build --platform linux/arm64 \
		-f Dockerfile.migrate \
		-t $(ECR_REPO_MIGRATE):$$TAG \
		-t $(ECR_REPO_MIGRATE):latest \
		--load . && \
	echo "==> Pushing Migrate Docker image to ECR..." && \
	docker push $(ECR_REPO_MIGRATE):$$TAG && \
	docker push $(ECR_REPO_MIGRATE):latest && \
	echo "==> Migrate Docker image released: $(ECR_REPO_MIGRATE):$$TAG"

# Sentinelstaging Migrate Docker Shortcuts
docker-release-migrate-staging:
	@$(MAKE) docker-release-migrate ENV=sentinelstaging

# =============================================================================
# Erigon Node Management (via Tailscale)
# =============================================================================

# WORKER_USER and WORKER_IP are required for worker deploy targets
# Usage: make deploy-oracle-pricing-backfill-worker WORKER_USER=root WORKER_IP=<tailscale-ip>
define require_worker_vars
	@if [ -z "$(WORKER_USER)" ] || [ -z "$(WORKER_IP)" ]; then \
		echo "Error: WORKER_USER and WORKER_IP are required"; \
		echo "Usage: make $@ WORKER_USER=<user> WORKER_IP=<tailscale-ip>"; \
		exit 1; \
	fi
endef

# ERIGON_USER and ERIGON_IP are required for all erigon-* targets
# Usage: make erigon-status ERIGON_USER=<user> ERIGON_IP=<tailscale-ip>

# Helper to check required vars are set
define require_erigon_vars
	@if [ -z "$(ERIGON_USER)" ] || [ -z "$(ERIGON_IP)" ]; then \
		echo "Error: ERIGON_USER and ERIGON_IP are required"; \
		echo "Usage: make $@ ERIGON_USER=<user> ERIGON_IP=<tailscale-ip>"; \
		exit 1; \
	fi
endef

# Check Erigon sync status
erigon-status:
	$(call require_erigon_vars)
	@echo "==> Checking Erigon status..."
	tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) 'systemctl status erigon --no-pager; echo "---"; df -h /data'

# View Erigon logs (last 100 lines)
erigon-logs:
	$(call require_erigon_vars)
	@echo "==> Viewing Erigon logs..."
	tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) 'journalctl -u erigon -n 100 --no-pager'

# Follow Erigon logs (streaming)
erigon-logs-follow:
	$(call require_erigon_vars)
	@echo "==> Following Erigon logs..."
	tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) 'journalctl -u erigon -f'

# Deploy erigon-readonly.service to node
erigon-deploy-readonly:
	$(call require_erigon_vars)
	@echo "==> Deploying erigon-readonly.service to Erigon node..."
	cat erigon/erigon-readonly.service | tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) 'cat > /etc/systemd/system/erigon-readonly.service && systemctl daemon-reload'
	@echo "==> Service deployed. Use 'make erigon-switch-readonly' to switch modes."

# Switch to read-only mode (for bulk downloading)
erigon-switch-readonly:
	$(call require_erigon_vars)
	@echo "==> Switching Erigon to read-only mode..."
	@echo "    WARNING: This will stop syncing and optimize for bulk RPC queries."
	tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) 'systemctl stop erigon && systemctl start erigon-readonly'
	@echo "==> Erigon is now in read-only mode. Use 'make erigon-switch-sync' to resume syncing."

# Switch back to sync mode (normal operation)
erigon-switch-sync:
	$(call require_erigon_vars)
	@echo "==> Switching Erigon back to sync mode..."
	tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) 'systemctl stop erigon-readonly && systemctl start erigon'
	@echo "==> Erigon is now syncing."

# Check which mode Erigon is in
erigon-mode:
	$(call require_erigon_vars)
	@echo "==> Checking Erigon mode..."
	@tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) '\
		if systemctl is-active --quiet erigon-readonly; then \
			echo "Mode: READ-ONLY (optimized for bulk queries)"; \
		elif systemctl is-active --quiet erigon; then \
			echo "Mode: SYNC (normal operation)"; \
		else \
			echo "Mode: STOPPED (neither service running)"; \
		fi'

# Run a command on Erigon node
# Usage: make erigon-run ERIGON_USER=<user> ERIGON_IP=<ip> CMD="df -h"
erigon-run:
	$(call require_erigon_vars)
	@tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) '$(CMD)'

# Build oracle-price-worker binary
build-oracle-worker:
	@echo "==> Building oracle-price-worker..."
	mkdir -p dist
	go build -o dist/oracle-price-worker ./cmd/workers/oracle-price-indexer
	@echo "==> Built: dist/oracle-price-worker"

# Build sparklend-position-tracker binary
build-sparklend-position-tracker:
	@echo "==> Building sparklend-position-tracker..."
	mkdir -p dist
	go build -o dist/sparklend-position-tracker ./cmd/workers/sparklend-indexer
	@echo "==> Built: dist/sparklend-position-tracker"

# Build allocation-tracker binary
build-allocation-tracker:
	@echo "==> Building allocation-tracker..."
	mkdir -p dist
	go build -o dist/allocation-tracker ./cmd/workers/prime-allocation-indexer
	@echo "==> Built: dist/allocation-tracker"

# Pattern rules for cronjob binaries — work for any cronjob under cmd/cronjobs/.
build-cronjob-%:
	@echo "==> Building $*..."
	mkdir -p dist
	go build -o dist/$* ./cmd/cronjobs/$*
	@echo "==> Built: dist/$*"

run-cronjob-%:
	@echo "==> Running $*..."
	@set -a && . cmd/cronjobs/$*/.env && [ -f cmd/cronjobs/$*/.env.local ] && . cmd/cronjobs/$*/.env.local; set +a && go run ./cmd/cronjobs/$*

# Build oracle-pricing-backfill binary for ARM64 Linux (to run on Erigon node)
build-oracle-pricing-backfill-arm64:
	@echo "==> Building oracle-pricing-backfill for ARM64 Linux..."
	mkdir -p dist
	GOOS=linux GOARCH=arm64 go build -o dist/oracle-pricing-backfill-linux-arm64 ./cmd/oracle-pricing-backfill
	@echo "==> Built: dist/oracle-pricing-backfill-linux-arm64"

# Deploy oracle-pricing-backfill to Erigon node
# Usage: make deploy-oracle-pricing-backfill ERIGON_USER=<user> ERIGON_IP=<ip>
deploy-oracle-pricing-backfill: build-oracle-pricing-backfill-arm64
	$(call require_erigon_vars)
	@echo "==> Deploying oracle-pricing-backfill to Erigon node..."
	cat dist/oracle-pricing-backfill-linux-arm64 | tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) 'cat > ~/oracle-pricing-backfill && chmod +x ~/oracle-pricing-backfill'
	@echo "==> Deployed to ~/oracle-pricing-backfill on Erigon node."

# Deploy oracle-pricing-backfill to worker instance
# Usage: make deploy-oracle-pricing-backfill-worker WORKER_USER=root WORKER_IP=<tailscale-ip>
deploy-oracle-pricing-backfill-worker: build-oracle-pricing-backfill-arm64
	$(call require_worker_vars)
	@echo "==> Deploying oracle-pricing-backfill to worker..."
	cat dist/oracle-pricing-backfill-linux-arm64 | tailscale ssh $(WORKER_USER)@$(WORKER_IP) 'cat > ~/oracle-pricing-backfill && chmod +x ~/oracle-pricing-backfill'
	@echo "==> Deployed to ~/oracle-pricing-backfill on worker."

# Build bulk-download binary for ARM64 Linux (to run on Erigon node)
build-bulk-download-arm64:
	@echo "==> Building bulk-download for ARM64 Linux..."
	mkdir -p dist
	GOOS=linux GOARCH=arm64 go build -o dist/bulk-download-linux-arm64 ./cmd/bulk-download
	@echo "==> Built: dist/bulk-download-linux-arm64"

# Deploy bulk-download to Erigon node
# Usage: make deploy-bulk-download ERIGON_USER=<user> ERIGON_IP=<ip>
deploy-bulk-download: build-bulk-download-arm64
	$(call require_erigon_vars)
	@echo "==> Deploying bulk-download to Erigon node..."
	cat dist/bulk-download-linux-arm64 | tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) 'cat > ~/bulk-download && chmod +x ~/bulk-download'
	@echo "==> Deployed to ~/bulk-download on Erigon node."

# Run bulk-download on Erigon node (dry-run by default)
# Usage: make erigon-bulk-download ERIGON_USER=<user> ERIGON_IP=<ip> BUCKET=<bucket> START=16000000 END=16001000 [DRY_RUN=false]
DRY_RUN ?= true
erigon-bulk-download:
	$(call require_erigon_vars)
	@if [ -z "$(BUCKET)" ]; then \
		echo "Error: BUCKET is required"; \
		echo "Usage: make $@ ERIGON_USER=<user> ERIGON_IP=<ip> BUCKET=<bucket> START=<start> END=<end>"; \
		exit 1; \
	fi
	@echo "==> Running bulk-download on Erigon node..."
	tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) '~/bulk-download \
		--start-block=$(START) \
		--end-block=$(END) \
		--bucket=$(BUCKET) \
		$(if $(filter true,$(DRY_RUN)),--dry-run,)'

# Build sparklend-backfill binary for ARM64 Linux (to run on Erigon node)
build-sparklend-backfill-arm64:
	@echo "==> Building sparklend-backfill for ARM64 Linux..."
	mkdir -p dist
	GOOS=linux GOARCH=arm64 go build -o dist/sparklend-backfill-linux-arm64 ./cmd/sparklend-backfill
	@echo "==> Built: dist/sparklend-backfill-linux-arm64"

# Deploy sparklend-backfill to Erigon node
# Usage: make deploy-sparklend-backfill ERIGON_USER=<user> ERIGON_IP=<ip>
deploy-sparklend-backfill: build-sparklend-backfill-arm64
	$(call require_erigon_vars)
	@echo "==> Deploying sparklend-backfill to Erigon node..."
	cat dist/sparklend-backfill-linux-arm64 | tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) 'cat > ~/sparklend-backfill && chmod +x ~/sparklend-backfill'
	@echo "==> Deployed to ~/sparklend-backfill on Erigon node."

# Run sparklend-backfill on Erigon node
# Usage: make erigon-sparklend-backfill ERIGON_USER=<user> ERIGON_IP=<ip> BUCKET=<bucket> START=<start> END=<end> DB_URL=<postgres-url> [RPC_URL=http://127.0.0.1:8545] [CONCURRENCY=10] [CHAIN_ID=1] [BACKFILL_AWS_REGION=us-east-1]
RPC_URL ?= http://127.0.0.1:8545
CONCURRENCY ?= 10
CHAIN_ID ?= 1
BACKFILL_AWS_REGION ?= us-east-1
erigon-sparklend-backfill:
	$(call require_erigon_vars)
	@if [ -z "$(BUCKET)" ] || [ -z "$(START)" ] || [ -z "$(END)" ] || [ -z "$(DB_URL)" ]; then \
		echo "Error: BUCKET, START, END, and DB_URL are required"; \
		echo "Usage: make $@ ERIGON_USER=<user> ERIGON_IP=<ip> BUCKET=<bucket> START=<start> END=<end> DB_URL=<postgres-url>"; \
		exit 1; \
	fi
	@echo "==> Running sparklend-backfill on Erigon node..."
	tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) '~/sparklend-backfill \
		--rpc-url=$(RPC_URL) \
		--from=$(START) \
		--to=$(END) \
		--bucket=$(BUCKET) \
		--db=$(DB_URL) \
		--concurrency=$(CONCURRENCY) \
		--chain-id=$(CHAIN_ID) \
		--aws-region=$(BACKFILL_AWS_REGION)'

# =============================================================================
# Local kind Cluster
# =============================================================================

kind-create:
	@mkdir -p $$HOME/.$(KIND_CLUSTER)/timescaledb $$HOME/.$(KIND_CLUSTER)/temporal-db
	@STALE=$$(kind get clusters 2>/dev/null | grep -v '^$(KIND_CLUSTER)$$' || true); \
	if [ -n "$$STALE" ]; then \
	  echo ""; \
	  echo "⚠️  Found unexpected kind cluster(s): $$STALE"; \
	  echo "   These may hold ports needed by this cluster. Delete them with:"; \
	  for c in $$STALE; do echo "   kind delete cluster --name $$c"; done; \
	  echo ""; \
	fi
	kind get clusters | grep -qx $(KIND_CLUSTER) || \
	  sed "s|STL_DATA_DIR|$$HOME/.$(KIND_CLUSTER)|g" k8s/kind.yaml | kind create cluster --config -

kind-delete:
	kind get clusters | grep -qx $(KIND_CLUSTER) && kind delete cluster --name $(KIND_CLUSTER) || true

kind-infra:
	kubectl --context=$(KUBE_CONTEXT) create namespace $(KUBE_NS) --dry-run=client -o yaml | kubectl --context=$(KUBE_CONTEXT) apply -f -
	# Install CNPG operator (required before any Cluster CR can be applied).
	kubectl --context=$(KUBE_CONTEXT) apply --server-side --force-conflicts -f https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.25/releases/cnpg-1.25.0.yaml
	@echo "==> Waiting for CNPG operator to be ready..."
	kubectl --context=$(KUBE_CONTEXT) rollout status deployment/cnpg-controller-manager -n cnpg-system --timeout=120s
	# Apply temporal resources to the temporal namespace (namespace declared in each manifest).
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/infra/temporal-00-namespace.yaml
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/infra/temporal-db.yaml
	@echo "==> Waiting for temporal-db CNPG cluster to be ready..."
	kubectl --context=$(KUBE_CONTEXT) wait cluster/temporal-db -n temporal --for=condition=Ready --timeout=120s
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/infra/temporal.yaml
	# Apply remaining infra resources to the app namespace.
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/infra/jaeger.yaml       -n $(KUBE_NS)
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/infra/localstack.yaml   -n $(KUBE_NS)
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/infra/redis.yaml        -n $(KUBE_NS)
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/infra/timescaledb.yaml  -n $(KUBE_NS)
	@echo "==> Waiting for timescaledb to be ready (may take a moment on first run — image pull)..."
	kubectl --context=$(KUBE_CONTEXT) rollout status statefulset/timescaledb -n $(KUBE_NS) --timeout=120s
	@echo "==> Waiting for localstack to be ready..."
	kubectl --context=$(KUBE_CONTEXT) rollout status deployment/localstack   -n $(KUBE_NS) --timeout=120s

kind-config:
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/config/configmap.yaml -n $(KUBE_NS)

kind-secrets:
	@test -f ../.env.secrets || { printf 'ALCHEMY_API_KEY=\nCOINGECKO_API_KEY=\nETHERSCAN_API_KEY=\nANCHORAGE_API_KEY=\n' > ../.env.secrets && chmod 600 ../.env.secrets; }
	@set -a; . ../.env.secrets; set +a; \
	kubectl --context=$(KUBE_CONTEXT) create secret generic stl-secrets \
	  --from-literal=ALCHEMY_API_KEY=$${ALCHEMY_API_KEY:-mock} \
	  --from-literal=COINGECKO_API_KEY=$${COINGECKO_API_KEY:-} \
	  --from-literal=ETHERSCAN_API_KEY=$${ETHERSCAN_API_KEY:-} \
	  --from-literal=ANCHORAGE_API_KEY=$${ANCHORAGE_API_KEY:-} \
	  --from-literal=AWS_ACCESS_KEY_ID=test \
	  --from-literal=AWS_SECRET_ACCESS_KEY=test \
	  --namespace=$(KUBE_NS) \
	  --dry-run=client -o yaml | kubectl --context=$(KUBE_CONTEXT) apply -f -

docker-build-migrate:
	docker build -t stl-migrate:local -f Dockerfile.migrate .

kind-load-images:
	@test -n "$(IMAGES)" || { echo "ERROR: IMAGES is required."; exit 1; }
	@for img in $(IMAGES); do \
		canonical="localhost/$$img"; \
		docker image inspect "$$img" >/dev/null 2>&1 || { echo "ERROR: local image $$img not found."; exit 1; }; \
		echo "==> Tagging $$img as $$canonical"; \
		docker tag "$$img" "$$canonical"; \
		if [ "$(KIND_EXPERIMENTAL_PROVIDER)" = "podman" ]; then \
			echo "==> Loading $$canonical into kind cluster via podman+ctr..."; \
			docker save "$$canonical" | podman exec -i $(KIND_CLUSTER)-control-plane ctr --namespace=k8s.io images import -; \
		else \
			echo "==> Loading $$canonical into kind cluster via kind load..."; \
			kind load docker-image "$$canonical" --name $(KIND_CLUSTER); \
		fi; \
	done

kind-load-migrate:
	@$(MAKE) --no-print-directory kind-load-images IMAGES="stl-migrate:local"

kind-bootstrap-db:
	kubectl --context=$(KUBE_CONTEXT) delete job/bootstrap-db --ignore-not-found -n $(KUBE_NS)
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/jobs/bootstrap-db.yaml -n $(KUBE_NS)
	@echo "==> Waiting for database bootstrap to complete..."
	kubectl --context=$(KUBE_CONTEXT) wait --for=condition=complete job/bootstrap-db -n $(KUBE_NS) --timeout=120s
	@echo "==> Bootstrap logs:"
	kubectl --context=$(KUBE_CONTEXT) logs -l job-name=bootstrap-db -n $(KUBE_NS) || true

kind-migrate: kind-bootstrap-db
	kubectl --context=$(KUBE_CONTEXT) delete job/migrate --ignore-not-found -n $(KUBE_NS)
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/jobs/migrate.yaml -n $(KUBE_NS)
	@echo "==> Waiting for migration to complete..."
	kubectl --context=$(KUBE_CONTEXT) wait --for=condition=complete job/migrate -n $(KUBE_NS) --timeout=120s
	@echo "==> Migration logs:"
	kubectl --context=$(KUBE_CONTEXT) logs -l job-name=migrate -n $(KUBE_NS) || true

kind-load-watcher:
	@$(MAKE) --no-print-directory kind-load-images IMAGES="stl-watcher:local"

kind-load-workers:
	@$(MAKE) --no-print-directory kind-load-images IMAGES="stl-oracle-price-worker:local stl-morpho-indexer:local stl-sparklend-position-tracker:local stl-prime-debt-indexer:local"

kind-deploy-watcher:
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/apps/watcher.yaml -n $(KUBE_NS)
	kubectl --context=$(KUBE_CONTEXT) rollout restart deployment/watcher -n $(KUBE_NS)

kind-deploy-workers:
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/apps/workers/ -n $(KUBE_NS)
	kubectl --context=$(KUBE_CONTEXT) rollout restart deployment/morpho-indexer deployment/oracle-price-worker deployment/avalanche-oracle-price-worker deployment/sparklend-position-tracker deployment/prime-debt-indexer -n $(KUBE_NS)

kind-load-python-api:
	@$(MAKE) --no-print-directory kind-load-images IMAGES="stl-python-api:local"

kind-deploy-python-api:
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/apps/python-api.yaml -n $(KUBE_NS)
	kubectl --context=$(KUBE_CONTEXT) rollout restart deployment/python-api -n $(KUBE_NS)

# --- Cronjobs (Temporal workers) ---
# Auto-discovered from cmd/cronjobs/*/main.go — no changes needed when adding a new cronjob.

CRONJOBS := $(sort $(notdir $(patsubst %/main.go,%,$(wildcard cmd/cronjobs/*/main.go))))
CRONJOB_DOCKERFILE = cmd/cronjobs/Dockerfile

# Pattern rule: docker-build-cronjob-<name> builds any cronjob image.
docker-build-cronjob-%:
	docker build --build-arg CRONJOB=$* -t stl-$*:local -f $(CRONJOB_DOCKERFILE) .

# Pattern rule: release a cronjob image to ECR (build + push).
# All cronjobs share a single ECR repo; the cronjob name is part of the tag.
# Usage: make docker-release-cronjob-anchorage-indexer ENV=sentinelstaging
docker-release-cronjob-%: ecr-login _docker-release-cronjob-%-internal

_docker-release-cronjob-%-internal:
	@TAG=$(IMAGE_TAG) && \
	REPO=$(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/stl-$(ENV)-cronjob && \
	COMMIT=$$(git rev-parse HEAD) && \
	BRANCH=$$(git rev-parse --abbrev-ref HEAD) && \
	TIME=$$(date -u +"%Y-%m-%dT%H:%M:%SZ") && \
	echo "==> Building Cronjob $* Docker image for ARM64..." && \
	echo "    Repository: $$REPO" && \
	echo "    Tag: $*-$$TAG" && \
	echo "    Commit: $$COMMIT" && \
	echo "    Branch: $$BRANCH" && \
	docker buildx build --platform linux/arm64 \
		--build-arg CRONJOB=$* \
		--build-arg GIT_COMMIT=$$COMMIT \
		--build-arg GIT_BRANCH=$$BRANCH \
		--build-arg BUILD_TIME=$$TIME \
		-f $(CRONJOB_DOCKERFILE) \
		-t $$REPO:$*-$$TAG \
		-t $$REPO:$*-latest \
		--load . && \
	echo "==> Pushing Cronjob $* Docker image to ECR..." && \
	docker push $$REPO:$*-$$TAG && \
	docker push $$REPO:$*-latest && \
	echo "==> Cronjob $* Docker image released: $$REPO:$*-$$TAG"

kind-load-cronjobs:
	@$(MAKE) --no-print-directory kind-load-images IMAGES="$(foreach cj,$(CRONJOBS),stl-$(cj):local)"

# Regenerate k8s manifests from cmd/cronjobs/ and apply them.
kind-deploy-cronjobs:
	go run ./cmd/util/cronjob-manifest
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/apps/cronjobs/ -n $(KUBE_NS)
	@kubectl --context=$(KUBE_CONTEXT) rollout restart $(addprefix deployment/,$(CRONJOBS)) -n $(KUBE_NS)

# Regenerate cronjob manifests without deploying (for committing to source control).
generate-cronjob-manifests:
	go run ./cmd/util/cronjob-manifest

# Rebuild and redeploy a specific worker or cronjob.
# Usage: make kind-redeploy-worker NAME=morpho-indexer
#        make kind-redeploy-worker NAME=anchorage-indexer
kind-redeploy-worker:
	@test -n "$(NAME)" || { echo "ERROR: NAME is required. Example: make kind-redeploy-worker NAME=morpho-indexer"; exit 1; }
	@if echo " $(CRONJOBS) " | grep -q " $(NAME) "; then \
	  $(MAKE) docker-build-cronjob-$(NAME); \
	else \
	  $(MAKE) docker-build-$(NAME) LOCAL=1; \
	fi
	@$(MAKE) --no-print-directory kind-load-images IMAGES="stl-$(NAME):local"
	kubectl --context=$(KUBE_CONTEXT) rollout restart deployment/$(NAME) -n $(KUBE_NS)

kind-redeploy-watcher:
	$(MAKE) docker-build-watcher LOCAL=1
	@$(MAKE) --no-print-directory kind-load-images IMAGES="stl-watcher:local"
	kubectl --context=$(KUBE_CONTEXT) rollout restart deployment/watcher -n $(KUBE_NS)

# CI targets — infra only, no apps, no secrets needed
# Migrations run via go run (no Docker build required)
kind-infra-ci: kind-create
	kubectl --context=$(KUBE_CONTEXT) create namespace $(KUBE_NS) --dry-run=client -o yaml | kubectl --context=$(KUBE_CONTEXT) apply -f -
	$(MAKE) kind-config kind-infra
	@echo "==> Running migrations..."
	DATABASE_URL=postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable \
	  go run ./cmd/migrate/...
	@echo "==> CI infra ready"

kind-ci-up: kind-infra-ci

kind-ci-down: kind-delete

# Delete all resources in the local namespace without tearing down the cluster.
# Useful for a clean redeploy: make kind-clean && make dev-up
kind-clean:
	@echo "$(KUBE_CONTEXT)" | grep -q '^kind-' || { echo "ERROR: kind-clean refused — '$(KUBE_CONTEXT)' is not a local kind context. Only kind-* contexts are allowed."; exit 1; }
	kubectl --context=$(KUBE_CONTEXT) delete namespace $(KUBE_NS) --ignore-not-found --wait
	kubectl --context=$(KUBE_CONTEXT) delete namespace temporal      --ignore-not-found --wait
	@echo "==> Namespaces $(KUBE_NS) and temporal deleted."

# =============================================================================
# Docker Build & Push - Python API
# =============================================================================

ECR_REPO_PYTHON_API = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/stl-$(ENV)-python-api

# Build Docker image for python-api (ARM64 for Graviton)
docker-build-python-api:
ifdef LOCAL
	docker build \
		--secret id=uikit_token,env=UIKIT_GH_PACKAGES_READ_TOKEN \
		--build-arg GIT_COMMIT=$(GIT_COMMIT_FULL) \
		-t stl-python-api:local -f python/Dockerfile .
else
	@echo "==> Building Python API Docker image for ARM64..."
	@echo "    Repository: $(ECR_REPO_PYTHON_API)"
	@echo "    Tag: $(IMAGE_TAG)"
	@echo "    Commit: $(GIT_COMMIT_FULL)"
	docker buildx build --platform linux/arm64 \
		--secret id=uikit_token,env=UIKIT_GH_PACKAGES_READ_TOKEN \
		--build-arg GIT_COMMIT=$(GIT_COMMIT_FULL) \
		-f python/Dockerfile \
		-t $(ECR_REPO_PYTHON_API):$(IMAGE_TAG) \
		-t $(ECR_REPO_PYTHON_API):latest \
		--load .
endif

# Push python-api Docker image to ECR
docker-push-python-api: ecr-login
	@echo "==> Pushing Python API Docker image to ECR..."
	docker push $(ECR_REPO_PYTHON_API):$(IMAGE_TAG)
	docker push $(ECR_REPO_PYTHON_API):latest

# Build and push python-api in one step (captures tag at start for consistency)
docker-release-python-api: ecr-login _docker-release-python-api-internal

_docker-release-python-api-internal:
	@TAG=$(IMAGE_TAG) && \
	COMMIT=$$(git rev-parse HEAD) && \
	echo "==> Building Python API Docker image for ARM64..." && \
	echo "    Repository: $(ECR_REPO_PYTHON_API)" && \
	echo "    Tag: $$TAG" && \
	echo "    Commit: $$COMMIT" && \
	docker buildx build --platform linux/arm64 \
		--secret id=uikit_token,env=UIKIT_GH_PACKAGES_READ_TOKEN \
		--build-arg GIT_COMMIT=$$COMMIT \
		-f python/Dockerfile \
		-t $(ECR_REPO_PYTHON_API):$$TAG \
		-t $(ECR_REPO_PYTHON_API):latest \
		--load . && \
	echo "==> Pushing Python API Docker image to ECR..." && \
	docker push $(ECR_REPO_PYTHON_API):$$TAG && \
	docker push $(ECR_REPO_PYTHON_API):latest && \
	echo "==> Python API Docker image released: $(ECR_REPO_PYTHON_API):$$TAG"

docker-release-python-api-staging:
	@$(MAKE) docker-release-python-api ENV=sentinelstaging

# =============================================================================
# Docker Build & Push - Mock Blockchain Server
# =============================================================================

ECR_REPO_MOCK_BLOCKCHAIN_SERVER = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/stl-$(ENV)-mock-blockchain-server

# Build Docker image for mock-blockchain-server (ARM64 for Fargate Graviton)
docker-build-mock-blockchain-server:
	@echo "==> Building Mock Blockchain Server Docker image for ARM64..."
	@echo "    Repository: $(ECR_REPO_MOCK_BLOCKCHAIN_SERVER)"
	@echo "    Tag: $(IMAGE_TAG)"
	@echo "    Commit: $(GIT_COMMIT_FULL)"
	@echo "    Branch: $(GIT_BRANCH)"
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$(GIT_COMMIT_FULL) \
		--build-arg GIT_BRANCH=$(GIT_BRANCH) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-f Dockerfile.mock-blockchain-server \
		-t $(ECR_REPO_MOCK_BLOCKCHAIN_SERVER):$(IMAGE_TAG) \
		-t $(ECR_REPO_MOCK_BLOCKCHAIN_SERVER):latest \
		--load .

# Push mock-blockchain-server Docker image to ECR
docker-push-mock-blockchain-server: ecr-login
	@echo "==> Pushing Mock Blockchain Server Docker image to ECR..."
	docker push $(ECR_REPO_MOCK_BLOCKCHAIN_SERVER):$(IMAGE_TAG)
	docker push $(ECR_REPO_MOCK_BLOCKCHAIN_SERVER):latest

# Build and push mock-blockchain-server in one step (captures tag at start for consistency)
docker-release-mock-blockchain-server: ecr-login _docker-release-mock-blockchain-server-internal

_docker-release-mock-blockchain-server-internal:
	@TAG=$(IMAGE_TAG) && \
	COMMIT=$$(git rev-parse HEAD) && \
	BRANCH=$$(git rev-parse --abbrev-ref HEAD) && \
	TIME=$$(date -u +"%Y-%m-%dT%H:%M:%SZ") && \
	echo "==> Building Mock Blockchain Server Docker image for ARM64..." && \
	echo "    Repository: $(ECR_REPO_MOCK_BLOCKCHAIN_SERVER)" && \
	echo "    Tag: $$TAG" && \
	echo "    Commit: $$COMMIT" && \
	echo "    Branch: $$BRANCH" && \
	docker buildx build --platform linux/arm64 \
		--build-arg GIT_COMMIT=$$COMMIT \
		--build-arg GIT_BRANCH=$$BRANCH \
		--build-arg BUILD_TIME=$$TIME \
		-f Dockerfile.mock-blockchain-server \
		-t $(ECR_REPO_MOCK_BLOCKCHAIN_SERVER):$$TAG \
		-t $(ECR_REPO_MOCK_BLOCKCHAIN_SERVER):latest \
		--load . && \
	echo "==> Pushing Mock Blockchain Server Docker image to ECR..." && \
	docker push $(ECR_REPO_MOCK_BLOCKCHAIN_SERVER):$$TAG && \
	docker push $(ECR_REPO_MOCK_BLOCKCHAIN_SERVER):latest && \
	echo "==> Mock Blockchain Server Docker image released: $(ECR_REPO_MOCK_BLOCKCHAIN_SERVER):$$TAG"

docker-release-mock-blockchain-server-staging:
	@$(MAKE) docker-release-mock-blockchain-server ENV=sentinelstaging

# =============================================================================
# Kind (local k8s) - Mock Blockchain Server
# =============================================================================

# Build local Docker image for kind
kind-build-mock-blockchain-server:
	docker build -f Dockerfile.mock-blockchain-server -t stl-mock-blockchain-server:local .

# Build and load image into kind cluster
kind-load-mock-blockchain-server: kind-build-mock-blockchain-server
	@$(MAKE) --no-print-directory kind-load-images IMAGES="stl-mock-blockchain-server:local"

# Deploy mock-blockchain-server to kind cluster (uses fixture data by default).
# To load real blocks for stress testing, run: make stress-test-data && make kind-deploy-mock-blockchain-server-s3
kind-deploy-mock-blockchain-server: kind-load-mock-blockchain-server
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/apps/mock-blockchain-server.yaml -n $(KUBE_NS)

# Deploy mock-blockchain-server with S3 block data (for stress testing).
kind-deploy-mock-blockchain-server-s3: kind-load-mock-blockchain-server stress-test-data
	kubectl --context=$(KUBE_CONTEXT) apply -f k8s/apps/mock-blockchain-server.yaml -n $(KUBE_NS)
	kubectl --context=$(KUBE_CONTEXT) patch deployment mock-blockchain-server -n $(KUBE_NS) --type=json \
	  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/args","value":["--addr=:8546","--admin-addr=:8547","--s3-bucket=stress-test-data","--s3-prefix=blocks/chain-1","--s3-endpoint=http://localstack:4566","--region=us-east-1"]}]'

# Point watcher at mock-blockchain-server instead of Alchemy
kind-use-mock:
	kubectl --context=$(KUBE_CONTEXT) set env deployment/watcher \
		ALCHEMY_WS_URL=ws://mock-blockchain-server:8546 \
		ALCHEMY_HTTP_URL=http://mock-blockchain-server:8546 \
		-n $(KUBE_NS)

# Restore watcher to real Alchemy (ConfigMap values take effect)
kind-use-alchemy:
	kubectl --context=$(KUBE_CONTEXT) set env deployment/watcher ALCHEMY_WS_URL- ALCHEMY_HTTP_URL- -n $(KUBE_NS)

# =============================================================================
# Stress Tests
# =============================================================================

# Block data export configuration (override via environment or make flags)
STRESS_TEST_SRC_BUCKET     ?= stl-sentinelstaging-ethereum-raw-89d540d0
STRESS_TEST_PREFIX         ?= blocks/chain-1
STRESS_TEST_START_BLOCK    ?= 16000000
STRESS_TEST_BLOCK_COUNT    ?= 500
STRESS_TEST_VERSION        ?= 1
STRESS_TEST_LOCAL_BUCKET        ?= stress-test-data
STRESS_TEST_LOCAL_ENDPOINT      ?= http://localhost:4566
STRESS_TEST_STAGING_BUCKET      ?= stl-sentinelstaging-stress-test-data-89d540d0
STRESS_TEST_TRANSFORMER_QUEUE   ?= stl-ethereum-transformer.fifo
STRESS_TEST_BACKUP_QUEUE        ?= stl-ethereum-backup.fifo

# Populate S3 with stress test block data.
# Local (default):        syncs blocks from staging S3 into LocalStack.
# Staging (TARGET=staging): exports blocks from raw backup bucket into the staging stress-test bucket
#                           (one-time operator step).
# Idempotent: skips when a manifest written by a previous run matches start_block, block_count, and version.
stress-test-data:
ifeq ($(TARGET),staging)
	@echo "==> Creating stress-test bucket in staging S3..."
	aws s3 mb s3://$(STRESS_TEST_STAGING_BUCKET) --region $(AWS_REGION) 2>/dev/null || true
	@WANT="start_block=$(STRESS_TEST_START_BLOCK) block_count=$(STRESS_TEST_BLOCK_COUNT) version=$(STRESS_TEST_VERSION)"; \
	GOT=$$(aws s3 cp s3://$(STRESS_TEST_STAGING_BUCKET)/$(STRESS_TEST_PREFIX)/.manifest - 2>/dev/null || true); \
	if [ "$$GOT" = "$$WANT" ]; then \
	  echo "==> Block data up-to-date ($$WANT), skipping export"; \
	else \
	  echo "==> Exporting $(STRESS_TEST_BLOCK_COUNT) blocks to staging S3 (requires staging creds)..."; \
	  go run ./cmd/stress-test/data-export \
	    --env staging \
	    --src-bucket $(STRESS_TEST_SRC_BUCKET) \
	    --dest-bucket $(STRESS_TEST_STAGING_BUCKET) \
	    --dest-prefix $(STRESS_TEST_PREFIX) \
	    --start-block $(STRESS_TEST_START_BLOCK) \
	    --block-count $(STRESS_TEST_BLOCK_COUNT) \
	    --version $(STRESS_TEST_VERSION) && \
	  echo "$$WANT" | aws s3 cp - s3://$(STRESS_TEST_STAGING_BUCKET)/$(STRESS_TEST_PREFIX)/.manifest; \
	fi
else
	@echo "==> Creating stress-test-data bucket in LocalStack..."
	aws --endpoint-url $(STRESS_TEST_LOCAL_ENDPOINT) s3 mb s3://$(STRESS_TEST_LOCAL_BUCKET) --region us-east-1 2>/dev/null || true
	@WANT="start_block=$(STRESS_TEST_START_BLOCK) block_count=$(STRESS_TEST_BLOCK_COUNT) version=$(STRESS_TEST_VERSION)"; \
	GOT=$$(aws --endpoint-url $(STRESS_TEST_LOCAL_ENDPOINT) s3 cp \
	  s3://$(STRESS_TEST_LOCAL_BUCKET)/$(STRESS_TEST_PREFIX)/.manifest - 2>/dev/null || true); \
	if [ "$$GOT" = "$$WANT" ]; then \
	  echo "==> Block data up-to-date ($$WANT), skipping sync"; \
	else \
	  echo "==> Syncing $(STRESS_TEST_BLOCK_COUNT) blocks from staging S3 to LocalStack (requires staging AWS credentials)..."; \
	  TMPDIR=$$(mktemp -d) && \
	  aws s3 sync s3://$(STRESS_TEST_STAGING_BUCKET)/$(STRESS_TEST_PREFIX) "$$TMPDIR" --region $(AWS_REGION) --quiet && \
	  AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
	    aws --endpoint-url $(STRESS_TEST_LOCAL_ENDPOINT) s3 sync "$$TMPDIR" \
	    s3://$(STRESS_TEST_LOCAL_BUCKET)/$(STRESS_TEST_PREFIX) --region us-east-1 --quiet && \
	  rm -rf "$$TMPDIR" && \
	  echo "$$WANT" | aws --endpoint-url $(STRESS_TEST_LOCAL_ENDPOINT) s3 cp \
	    - s3://$(STRESS_TEST_LOCAL_BUCKET)/$(STRESS_TEST_PREFIX)/.manifest; \
	fi
endif

# Run k6 stress test as a Job inside the cluster (kind or EKS).
#
# Options:
#   TARGET          - Set to "staging" to target real AWS instead of LocalStack (default: local)
#   SCENARIO        - Scenario to run from stress-test/k6/scenarios/ (default: watcher)
#                     e.g. SCENARIO=watcher → scenarios/watcher.js
#   INTERVAL_MS     - Block emission interval in ms (default: 12000 ~Ethereum)
#                     Examples: 50 (high-frequency), 250 (Base), 2000 (BNB Chain)
#   DURATION        - k6 test duration (default: 2m)
#                     Examples: 30s, 5m, 1h
#   REORG           - Enable reorg injection; set to any value to enable (default: off)
#   REORG_DEPTH     - Number of blocks rolled back per reorg, 1–64 (default: 5)
#   REORG_INTERVAL_S - Seconds between reorgs (default: 30)
#
# Examples:
#   make k6-stress-test
#   make k6-stress-test INTERVAL_MS=50 DURATION=30s
#   make k6-stress-test REORG=1 REORG_DEPTH=10 REORG_INTERVAL_S=15
#   make k6-stress-test TARGET=staging DURATION=5m
k6-stress-test:
	kubectl --context=$(KUBE_CONTEXT) exec -n $(KUBE_NS) timescaledb-0 -- psql -U postgres -d stl_verify -c "TRUNCATE block_states CASCADE;" -q
	kubectl --context=$(KUBE_CONTEXT) exec -n $(KUBE_NS) deployment/redis -- redis-cli FLUSHDB
ifeq ($(TARGET),staging)
	aws --region $(AWS_REGION) sqs purge-queue \
	  --queue-url https://sqs.$(AWS_REGION).amazonaws.com/$(AWS_ACCOUNT_ID)/$(STRESS_TEST_TRANSFORMER_QUEUE) || true
	aws --region $(AWS_REGION) sqs purge-queue \
	  --queue-url https://sqs.$(AWS_REGION).amazonaws.com/$(AWS_ACCOUNT_ID)/$(STRESS_TEST_BACKUP_QUEUE) || true
else
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url http://localhost:4566 --region us-east-1 \
	  sqs purge-queue --queue-url http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/$(STRESS_TEST_TRANSFORMER_QUEUE) || true
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url http://localhost:4566 --region us-east-1 \
	  sqs purge-queue --queue-url http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/$(STRESS_TEST_BACKUP_QUEUE) || true
endif
	kubectl --context=$(KUBE_CONTEXT) wait --for=condition=available deployment/mock-blockchain-server -n $(KUBE_NS) --timeout=120s
	kubectl --context=$(KUBE_CONTEXT) create configmap k6-scripts --from-file=stress-test/k6/scenarios/ --from-file=stress-test/k6/lib/ -n $(KUBE_NS) --dry-run=client -o yaml | kubectl --context=$(KUBE_CONTEXT) apply -f -
	kubectl --context=$(KUBE_CONTEXT) delete job k6-stress-test -n $(KUBE_NS) --ignore-not-found
	sed -e 's|$${SCENARIO}|$(or $(SCENARIO),watcher)|g' \
	    -e 's|$${INTERVAL_MS}|$(or $(INTERVAL_MS),12000)|g' \
	    -e 's|$${DURATION}|$(or $(DURATION),2m)|g' \
	    -e 's|$${REORG}|$(or $(REORG),)|g' \
	    -e 's|$${REORG_DEPTH}|$(or $(REORG_DEPTH),5)|g' \
	    -e 's|$${REORG_INTERVAL_S}|$(or $(REORG_INTERVAL_S),30)|g' \
	    k8s/jobs/k6-stress-test.yaml | kubectl --context=$(KUBE_CONTEXT) apply -f - -n $(KUBE_NS)
	kubectl --context=$(KUBE_CONTEXT) wait --for=condition=ready pod -l job-name=k6-stress-test -n $(KUBE_NS) --timeout=60s
	kubectl --context=$(KUBE_CONTEXT) logs job/k6-stress-test -n $(KUBE_NS) -f
	kubectl --context=$(KUBE_CONTEXT) wait --for=condition=complete job/k6-stress-test -n $(KUBE_NS) --timeout=10s
	@echo "==> Verifying chain correctness..."
	kubectl --context=$(KUBE_CONTEXT) exec -n $(KUBE_NS) timescaledb-0 -- psql -U postgres -d stl_verify \
	  -c "SELECT count(*) AS gaps FROM generate_series((SELECT min(number) FROM block_states WHERE chain_id = 1 AND NOT is_orphaned),(SELECT max(number) FROM block_states WHERE chain_id = 1 AND NOT is_orphaned)) AS s(n) LEFT JOIN block_states b ON b.number = s.n AND b.chain_id = 1 AND NOT b.is_orphaned WHERE b.number IS NULL;" \
	  -c "SELECT count(*) AS unexpected_orphans FROM block_states WHERE chain_id = 1 AND is_orphaned AND number > (SELECT max(number) - $(shell echo $$(( 64 + $(if $(REORG),$(or $(REORG_DEPTH),5),0) ))) FROM block_states WHERE chain_id = 1);" \
	  -c "SELECT count(*) AS broken_parent_links FROM block_states b JOIN block_states p ON p.number = b.number - 1 AND p.chain_id = b.chain_id AND NOT p.is_orphaned WHERE NOT b.is_orphaned AND b.chain_id = 1 AND b.parent_hash != p.hash;"
