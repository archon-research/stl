# stl-verify Makefile
# Go 1.25+ best practices

.PHONY: all test lint vet fmt tidy check clean cover run help \
        tf-init tf-validate tf-plan tf-apply tf-apply-auto tf-destroy tf-state tf-check \
        tf-init-staging tf-validate-staging tf-plan-staging tf-apply-staging tf-check-staging

# Go bin path for installed tools
GOBIN := $(shell go env GOPATH)/bin

# Run the application
run:
	@echo "==> Running..."
	go run ./cmd/server

# Run tests with race detector
test-race:
	@echo "==> Running tests with race detector..."
	go test -race -v ./...

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

# Format code
fmt:
	@echo "==> Formatting code..."
	go fmt ./...
	gofmt -s -w .

# Check formatting (CI-friendly, fails if not formatted)
fmt-check:
	@echo "==> Checking format..."
	@test -z "$$(gofmt -l .)" || (echo "Files not formatted:"; gofmt -l .; exit 1)

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
	@echo "==> Running staticcheck with all checks..."
	$(GOBIN)/staticcheck ./...

# Run golangci-lint with all linters (install if needed)
golangci-lint:
	@echo "==> Running golangci-lint with all linters..."
	$(GOBIN)/golangci-lint run

# Run govulncheck for security vulnerabilities (install if needed)
vulncheck:
	@echo "==> Running govulncheck..."
	$(GOBIN)/govulncheck ./...

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
	go test -tags=e2e -v -timeout=5m ./cmd/watcher/...

# Install development tools
tools:
	@echo "==> Installing development tools..."
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/go-delve/delve/cmd/dlv@latest
	go install golang.org/x/tools/gopls@latest

# CI target: everything with strict checks
ci: test-race vet fmt-check tidy-check staticcheck vulncheck golangci-lint

# =============================================================================
# Terraform Commands
# =============================================================================

INFRA_DIR := ../infra

# Helper to load TigerData credentials from Secrets Manager
# This is needed because provider configuration happens before data sources are read
define load_tigerdata_creds
	$(eval SECRET_JSON := $(shell aws secretsmanager get-secret-value --secret-id stl-$(ENV)-tigerdata --query SecretString --output text 2>/dev/null || echo '{}'))
	$(eval export TF_VAR_tigerdata_project_id := $(shell echo '$(SECRET_JSON)' | jq -r '.project_id // empty'))
	$(eval export TF_VAR_tigerdata_access_key := $(shell echo '$(SECRET_JSON)' | jq -r '.access_key // empty'))
	$(eval export TF_VAR_tigerdata_secret_key := $(shell echo '$(SECRET_JSON)' | jq -r '.secret_key // empty'))
endef

# Initialize Terraform for an environment
# Usage: make tf-init ENV=sentinelstaging
tf-init:
	@echo "==> Initializing Terraform for $(ENV)..."
	cd $(INFRA_DIR) && tofu init -backend-config=environments/$(ENV).backend.hcl -reconfigure

# Validate Terraform configuration
# Usage: make tf-validate ENV=sentinelstaging
tf-validate:
	@echo "==> Validating Terraform for $(ENV)..."
	@$(call load_tigerdata_creds)
	cd $(INFRA_DIR) && tofu validate

# Plan Terraform changes
# Usage: make tf-plan ENV=sentinelstaging
tf-plan:
	@echo "==> Planning Terraform for $(ENV)..."
	@$(call load_tigerdata_creds)
	cd $(INFRA_DIR) && tofu plan -var-file=environments/$(ENV).tfvars

# Apply Terraform changes
# Usage: make tf-apply ENV=sentinelstaging
tf-apply:
	@echo "==> Applying Terraform for $(ENV)..."
	@$(call load_tigerdata_creds)
	cd $(INFRA_DIR) && tofu apply -var-file=environments/$(ENV).tfvars

# Full Terraform workflow: init, validate, plan
# Usage: make tf-check ENV=sentinelstaging
tf-check: tf-init tf-validate tf-plan

# =============================================================================
# Sentinelstaging Shortcuts
# =============================================================================

tf-init-staging:
	@$(MAKE) tf-init ENV=sentinelstaging

tf-validate-staging:
	@$(MAKE) tf-validate ENV=sentinelstaging

tf-plan-staging:
	@$(MAKE) tf-plan ENV=sentinelstaging

tf-apply-staging:
	@$(MAKE) tf-apply ENV=sentinelstaging

tf-check-staging: tf-init-staging tf-validate-staging tf-plan-staging
