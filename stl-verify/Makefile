# stl-verify Makefile
# Go 1.25+ best practices

.PHONY: all test lint vet fmt tidy check clean cover run help \
        dev-up dev-down dev-ips \
        tf-init tf-validate tf-plan tf-apply tf-apply-auto tf-destroy tf-state tf-check \
        tf-init-staging tf-validate-staging tf-plan-staging tf-apply-staging tf-check-staging \
        ecr-login docker-build docker-push docker-release \
        docker-build-staging docker-push-staging docker-release-staging

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
	@echo "==> Running staticcheck..."
	$(GOBIN)/staticcheck ./...

# Run golangci-lint with all linters (install if needed)
golangci-lint:
	@echo "==> Running golangci-lint..."
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
# Local Development
# =============================================================================

# Start all local services (Postgres, Redis, LocalStack, Jaeger)
dev-up:
	@echo "==> Starting local services..."
	docker compose up -d
	@echo "==> Waiting for LocalStack to be healthy..."
	@until docker inspect stl-verify-localstack --format '{{.State.Health.Status}}' 2>/dev/null | grep -q healthy; do \
		echo "Waiting for LocalStack..."; \
		sleep 2; \
	done
	@echo "==> Initializing LocalStack resources..."
	@LOCALSTACK_IP=$$(docker inspect stl-verify-localstack --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'); \
	AWS_ENDPOINT_URL=http://$$LOCALSTACK_IP:4566 ./localstack-init/init-aws.sh
	@echo "==> Local services ready!"

# Stop all local services
dev-down:
	@echo "==> Stopping local services..."
	docker compose down

# Show service IPs (useful for devcontainer)
dev-ips:
	@echo "=== Service IPs ==="
	@docker inspect stl-verify-postgres --format 'Postgres:    {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 2>/dev/null || echo "Postgres: not running"
	@docker inspect stl-verify-redis --format 'Redis:       {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 2>/dev/null || echo "Redis: not running"
	@docker inspect stl-verify-localstack --format 'LocalStack:  {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 2>/dev/null || echo "LocalStack: not running"
	@docker inspect stl-verify-jaeger --format 'Jaeger:      {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 2>/dev/null || echo "Jaeger: not running"

# =============================================================================
# Terraform Commands
# =============================================================================

INFRA_DIR := ../infra

# Helper script to load TigerData credentials and run tofu
# This exports credentials directly in the shell to ensure they're passed to tofu
define run_tofu_with_creds
	@SECRET_JSON=$$(aws secretsmanager get-secret-value --secret-id stl-$(ENV)-tigerdata --query SecretString --output text 2>/dev/null || echo '{}') && \
	export TF_VAR_tigerdata_project_id=$$(echo "$$SECRET_JSON" | jq -r '.project_id // empty') && \
	export TF_VAR_tigerdata_access_key=$$(echo "$$SECRET_JSON" | jq -r '.access_key // empty') && \
	export TF_VAR_tigerdata_secret_key=$$(echo "$$SECRET_JSON" | jq -r '.secret_key // empty') && \
	cd $(INFRA_DIR) && $(1)
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
	$(call run_tofu_with_creds,tofu validate)

# Plan Terraform changes
# Usage: make tf-plan ENV=sentinelstaging
tf-plan:
	@echo "==> Planning Terraform for $(ENV)..."
	$(call run_tofu_with_creds,tofu plan -var-file=environments/$(ENV).tfvars)

# Apply Terraform changes
# Usage: make tf-apply ENV=sentinelstaging
tf-apply:
	@echo "==> Applying Terraform for $(ENV)..."
	$(call run_tofu_with_creds,tofu apply -var-file=environments/$(ENV).tfvars)

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

# Build and push in one step
docker-release: docker-build docker-push
	@echo "==> Docker image released: $(ECR_REPO):$(IMAGE_TAG)"

# Clean up local Docker images
docker-clean:
	@echo "==> Cleaning up Docker images..."
	-docker rmi $$(docker images --filter=reference='*stl-*-watcher*' -q) 2>/dev/null || true
	-docker builder prune -f
	@echo "==> Docker cleanup complete"

# =============================================================================
# Sentinelstaging Docker Shortcuts
# =============================================================================

docker-build-staging:
	@$(MAKE) docker-build ENV=sentinelstaging

docker-push-staging:
	@$(MAKE) docker-push ENV=sentinelstaging

docker-release-staging:
	@$(MAKE) docker-release ENV=sentinelstaging
