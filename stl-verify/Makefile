# stl-verify Makefile
# Go 1.25+ best practices

.PHONY: all test test-race test-integration lint vet fmt tidy check clean cover run run-watcher help \
        dev-up dev-down dev-ips \
        tf-init tf-validate tf-plan tf-apply tf-apply-auto tf-destroy tf-state tf-check \
        tf-init-staging tf-validate-staging tf-plan-staging tf-apply-staging tf-check-staging \
        ecr-login docker-build docker-push docker-release \
        docker-build-watcher-staging docker-push-watcher-staging docker-release-watcher-staging \
        docker-build-backup docker-push-backup docker-release-backup \
        docker-build-backup-staging docker-push-backup-staging docker-release-backup-staging \
        erigon-init erigon-validate erigon-plan erigon-apply erigon-apply-auto erigon-destroy erigon-check \
        erigon-status erigon-logs erigon-logs-follow erigon-deploy-readonly erigon-switch-readonly erigon-switch-sync erigon-mode \
        erigon-run build-bulk-download-arm64 deploy-bulk-download erigon-bulk-download

# Go bin path for installed tools
GOBIN := $(shell go env GOPATH)/bin

# Run the application
run:
	@echo "==> Running..."
	go run ./cmd/server

# Run the watcher (requires dev-up first)
run-watcher:
	@echo "==> Running watcher..."
	@set -a && . cmd/watcher/.env && [ -f cmd/watcher/.env.local ] && . cmd/watcher/.env.local; set +a && go run ./cmd/watcher

# Run tests with race detector
test-race:
	@echo "==> Running tests with race detector..."
	go test -race -v ./...

# Run unit tests only (default, no build tags)
test:
	@echo "==> Running unit tests..."
	go test -v ./...

# Run integration tests (requires integration build tag)
test-integration:
	@echo "==> Running integration tests..."
	go test -tags=integration -v -timeout=5m ./...

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
	./localstack-init/init-aws.sh
	@echo "==> Resolving Alchemy API key..."
	@ALCHEMY_KEY=""; \
	ALCHEMY_KEY=$$(aws secretsmanager get-secret-value \
		--secret-id stl-sentinelstaging-watcher-config \
		--query SecretString --output text --region eu-west-1 2>/dev/null | jq -r '.alchemy_api_key // empty' 2>/dev/null) || true; \
	if [ -n "$$ALCHEMY_KEY" ]; then \
		echo "    Found in AWS Secrets Manager"; \
	elif [ -f cmd/watcher/.env.local ]; then \
		ALCHEMY_KEY=$$(grep -E '^ALCHEMY_API_KEY=' cmd/watcher/.env.local 2>/dev/null | cut -d= -f2- | tr -d '"'"'" || true); \
		if [ -n "$$ALCHEMY_KEY" ]; then \
			echo "    Found in .env.local"; \
		else \
			echo "    WARNING: .env.local exists but ALCHEMY_API_KEY not set, using empty string"; \
		fi; \
	else \
		echo "    WARNING: No AWS access and no .env.local found, using empty string"; \
	fi; \
	echo "==> Generating .env with local service addresses..." && \
	echo "# =============================================================================" > cmd/watcher/.env && \
	echo "# Generated by Makefile dev-up on $$(date)" >> cmd/watcher/.env && \
	echo "# Used for local development (Host -> Docker)" >> cmd/watcher/.env && \
	echo "# =============================================================================" >> cmd/watcher/.env && \
	echo "" >> cmd/watcher/.env && \
	echo "# =============================================================================" >> cmd/watcher/.env && \
	echo "# Alchemy" >> cmd/watcher/.env && \
	echo "# =============================================================================" >> cmd/watcher/.env && \
	echo "ALCHEMY_API_KEY=$$ALCHEMY_KEY" >> cmd/watcher/.env && \
	echo "ALCHEMY_HTTP_URL=https://eth-mainnet.g.alchemy.com/v2" >> cmd/watcher/.env && \
	echo "ALCHEMY_WS_URL=wss://eth-mainnet.g.alchemy.com/v2" >> cmd/watcher/.env && \
	echo "" >> cmd/watcher/.env && \
	echo "# =============================================================================" >> cmd/watcher/.env && \
	echo "# Chain Configuration" >> cmd/watcher/.env && \
	echo "# =============================================================================" >> cmd/watcher/.env && \
	echo "CHAIN_ID=1" >> cmd/watcher/.env && \
	echo "" >> cmd/watcher/.env && \
	echo "# =============================================================================" >> cmd/watcher/.env && \
	echo "# LocalStack (Local AWS SNS/SQS)" >> cmd/watcher/.env && \
	echo "# =============================================================================" >> cmd/watcher/.env && \
	echo "AWS_SNS_ENDPOINT=http://127.0.0.1:4566" >> cmd/watcher/.env && \
	echo "AWS_ACCESS_KEY_ID=test" >> cmd/watcher/.env && \
	echo "AWS_SECRET_ACCESS_KEY=test" >> cmd/watcher/.env && \
	echo "AWS_REGION=us-east-1" >> cmd/watcher/.env && \
	echo "AWS_SNS_TOPIC_ARN=arn:aws:sns:us-east-1:000000000000:stl-ethereum-blocks.fifo" >> cmd/watcher/.env && \
	echo "" >> cmd/watcher/.env && \
	echo "# =============================================================================" >> cmd/watcher/.env && \
	echo "# Database and Redis" >> cmd/watcher/.env && \
	echo "# =============================================================================" >> cmd/watcher/.env && \
	echo "DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" >> cmd/watcher/.env && \
	echo "REDIS_ADDR=127.0.0.1:6379" >> cmd/watcher/.env && \
	echo "" >> cmd/watcher/.env && \
	echo "# =============================================================================" >> cmd/watcher/.env && \
	echo "# Jaeger (Tracing)" >> cmd/watcher/.env && \
	echo "# =============================================================================" >> cmd/watcher/.env && \
	echo "JAEGER_ENDPOINT=127.0.0.1:4317" >> cmd/watcher/.env && \
	echo "Generated .env file"
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

# Build and push in one step (single recipe to ensure consistent IMAGE_TAG)
docker-release: ecr-login
	@IMAGE_TAG=$$(git rev-parse --short HEAD) && \
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

# =============================================================================
# Sentinelstaging Docker Shortcuts
# =============================================================================

docker-build-watcher-staging:
	@$(MAKE) docker-build ENV=sentinelstaging

docker-push-watcher-staging:
	@$(MAKE) docker-push ENV=sentinelstaging

docker-release-watcher-staging:
	@$(MAKE) docker-release ENV=sentinelstaging

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
docker-release-backup: ecr-login
	@TAG=$$(git rev-parse --short HEAD) && \
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
# Erigon Node Terraform Commands
# =============================================================================

ERIGON_DIR := infrastructure/erigon-node
AWS_PROFILE_ERIGON ?= AdministratorAccess-579039992622

# Initialize Erigon Terraform
erigon-init:
	@echo "==> Initializing Erigon Terraform..."
	cd $(ERIGON_DIR) && AWS_PROFILE=$(AWS_PROFILE_ERIGON) tofu init

# Validate Erigon Terraform configuration
erigon-validate:
	@echo "==> Validating Erigon Terraform..."
	cd $(ERIGON_DIR) && AWS_PROFILE=$(AWS_PROFILE_ERIGON) tofu validate

# Plan Erigon Terraform changes
erigon-plan:
	@echo "==> Planning Erigon Terraform..."
	cd $(ERIGON_DIR) && AWS_PROFILE=$(AWS_PROFILE_ERIGON) tofu plan

# Apply Erigon Terraform changes
erigon-apply:
	@echo "==> Applying Erigon Terraform..."
	cd $(ERIGON_DIR) && AWS_PROFILE=$(AWS_PROFILE_ERIGON) tofu apply

# Apply Erigon Terraform changes (auto-approve)
erigon-apply-auto:
	@echo "==> Applying Erigon Terraform (auto-approve)..."
	cd $(ERIGON_DIR) && AWS_PROFILE=$(AWS_PROFILE_ERIGON) tofu apply -auto-approve

# Destroy Erigon infrastructure
erigon-destroy:
	@echo "==> Destroying Erigon infrastructure..."
	cd $(ERIGON_DIR) && AWS_PROFILE=$(AWS_PROFILE_ERIGON) tofu destroy

# Full Erigon workflow: init, validate, plan
erigon-check: erigon-init erigon-validate erigon-plan

# =============================================================================
# Erigon Node Management (via Tailscale)
# =============================================================================

# ERIGON_USER and ERIGON_IP are required for all erigon-* targets
# Usage: make erigon-status ERIGON_USER=root ERIGON_IP=<tailscale-ip>

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
	cat $(ERIGON_DIR)/erigon-readonly.service | tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) 'cat > /etc/systemd/system/erigon-readonly.service && systemctl daemon-reload'
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
# Usage: make erigon-run ERIGON_USER=root ERIGON_IP=<ip> CMD="df -h"
erigon-run:
	$(call require_erigon_vars)
	@tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) '$(CMD)'

# Build bulk-download binary for ARM64 Linux (to run on Erigon node)
build-bulk-download-arm64:
	@echo "==> Building bulk-download for ARM64 Linux..."
	mkdir -p dist
	GOOS=linux GOARCH=arm64 go build -o dist/bulk-download-linux-arm64 ./cmd/bulk-download
	@echo "==> Built: dist/bulk-download-linux-arm64"

# Deploy bulk-download to Erigon node
# Usage: make deploy-bulk-download ERIGON_USER=root ERIGON_IP=<ip>
deploy-bulk-download: build-bulk-download-arm64
	$(call require_erigon_vars)
	@echo "==> Deploying bulk-download to Erigon node..."
	cat dist/bulk-download-linux-arm64 | tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) 'cat > ~/bulk-download && chmod +x ~/bulk-download'
	@echo "==> Deployed to ~/bulk-download on Erigon node."

# Run bulk-download on Erigon node (dry-run by default)
# Usage: make erigon-bulk-download ERIGON_USER=root ERIGON_IP=<ip> START=16000000 END=16001000 [DRY_RUN=false]
DRY_RUN ?= true
erigon-bulk-download:
	$(call require_erigon_vars)
	@echo "==> Running bulk-download on Erigon node..."
	tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) '~/bulk-download \
		--start-block=$(START) \
		--end-block=$(END) \
		--bucket=stl-sentinelstaging-ethereum-raw-89d540d0 \
		$(if $(filter true,$(DRY_RUN)),--dry-run,)'

