# stl-verify Makefile
# Go 1.25+ best practices

.PHONY: all test test-race test-integration lint vet fmt tidy check clean cover run run-watcher run-watcher-avax help \
        dev-up dev-env dev-down dev-ips \
        tf-init tf-validate tf-plan tf-apply tf-apply-auto tf-destroy tf-state tf-check \
        ecr-login docker-build docker-push docker-release \
        docker-build-watcher-staging docker-push-watcher-staging docker-release-watcher-staging \
        docker-build-backup docker-push-backup docker-release-backup \
        docker-build-backup-staging docker-push-backup-staging docker-release-backup-staging \
        docker-build-oracle-price-worker docker-push-oracle-price-worker docker-release-oracle-price-worker \
        docker-build-oracle-price-worker-staging docker-push-oracle-price-worker-staging docker-release-oracle-price-worker-staging \
        ecs-deploy-watcher ecs-deploy-watcher-staging \
        bastion-connect bastion-connect-staging bastion-tailscale-status bastion-tailscale-status-staging \
        bastion-create-tailscale-secret bastion-create-tailscale-secret-staging tigerdata-tunnel \
        db-migrate db-set-passwords db-test-app-user \
        erigon-init erigon-validate erigon-plan erigon-apply erigon-apply-auto erigon-destroy erigon-check \
        erigon-status erigon-logs erigon-logs-follow erigon-deploy-readonly erigon-switch-readonly erigon-switch-sync erigon-mode \
        erigon-run build-bulk-download-arm64 deploy-bulk-download erigon-bulk-download \
        build-oracle-pricing-backfill-arm64 deploy-oracle-pricing-backfill

# Go bin path for installed tools
GOBIN := $(shell go env GOPATH)/bin

# Run the application
run:
	@echo "==> Running..."
	go run ./cmd/server

# Run the Ethereum watcher (requires dev-up first)
run-watcher:
	@echo "==> Running Ethereum watcher (chain_id=1)..."
	@set -a && . cmd/watcher/.env && [ -f cmd/watcher/.env.local ] && . cmd/watcher/.env.local; set +a && go run ./cmd/watcher

# Run the Avalanche watcher (requires dev-up first)
# Avalanche C-Chain does not support trace_block, so traces are disabled.
run-watcher-avax:
	@echo "==> Running Avalanche watcher (chain_id=43114)..."
	@set -a && . cmd/watcher/.env.avax && [ -f cmd/watcher/.env.avax.local ] && . cmd/watcher/.env.avax.local; set +a && go run ./cmd/watcher --enable-traces=false

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
ci: test-race vet fmt-check tidy-check vulncheck golangci-lint staticcheck

# =============================================================================
# Local Development
# =============================================================================

# Start all local services (Postgres, Redis, LocalStack, Jaeger)
dev-up:
	@docker compose up -d
	@until docker inspect stl-verify-localstack --format '{{.State.Health.Status}}' 2>/dev/null | grep -q healthy; do sleep 1; done
	@./localstack-init/init-aws.sh > /dev/null 2>&1
	@echo "Running database migrations..."
	@DATABASE_URL="postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" go run ./cmd/migrate
	@$(MAKE) dev-env

# Generate .env files for all services (fetches secrets from AWS)
# Can be re-run independently to refresh secrets without restarting services.
dev-env:
	@ALCHEMY_API_KEY=$$(aws secretsmanager get-secret-value --secret-id stl-sentinelstaging-watcher-config --query SecretString --output text | jq -r '.alchemy_api_key // empty') || \
		{ echo "WARNING: Failed to fetch ALCHEMY_API_KEY from AWS Secrets Manager. Set it manually in cmd/watcher/.env"; ALCHEMY_API_KEY=''; }; \
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
		> cmd/watcher/.env && \
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
		> cmd/watcher/.env.avax && \
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
		"LOG_LEVEL=debug" \
		> cmd/oracle-price-worker/.env
	@COINGECKO_API_KEY=$$(aws secretsmanager get-secret-value --secret-id coingecko_api_key --query SecretString --output text | jq -r '.coingecko_api_key // empty') || \
		{ echo "WARNING: Failed to fetch COINGECKO_API_KEY from AWS Secrets Manager."; COINGECKO_API_KEY=''; }; \
	printf '%s\n' \
		"ENVIRONMENT=development" \
		"COINGECKO_API_KEY=$$COINGECKO_API_KEY" \
		"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" \
		> cmd/price-fetcher/.env
	@printf '%s\n' \
		"DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable" \
		"LOG_LEVEL=debug" \
		> cmd/oracle-backfill/.env
	@ETHERSCAN_API_KEY=$$(aws secretsmanager get-secret-value --secret-id etherscan_api_key --query SecretString --output text) || \
		{ echo "WARNING: Failed to fetch ETHERSCAN_API_KEY from AWS Secrets Manager."; ETHERSCAN_API_KEY=''; }; \
	TIGERDATA_URL=$$(aws secretsmanager get-secret-value --secret-id stl-sentinelstaging-tigerdata-readonly --query SecretString --output text | jq -r '.connection_url // empty' | sed 's|@[^:]*:|@localhost:|') || \
		{ echo "WARNING: Failed to fetch TIGERDATA_URL from AWS Secrets Manager."; TIGERDATA_URL=''; }; \
	printf '%s\n' \
		"ENVIRONMENT=development" \
		"ETHERSCAN_API_KEY=$$ETHERSCAN_API_KEY" \
		"DATABASE_URL=$$TIGERDATA_URL" \
		> cmd/data-validator/.env
	@echo "Environment files generated."
	@echo "Note: data-validator uses staging TigerData. Run 'make tigerdata-tunnel' first."

# Stop all local services
dev-down:
	@docker compose down

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

# Apply Terraform changes (auto-approve)
# Usage: make tf-apply-auto ENV=sentinelstaging
tf-apply-auto:
	@echo "==> Applying Terraform for $(ENV) (auto-approve)..."
	$(call run_tofu_with_creds,tofu apply -auto-approve -var-file=environments/$(ENV).tfvars)

# Full Terraform workflow: init, validate, plan
# Usage: make tf-check ENV=sentinelstaging
tf-check: tf-init tf-validate tf-plan

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

# Force new deployment of watcher service (pulls latest image)
ecs-deploy-watcher:
	@echo "==> Forcing new deployment of watcher service..."
	aws ecs update-service --cluster stl-$(ENV)-cluster --service stl-$(ENV)-watcher --force-new-deployment --region $(AWS_REGION)
	@echo "==> Deployment initiated. Monitor with: aws ecs describe-services --cluster stl-$(ENV)-cluster --services stl-$(ENV)-watcher --region $(AWS_REGION)"

ecs-deploy-watcher-staging:
	@$(MAKE) ecs-deploy-watcher ENV=sentinelstaging

# =============================================================================
# Bastion Host Management
# =============================================================================

# SSH tunnel to TigerData via bastion (after Tailscale is connected)
# Usage: make tigerdata-tunnel BASTION_IP=<tailscale-ip>
# Then connect locally: psql "postgres://user:pass@localhost:5432/dbname"
tigerdata-tunnel:
	@if [ -z "$(BASTION_IP)" ] || [ -z "$(TIGERDATA_HOST)" ]; then \
		echo "Error: BASTION_IP and TIGERDATA_HOST are required"; \
		echo "Usage: make $@ BASTION_IP=<tailscale-ip> TIGERDATA_HOST=<tigerdata-host>"; \
		echo "Get TIGERDATA_HOST from: aws secretsmanager get-secret-value --secret-id stl-$(ENV)-tigerdata-connection --query SecretString --output text | jq -r .host"; \
		exit 1; \
	fi
	@echo "==> Starting SSH tunnel to TigerData via Tailscale..."
	@echo "    Local:  localhost:5432"
	@echo "    Remote: $(TIGERDATA_HOST):5432"
	@echo "    Press Ctrl+C to stop"
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
# Usage: make db-set-passwords DATABASE_URL="postgresql://tsdbadmin:pass@localhost:5432/tsdb?sslmode=require" ENV=sentinelstaging
# Requires:
#   1. Tunnel running: make tigerdata-tunnel BASTION_IP=... TIGERDATA_HOST=...
#   2. Terraform applied: make tf-apply ENV=sentinelstaging (creates secrets with passwords)
#   3. Migrations run: make db-migrate DATABASE_URL=... (creates users with placeholder passwords)
db-set-passwords:
	@if [ -z "$(DATABASE_URL)" ] || [ -z "$(ENV)" ]; then \
		echo "Error: DATABASE_URL and ENV are required"; \
		echo "Usage: make $@ DATABASE_URL=\"postgresql://tsdbadmin:pass@localhost:5432/tsdb?sslmode=require\" ENV=sentinelstaging"; \
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
# Usage: make db-test-app-user ENV=sentinelstaging
# Requires tunnel to be running first: make tigerdata-tunnel BASTION_IP=... TIGERDATA_HOST=...
db-test-app-user:
	@if [ -z "$(ENV)" ]; then \
		echo "Error: ENV is required"; \
		echo "Usage: make $@ ENV=sentinelstaging"; \
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
# Docker Build & Push - Oracle Price Worker
# =============================================================================

# ECR repository URL for oracle price worker
ECR_REPO_ORACLE_PRICE_WORKER = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/stl-$(ENV)-oracle-price-worker

# Build Docker image for oracle price worker (ARM64 for Fargate Graviton)
docker-build-oracle-price-worker:
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

# Push oracle price worker Docker image to ECR
docker-push-oracle-price-worker: ecr-login
	@echo "==> Pushing Oracle Price Worker Docker image to ECR..."
	docker push $(ECR_REPO_ORACLE_PRICE_WORKER):$(IMAGE_TAG)
	docker push $(ECR_REPO_ORACLE_PRICE_WORKER):latest

# Build and push oracle price worker in one step (captures tag at start to ensure consistency)
docker-release-oracle-price-worker: ecr-login
	@TAG=$$(git rev-parse --short HEAD) && \
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

# =============================================================================
# Erigon Node Terraform Commands
# =============================================================================

ERIGON_DIR := ephemeral-infra/erigon-node

# Helper to check AWS_PROFILE_ERIGON is set
define require_aws_profile_erigon
	@if [ -z "$(AWS_PROFILE_ERIGON)" ]; then \
		echo "Error: AWS_PROFILE_ERIGON is required"; \
		echo "Usage: make $@ AWS_PROFILE_ERIGON=<profile-name>"; \
		exit 1; \
	fi
endef

# Initialize Erigon Terraform
erigon-init:
	$(call require_aws_profile_erigon)
	@echo "==> Initializing Erigon Terraform..."
	cd $(ERIGON_DIR) && AWS_PROFILE=$(AWS_PROFILE_ERIGON) tofu init -backend-config=environments/sentinelstaging.backend.hcl

# Validate Erigon Terraform configuration
erigon-validate:
	$(call require_aws_profile_erigon)
	@echo "==> Validating Erigon Terraform..."
	cd $(ERIGON_DIR) && AWS_PROFILE=$(AWS_PROFILE_ERIGON) tofu validate

# Plan Erigon Terraform changes
erigon-plan:
	$(call require_aws_profile_erigon)
	@echo "==> Planning Erigon Terraform..."
	cd $(ERIGON_DIR) && AWS_PROFILE=$(AWS_PROFILE_ERIGON) tofu plan

# Apply Erigon Terraform changes
erigon-apply:
	$(call require_aws_profile_erigon)
	@echo "==> Applying Erigon Terraform..."
	cd $(ERIGON_DIR) && AWS_PROFILE=$(AWS_PROFILE_ERIGON) tofu apply

# Apply Erigon Terraform changes (auto-approve)
erigon-apply-auto:
	$(call require_aws_profile_erigon)
	@echo "==> Applying Erigon Terraform (auto-approve)..."
	cd $(ERIGON_DIR) && AWS_PROFILE=$(AWS_PROFILE_ERIGON) tofu apply -auto-approve

# Destroy Erigon infrastructure
erigon-destroy:
	$(call require_aws_profile_erigon)
	@echo "==> Destroying Erigon infrastructure..."
	cd $(ERIGON_DIR) && AWS_PROFILE=$(AWS_PROFILE_ERIGON) tofu destroy

# Full Erigon workflow: init, validate, plan
erigon-check: erigon-init erigon-validate erigon-plan

# =============================================================================
# Erigon Node Management (via Tailscale)
# =============================================================================

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
# Usage: make erigon-run ERIGON_USER=<user> ERIGON_IP=<ip> CMD="df -h"
erigon-run:
	$(call require_erigon_vars)
	@tailscale ssh $(ERIGON_USER)@$(ERIGON_IP) '$(CMD)'

# Build oracle-price-worker binary
build-oracle-worker:
	@echo "==> Building oracle-price-worker..."
	mkdir -p dist
	go build -o dist/oracle-price-worker ./cmd/oracle-price-worker
	@echo "==> Built: dist/oracle-price-worker"

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

