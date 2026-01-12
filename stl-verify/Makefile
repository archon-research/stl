# stl-verify Makefile
# Go 1.25+ best practices

.PHONY: all test lint vet fmt tidy check clean cover cover-all bench-postgres run help e2e

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
	$(GOBIN)/staticcheck -checks=all ./...

# Run golangci-lint with all linters (install if needed)
golangci-lint:
	@echo "==> Running golangci-lint with all linters..."
	$(GOBIN)/golangci-lint run --enable-all

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
	@echo "    Expected runtime: 10-20 minutes"
	go test -tags=benchmark -v -timeout=60m -run TestLargeDataset_QueryPerformance ./internal/adapters/outbound/postgres/...

# Run PostgreSQL EXPLAIN ANALYZE (for query plan inspection)
bench-postgres-explain:
	@echo "==> Running PostgreSQL EXPLAIN ANALYZE tests..."
	go test -tags=benchmark -v -timeout=30m -run TestExplainAnalyze ./internal/adapters/outbound/postgres/...

# Run Go benchmarks for PostgreSQL (uses 1M rows for faster iteration)
bench-postgres-go:
	@echo "==> Running Go benchmarks for PostgreSQL..."
	go test -tags=benchmark -bench=BenchmarkQueries -benchmem -timeout=30m ./internal/adapters/outbound/postgres/...

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