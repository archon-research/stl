# Tests

This directory contains all tests that are not unit tests. Unit tests are kept alongside the source code they test (e.g., `verifier_test.go` next to `verifier.go`).

## Structure

```
tests/
├── integration/          # Integration tests (require external dependencies)
│   ├── stl-verify/       # Verification subsystem integration tests
│   └── stl-trade/        # Trading subsystem integration tests
├── e2e/                  # End-to-end tests (full system)
├── benchmarks/           # Performance benchmark tests
└── testutil/             # Shared test utilities, mocks, and fixtures
```

## Running Tests

### Unit Tests
```bash
go test ./internal/... ./pkg/...
```

### Unit Tests (short mode, skip slow tests)
```bash
go test -short ./...
```

### Integration Tests
```bash
go test ./tests/integration/...
```

### E2E Tests
```bash
go test ./tests/e2e/...
```

### Benchmark Tests
```bash
go test -bench=. ./tests/benchmarks/...
```

### All Tests
```bash
go test ./...
```

## Test Tags

Integration and E2E tests check for `-short` flag and skip when running in short mode:
```bash
go test -short ./...  # Skips integration and e2e tests
```
