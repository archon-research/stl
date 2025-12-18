# Tests

Integration, E2E, and benchmark tests. Unit tests live next to source code.

## Contents
- `integration/stl-*/` - Tests requiring DB or external services
- `e2e/` - Full system tests
- `benchmarks/` - Performance tests
- `testutil/` - Shared mocks (`mocks.go`) and fixtures (`fixtures.go`)

## Conventions
- Integration/E2E tests skip with `-short` flag
- Use `testutil/mocks.go` for mock implementations
- Use `testutil/fixtures.go` for test data

## Running
```bash
go test ./tests/integration/...   # Integration
go test ./tests/e2e/...           # E2E
go test -bench=. ./tests/benchmarks/...  # Benchmarks
```
go test -bench=. ./tests/benchmarks/...
```

## Adding Tests
- New integration test: `tests/integration/stl-<subsystem>/<feature>_test.go`
- New mock: Add to `testutil/mocks.go`
- New fixture: Add to `testutil/fixtures.go`
