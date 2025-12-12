# Agent Instructions: Tests

## Purpose
Contains integration, E2E, and benchmark tests. Unit tests live next to source code.

## Structure
```
tests/
├── integration/      # Tests requiring external dependencies (DB)
│   ├── stl-trade/
│   └── stl-verify/
├── e2e/              # Full system tests
├── benchmarks/       # Performance tests
└── testutil/         # Shared mocks and fixtures
```

## Conventions
- Integration/E2E tests should skip with `-short` flag:
  ```go
  if testing.Short() {
      t.Skip("skipping integration test")
  }
  ```
- Use `testutil/mocks.go` for mock implementations
- Use `testutil/fixtures.go` for test data

## Unit Testing Principles
**Test behavior, not implementation details.**

✅ DO:
- Test public interfaces defined in `ports/`
- Verify outputs and observable side effects
- Use table-driven tests for multiple scenarios
- Mock dependencies via interfaces

❌ DON'T:
- Test private/internal functions directly
- Assert on exact method call counts or argument order
- Couple tests to internal data structures
- Write tests that break when refactoring without behavior change

### Example
```go
// Good: Tests behavior
func TestVerifier_Verify_ReturnsRiskScore(t *testing.T) {
    v := services.NewVerifier(mockRepo)
    result, err := v.Verify("tx-123")
    assert.NoError(t, err)
    assert.True(t, result.Score >= 0 && result.Score <= 1)
}

// Bad: Tests implementation
func TestVerifier_Verify_CallsRepoOnce(t *testing.T) {
    // This breaks if we add caching or batch writes
    assert.Equal(t, 1, mockRepo.SaveCallCount)
}
```

## Running Tests
```bash
# Unit tests only
go test ./internal/... ./pkg/...

# Skip slow tests
go test -short ./...

# Integration tests
go test ./tests/integration/...

# Benchmarks
go test -bench=. ./tests/benchmarks/...
```

## Adding Tests
- New integration test: `tests/integration/stl-<subsystem>/<feature>_test.go`
- New mock: Add to `testutil/mocks.go`
- New fixture: Add to `testutil/fixtures.go`
