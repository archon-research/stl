---
description: 'Code review instructions for STL Verify'
applyTo: '**'
---

# STL Verify Code Review Instructions

STL Verify is a Go service using **Ports and Adapters (Hexagonal) Architecture** that watches Ethereum blocks via Alchemy WebSocket, handles chain reorganizations, and publishes to AWS SNS.

## Review Priorities

### 🔴 CRITICAL - Block merge
- **Architecture violations**: Adapters imported in domain/application layers
- **Security**: Exposed secrets, missing error handling on external calls
- **Data integrity**: Chain reorg handling errors, race conditions
- **Resource leaks**: Unclosed DB connections, HTTP bodies, file handles

### 🟡 IMPORTANT - Discuss before merge
- **Testing**: Missing tests for critical paths, no table-driven tests
- **Error handling**: Generic errors without context, swallowed errors
- **Hexagonal violations**: Business logic in adapters, missing ports
- **Performance**: N+1 queries, missing pagination on large datasets

### 🟢 SUGGESTION - Non-blocking
- **Naming**: Unclear variable/function names
- **Documentation**: Missing godoc on exported functions
- **Style**: Inconsistent formatting

## Architecture Enforcement

### Dependency Rules (MUST FOLLOW)
- **Domain layer** (`internal/domain/`): NO external dependencies, only standard library
- **Application layer** (`internal/application/`): Depends only on domain and ports
- **Adapters** (`internal/adapters/`): Implement ports, can use any dependencies
- **Ports** (`internal/ports/`): Interface definitions only

**Violation Example:**
```go
// ❌ BAD: Domain importing database package
package domain
import "stl-verify/internal/adapters/postgres"  // NEVER DO THIS

// ❌ BAD: Application importing adapter directly
package application
import "stl-verify/internal/adapters/alchemy"  // NEVER DO THIS
```

### Interface Naming
- Use `-er` suffix for behavior interfaces: `BlockReader`, `EventPublisher`
- Keep interfaces small and focused

## Go-Specific Patterns

### Error Handling
```go
// ❌ BAD: Generic or swallowed errors
user, err := db.Get(id)
if err != nil {
    return err  // Lost context!
}

// ❌ BAD: Silent failures
try {
    processBlock(block)
} catch { }  // Never ignore errors

// ✅ GOOD: Wrap with context
user, err := db.Get(ctx, id)
if err != nil {
    return fmt.Errorf("fetching user %d: %w", id, err)
}
```

### Testing
```go
// ✅ GOOD: Table-driven tests with clear names
func TestCalculateDiscount(t *testing.T) {
    tests := []struct {
        name     string
        total    float64
        expected float64
    }{
        {"standard order", 50.0, 5.0},
        {"premium order", 150.0, 22.5},
        {"edge case zero", 0.0, 0.0},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := CalculateDiscount(tt.total)
            assert.Equal(t, tt.expected, result)
        })
    }
}
```

### Resource Management
```go
// ✅ GOOD: Always defer close
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    return fmt.Errorf("beginning transaction: %w", err)
}
defer tx.Rollback()  // Safe to call even after Commit

// ✅ GOOD: HTTP body handling
resp, err := client.Do(req)
if err != nil {
    return fmt.Errorf("alchemy request: %w", err)
}
defer resp.Body.Close()
```

## Blockchain/Ethereum Specifics

### Big.Int Handling
```go
// ❌ BAD: Using float64 for wei amounts
amount := float64(wei) / 1e18  // Precision loss!

// ✅ GOOD: Use big.Int arithmetic
amount := new(big.Int).Div(wei, big.NewInt(1e18))
```

### Chain Reorganization Safety
```go
// ✅ GOOD: Always handle reorg detection
if block.Number <= lastFinalized {
    return fmt.Errorf("block %d already finalized, possible reorg", block.Number)
}
```

### Address Validation
```go
// ❌ BAD: No validation
addr := "0x" + userInput

// ✅ GOOD: Checksum validation
if !common.IsHexAddress(addr) {
    return fmt.Errorf("invalid address: %s", addr)
}
```

## Infra Patterns

### AWS SDK Usage
```go
// ✅ GOOD: Context propagation and timeout
cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()

result, err := snsClient.PublishWithContext(cctx, input)
```

### Database Transactions
```go
// ✅ GOOD: Use the repository pattern with transaction support
func (r *PostgresRepo) SaveBlock(ctx context.Context, block *entity.Block) error {
    return r.withTx(ctx, func(tx *sql.Tx) error {
        // All operations use same transaction
        if err := r.saveBlock(tx, block); err != nil {
            return fmt.Errorf("saving block: %w", err)
        }
        return r.saveReceipts(tx, block.Receipts)
    })
}
```

## Code Style

### Naming
- **Files**: `snake_case.go` (e.g., `block_repository.go`)
- **Constructors**: `New` prefix (e.g., `NewBlockRepository`)
- **Variables**: Meaningful names, avoid `i`, `j`, `k` except in short loops
- **Acronyms**: Keep uppercase (e.g., `URL`, `HTTP`, `DB`)

### Formatting
- Run `gofmt` and `goimports` before committing
- Max line length: 120 characters (pragmatic, not strict)
- Group imports: standard library, external, internal

## Comment Format

Use this structure for review comments:

```markdown
**[PRIORITY] Category: Brief title**

Description of the issue.

**Why:** Impact and reasoning.

**Fix:**
```go
// Corrected code here
```
```

Example:
```markdown
**🟡 IMPORTANT - Architecture: Business logic in adapter**

The block validation logic is implemented directly in the Alchemy adapter.

**Why:** This couples business rules to Alchemy's API. If we switch providers, we lose validation.

**Fix:**
Move validation to domain service:
```go
// In domain/service/block_validator.go
func (v *BlockValidator) ValidateBlock(block *entity.Block) error {
    // Validation logic here
}
```
```

## Tech Stack Context

- **Go**: 1.25+
- **Database**: PostgreSQL with sqlx
- **Cache**: Redis
- **AWS**: SNS (FIFO), SQS, S3
- **Blockchain**: Ethereum mainnet via Alchemy
- **Observability**: OpenTelemetry + Jaeger
- **Testing**: stretchr/testify, table-driven tests
