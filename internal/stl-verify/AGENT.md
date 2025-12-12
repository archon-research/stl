# Agent Instructions: stl-verify

## Purpose
The `stl-verify` subsystem handles risk analysis and transaction verification.

## Structure
```
stl-verify/
├── domain/         # Risk models, verification logic
├── ports/          # VerifierService, RiskRepository interfaces
├── services/       # Verifier implementation
└── adapters/
    ├── primary/
    │   ├── cli/    # CLI commands
    │   └── http/   # HTTP handlers
    └── secondary/
        └── postgres/
            ├── migrations/  # SQL schema
            └── models/      # DB models
```

## Key Interfaces (ports/)
- `VerifierService` - Primary port for verification operations
- `RiskRepository` - Secondary port for persistence

## Domain Concepts
- `RiskModel` - Core risk calculation entity
- Verification is stateless calculation based on transaction data

## Adding Features
1. Add domain types to `domain/risk_model.go`
2. Extend interfaces in `ports/ports.go`
3. Implement in `services/verifier.go`
4. Expose via adapters (CLI or HTTP)

## Testing
```bash
go test ./internal/stl-verify/...
go test ./tests/integration/stl-verify/...
```

### Unit Testing Guidelines
- Test the `VerifierService` interface behavior, not the `Verifier` struct internals
- Mock `RiskRepository` via the interface, not the concrete implementation
- Test: "Given this input, verify returns this risk score" — not: "verify calls repo.Save exactly once"
- Refactoring `services/verifier.go` should not break tests if behavior is unchanged
