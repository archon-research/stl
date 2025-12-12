# stl-verify

Risk analysis and transaction verification subsystem.

## Key Interfaces
- `VerifierService` - Primary port for verification operations
- `RiskRepository` - Secondary port for persistence

## Domain Concepts
- `RiskModel` - Core risk calculation entity
- Verification is stateless calculation based on transaction data

## Key Files
- `domain/risk_model.go` - Risk calculation logic
- `ports/ports.go` - Interface definitions
- `services/verifier.go` - Service implementation
- `adapters/primary/cli/` - CLI commands
- `adapters/primary/http/` - HTTP handlers
- `adapters/secondary/postgres/migrations/` - DB schema
