# Agent Instructions

This document provides guidance for AI agents working with the STL codebase.

## Project Overview

STL is a Go-based modular monolith for blockchain risk analysis and verification. It uses Ports & Adapters (Hexagonal Architecture) for each subsystem.

## Key Conventions

### Go Version
- Go 1.25

### Module Path
- `github.com/archon-research/stl`

### Architecture Pattern
Each subsystem follows **Ports & Adapters**:
- `domain/` - Pure business logic, no external dependencies
- `interfaces/` - Interfaces defining boundaries (no implementations)
- `services/` - Application services implementing business use cases
- `adapters/primary/` - Driving adapters (HTTP, CLI, gRPC)
- `adapters/secondary/` - Driven adapters (databases, external APIs)

### Directory Structure
- `cmd/` - Entry points (main.go files only)
- `internal/` - Private application code
- `internal/gateways/` - Shared infrastructure clients (Ethereum, Postgres)
- `internal/stl-*/` - Subsystem code
- `pkg/` - Public libraries for external use
- `tests/` - Integration, E2E, and benchmark tests
- `pipelines/` - Temporal.io workflows

### Naming Conventions
- Subsystems are prefixed with `stl-` (e.g., `stl-verify`, `stl-trade`)
- Package names match directory names (use underscores for multi-word: avoid if possible)
- Interfaces in `interfaces/` define the contract; implementations live in `adapters/` or `services/`

### Testing
- Unit tests: Next to source files (`*_test.go`)
- Integration tests: `tests/integration/`
- E2E tests: `tests/e2e/`
- Benchmarks: `tests/benchmarks/`
- Use `-short` flag to skip slow tests

### Unit Testing Principles
- **Test behavior, not implementation** - Tests should verify what a function does, not how it does it
- Test through public interfaces (ports), not internal methods
- Avoid testing private functions directly; test them through their public callers
- Mock dependencies via interfaces, not concrete types
- If refactoring breaks tests but not behavior, the tests are too coupled to implementation
- Focus on inputs → outputs and side effects, not internal state

### Database
- Each subsystem has its own schema/migrations in `adapters/secondary/postgres/migrations/`
- Shared connection logic is in `internal/gateways/postgres/`

## Common Tasks

### Adding a new feature to a subsystem
1. Define domain types in `domain/`
2. Add interface to `ports/`
3. Implement service logic in `services/`
4. Wire up adapters in `adapters/`

### Adding a new subsystem
1. Create `internal/stl-<name>/` with `domain/`, `ports/`, `services/`, `adapters/`
2. Add entry point in `cmd/stl-<name>/`
3. Add migrations in `adapters/secondary/postgres/migrations/`
4. Add integration tests in `tests/integration/stl-<name>/`

### Running the project
```bash
go build ./...
go test ./...
go run ./cmd/stl-verify
go run ./cmd/stl-trade
```

## Do NOT
- Put business logic in adapters
- Import `internal/stl-X` from `internal/stl-Y` (subsystems should be independent)
- Use concrete types in ports (use interfaces)
- Skip writing tests for new functionality
