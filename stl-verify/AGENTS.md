# Agent Instructions

This document provides guidance for AI coding agents working on this codebase.

## Project Overview

**stl-verify** is a Go service using Ports and Adapters (Hexagonal) architecture.

## Architecture

```
cmd/server/          # Application entry point
internal/
├── domain/          # Core business logic (no external dependencies)
│   ├── entity/      # Business objects with identity
│   ├── valueobject/ # Immutable value types
│   └── service/     # Domain services
├── ports/           # Interface definitions (boundaries)
│   ├── inbound/     # Use case interfaces (what the app exposes)
│   └── outbound/    # Infrastructure interfaces (what the app needs)
├── application/     # Use case implementations
├── adapters/        # Concrete implementations
│   ├── inbound/     # HTTP handlers, CLI, gRPC (incoming requests)
│   └── outbound/    # Repositories, external APIs (outgoing calls)
```

## Key Principles

1. **Dependency Rule**: Dependencies flow inward. Domain has no dependencies; adapters depend on ports; ports depend on domain.

2. **Interface Segregation**: Define ports as small, focused interfaces. Prefer multiple small interfaces over one large one.

3. **Dependency Injection**: All dependencies are injected via constructors. Never import adapters directly in application code.

## Code Conventions

### Go Standards
- Use `gofmt` and `goimports` for formatting
- Follow [Effective Go](https://go.dev/doc/effective_go) guidelines
- Use meaningful variable names; avoid single letters except for short loops
- Errors should be wrapped with context: `fmt.Errorf("doing X: %w", err)`

### Naming
- Interfaces: Use `-er` suffix when describing behavior (e.g., `Reader`, `Publisher`)
- Constructors: Use `New` prefix (e.g., `NewVerificationService`)
- Files: Use snake_case (e.g., `verification_service.go`)

### Testing
- Place tests in the same package with `_test.go` suffix
- Use table-driven tests for multiple cases
- Mock outbound ports for unit testing application services
- Use the in-memory adapter for integration tests

## Adding New Features

### New Use Case
1. Add method to inbound port interface in `ports/inbound/`
2. Implement the method in `application/`
3. Add HTTP handler in `adapters/inbound/http/`

### New External Dependency
1. Define interface in `ports/outbound/`
2. Implement adapter in `adapters/outbound/<name>/`
3. Inject via constructor in `cmd/<cmd>/main.go`

### New Entity
1. Create entity in `domain/entity/`
2. Add repository methods to outbound port
3. Implement in relevant adapters

## Common Commands

```bash
# Build
go build ./...

# Test
go test ./...

# Run
go run ./cmd/server

# Format
gofmt -w .
goimports -w .
```

## Do NOT

- Import adapters in the domain or application layer
- Add business logic to adapters
- Use global state or singletons
- Skip error handling
- Commit generated files or binaries
