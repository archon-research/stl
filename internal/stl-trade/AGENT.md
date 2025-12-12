# Agent Instructions: stl-trade

## Purpose
The `stl-trade` subsystem handles trade execution and order management.

## Structure
```
stl-trade/
├── domain/         # Order models, execution logic
├── ports/          # TradeService, OrderRepository interfaces
├── services/       # Executor implementation
└── adapters/
    ├── primary/    # HTTP handlers
    └── secondary/
        └── postgres/
            ├── migrations/  # SQL schema
            └── models/      # DB models
```

## Key Interfaces (ports/)
- `TradeService` / `ExecutorService` - Primary port for trade operations
- `OrderRepository` - Secondary port for order persistence

## Domain Concepts
- `Order` - Trade order with symbol, amount, side (BUY/SELL)
- Execution strategies live in domain logic

## Adding Features
1. Add domain types to `domain/order.go`
2. Extend interfaces in `ports/ports.go`
3. Implement in `services/executor.go`
4. Expose via HTTP adapter

## Testing
```bash
go test ./internal/stl-trade/...
go test ./tests/integration/stl-trade/...
```

### Unit Testing Guidelines
- Test the `TradeService` interface behavior, not the `Executor` struct internals
- Mock `OrderRepository` via the interface, not the concrete implementation
- Test: "Given this order, execution succeeds/fails" — not: "executor calls repo.SaveOrder with these exact args"
- Refactoring `services/executor.go` should not break tests if behavior is unchanged
