# stl-trade

Trade execution and order management subsystem.

## Key Interfaces
- `TradeService` / `ExecutorService` - Primary port for trade operations
- `OrderRepository` - Secondary port for order persistence

## Domain Concepts
- `Order` - Trade order with symbol, amount, side (BUY/SELL)
- Execution strategies live in domain logic

## Key Files
- `domain/order.go` - Order models and validation
- `interfaces/interfaces.go` - Interface definitions
- `services/executor.go` - Trade execution logic
- `adapters/primary/handler.go` - HTTP handlers
- `adapters/secondary/postgres/migrations/` - DB schema
