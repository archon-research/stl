# Adapters Layer

Adapters connect the application to the outside world.

## Inbound Adapters (Primary)

Located in `inbound/` - These adapters receive input from external sources
and translate it into calls to the application's inbound ports.

- `http/` - REST API handlers
- `grpc/` - gRPC service implementations
- `cli/` - Command-line interface

## Outbound Adapters (Secondary)

Located in `outbound/` - These adapters implement the outbound port interfaces
and handle communication with external systems.

- `postgres/` - PostgreSQL repository implementation
- `memory/` - In-memory repository (for testing)
- `kafka/` - Event publishing via Kafka
- `http/` - HTTP clients for external APIs
