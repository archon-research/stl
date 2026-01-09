# Ports Layer

Ports define the boundaries of the application through interfaces.

## Inbound Ports (Primary)

Located in `inbound/` - These are interfaces that the outside world uses to interact
with our application. They define the use cases/application services.

Examples:
- `VerificationService` - Use cases for verification operations

## Outbound Ports (Secondary)

Located in `outbound/` - These are interfaces that our application uses to interact
with the outside world. They are implemented by adapters.

Examples:
- `Repository` - Data persistence interface
- `EventPublisher` - Event publishing interface
- `ExternalAPIClient` - External service interface
