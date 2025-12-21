# Domain Layer

This is the core of the application containing:

- **Entities**: Core business objects with identity
- **Value Objects**: Immutable objects without identity
- **Domain Services**: Business logic that doesn't naturally fit within an entity
- **Domain Events**: Events that occur within the domain

This layer has **no dependencies** on external packages or infrastructure.
All interfaces (ports) that the domain needs are defined here.
