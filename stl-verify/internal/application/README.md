# Application Layer

This layer contains the implementation of the inbound ports (use cases).
It orchestrates the flow of data between the domain and the outbound ports.

Application services:
- Implement the inbound port interfaces
- Coordinate domain objects to perform use cases
- Use outbound ports to interact with infrastructure
- Handle transactions and cross-cutting concerns

This layer depends on:
- Domain layer (entities, value objects)
- Port interfaces (but NOT adapter implementations)
