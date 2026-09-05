"""Auth plane integration (ADR-015): token verification and OpenFGA client.

Edge-agnostic by design — the app verifies tokens itself and consults OpenFGA
itself, so enforcement does not depend on a proxy writing trusted headers or on
a NetworkPolicy staying correct. Everything here is inert unless
``Settings.auth_enabled`` is true.
"""
