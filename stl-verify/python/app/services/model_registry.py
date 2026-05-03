from collections.abc import Sequence

from app.domain.entities.allocation import EthAddress
from app.ports.risk_model import RiskModel


class ModelRegistry:
    def __init__(self, models: Sequence[RiskModel]) -> None:
        self._models = tuple(models)
        names = [m.risk_model for m in self._models]
        # Two models sharing a discriminator would make
        # ``overrides.get(m.risk_model, ...)`` ambiguous and silently break
        # the unknown-model 422 check. Fail loud at startup.
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise ValueError(f"duplicate risk_model discriminators: {duplicates}")
        self._risk_model_names = frozenset(names)

    def applicable(self, asset_id: int, prime_id: EthAddress) -> list[RiskModel]:
        return [m for m in self._models if m.applies_to(asset_id, prime_id)]

    @property
    def risk_model_names(self) -> frozenset[str]:
        """Discriminator strings of every registered model."""
        return self._risk_model_names
