from collections.abc import Sequence

from app.domain.entities.allocation import EthAddress
from app.ports.risk_model import RiskModel


class ModelRegistry:
    def __init__(self, models: Sequence[RiskModel]) -> None:
        self._models = tuple(models)
        self._risk_model_names = frozenset(m.risk_model for m in self._models)

    def applicable(self, asset_id: int, prime_id: EthAddress) -> list[RiskModel]:
        return [m for m in self._models if m.applies_to(asset_id, prime_id)]

    @property
    def risk_model_names(self) -> frozenset[str]:
        """Discriminator strings of every registered model."""
        return self._risk_model_names
