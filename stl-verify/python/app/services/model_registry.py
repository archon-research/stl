from collections.abc import Sequence

from app.domain.entities.allocation import EthAddress
from app.ports.risk_model import RiskModel


class ModelRegistry:
    def __init__(self, models: Sequence[RiskModel]) -> None:
        self._models = tuple(models)

    def applicable(self, asset_id: int, prime_id: EthAddress) -> list[RiskModel]:
        return [model for model in self._models if model.applies_to(asset_id, prime_id)]
