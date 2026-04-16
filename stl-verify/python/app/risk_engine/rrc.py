"""RRC computation for portfolio (GET) and scenario (POST) paths.

Pure functions over (breakdown or asset, mapping, ratings). No I/O, no
service dependencies — the endpoint layer composes these with the
existing risk service or caller-supplied inputs.

``final_crr_pct`` is ``max(m.crr_pct for m in models)`` — the cross-model
aggregation contract. With a single model (MVP), this equals that model's
weighted-average CRR over its modeled exposure. When a second model lands,
we'll need to reconsider how ``final_rrc_usd`` is defined across models
that cover different subjects.
"""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from app.domain.entities.risk import RiskBreakdown
from app.risk_engine.suraf.result import SurafResult

_HUNDRED = Decimal("100")


class RrcModelEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    source_commit_sha: str
    modeled_exposure_usd: Decimal
    crr_pct: Decimal
    rrc_usd: Decimal


class RrcItem(BaseModel):
    """Per-collateral contribution. ``rating_id``/``crr_pct``/``rrc_usd``
    are ``None`` when no model applies — surfaces coverage gaps so they
    can't hide as zero-risk in the weighted aggregate."""

    model_config = ConfigDict(frozen=True)

    token_id: int
    symbol: str
    amount_usd: Decimal
    rating_id: str | None
    crr_pct: Decimal | None
    rrc_usd: Decimal | None


class PortfolioRrcResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    total_exposure_usd: Decimal
    modeled_exposure_usd: Decimal
    final_crr_pct: Decimal | None
    final_rrc_usd: Decimal | None
    models: list[RrcModelEntry]
    items: list[RrcItem]


class ScenarioRrcResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    asset: str
    usd_exposure: Decimal
    final_crr_pct: Decimal
    final_rrc_usd: Decimal
    models: list[RrcModelEntry]


def compute_portfolio_rrc(
    breakdown: RiskBreakdown,
    asset_to_rating: dict[str, str],
    suraf_ratings: dict[str, SurafResult],
) -> PortfolioRrcResult:
    items: list[RrcItem] = []
    modeled_exposure = Decimal("0")
    suraf_rrc = Decimal("0")
    suraf_sha: str | None = None

    for c in breakdown.items:
        rating_id = asset_to_rating.get(c.symbol)
        if rating_id is None:
            items.append(
                RrcItem(
                    token_id=c.token_id,
                    symbol=c.symbol,
                    amount_usd=c.amount_usd,
                    rating_id=None,
                    crr_pct=None,
                    rrc_usd=None,
                )
            )
            continue
        rating = suraf_ratings[rating_id]
        rrc = c.amount_usd * rating.crr_pct / _HUNDRED
        items.append(
            RrcItem(
                token_id=c.token_id,
                symbol=c.symbol,
                amount_usd=c.amount_usd,
                rating_id=rating_id,
                crr_pct=rating.crr_pct,
                rrc_usd=rrc,
            )
        )
        modeled_exposure += c.amount_usd
        suraf_rrc += rrc
        suraf_sha = rating.source_commit_sha

    total_exposure = sum((c.amount_usd for c in breakdown.items), Decimal("0"))

    if modeled_exposure == 0:
        return PortfolioRrcResult(
            total_exposure_usd=total_exposure,
            modeled_exposure_usd=Decimal("0"),
            final_crr_pct=None,
            final_rrc_usd=None,
            models=[],
            items=items,
        )

    suraf_crr = suraf_rrc / modeled_exposure * _HUNDRED
    assert suraf_sha is not None  # modeled_exposure > 0 implies at least one mapped item
    models = [
        RrcModelEntry(
            name="suraf",
            source_commit_sha=suraf_sha,
            modeled_exposure_usd=modeled_exposure,
            crr_pct=suraf_crr,
            rrc_usd=suraf_rrc,
        )
    ]
    final_crr = max(m.crr_pct for m in models)
    final_rrc = next(m.rrc_usd for m in models if m.crr_pct == final_crr)

    return PortfolioRrcResult(
        total_exposure_usd=total_exposure,
        modeled_exposure_usd=modeled_exposure,
        final_crr_pct=final_crr,
        final_rrc_usd=final_rrc,
        models=models,
        items=items,
    )


def compute_scenario_rrc(
    asset: str,
    usd_exposure: Decimal,
    asset_to_rating: dict[str, str],
    suraf_ratings: dict[str, SurafResult],
) -> ScenarioRrcResult | None:
    """Return ``None`` if no rating is mapped for ``asset``."""
    rating_id = asset_to_rating.get(asset)
    if rating_id is None:
        return None
    rating = suraf_ratings[rating_id]
    rrc = usd_exposure * rating.crr_pct / _HUNDRED
    entry = RrcModelEntry(
        name="suraf",
        source_commit_sha=rating.source_commit_sha,
        modeled_exposure_usd=usd_exposure,
        crr_pct=rating.crr_pct,
        rrc_usd=rrc,
    )
    return ScenarioRrcResult(
        asset=asset,
        usd_exposure=usd_exposure,
        final_crr_pct=rating.crr_pct,
        final_rrc_usd=rrc,
        models=[entry],
    )
