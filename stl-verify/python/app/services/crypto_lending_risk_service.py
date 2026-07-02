from collections.abc import Collection, Mapping
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Any

from app.domain.entities.allocation import EthAddress
from app.domain.entities.backed_breakdown import BackedBreakdown, CollateralContribution
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.risk import (
    GapSweepDetails,
    LiquidationParams,
    ModelName,
    RiskBreakdown,
    RiskEnrichedCollateral,
    RrcResult,
)
from app.domain.exceptions import InvalidOverrideError
from app.logging import get_logger
from app.ports.crypto_lending_reader import CryptoLendingReader
from app.risk_engine.crypto_lending import gap_sweep

logger = get_logger(__name__)

_ALLOWED_OVERRIDES = frozenset({"gap_pct"})
# Reject pathological input strings *before* `Decimal(str(raw))` — parsing a
# multi-megabyte numeric literal is a CPU-burn vector even though the value
# would later fail the [0, 1] range check.
_DECIMAL_STR_MAX_LEN = 64
# Quantize RRC to USD cents at the service boundary so clients get a
# bounded-precision number instead of float-noise tails from gap_sweep math.
_USD_CENT = Decimal("0.01")
_HUNDRED = Decimal("100")
_ONE = Decimal("1")


class CryptoLendingRiskService:
    """RiskModel for crypto-lending assets using the gap-sweep stress."""

    risk_model: ModelName = "gap_sweep"

    def __init__(
        self,
        reader: CryptoLendingReader,
        default_gap_pct: Decimal,
        supported_asset_ids: Collection[int],
    ) -> None:
        self._reader = reader
        self._default_gap_pct = default_gap_pct
        self._supported_asset_ids = frozenset(supported_asset_ids)

    # ------------------------------------------------------------------
    # RiskModel interface implementation
    # ------------------------------------------------------------------

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:  # noqa: ARG002
        return asset_id in self._supported_asset_ids

    async def compute(
        self,
        asset_id: int,
        prime_id: EthAddress,
        overrides: Mapping[str, Any],
    ) -> RrcResult:
        """Compute the RRC for the given asset and prime via gap-sweep."""
        if not self.applies_to(asset_id, prime_id):
            raise ValueError(f"unsupported asset_id={asset_id}")

        gap_pct = self._resolve_gap_pct(overrides)
        items = await self._load_enriched_items(receipt_token_id=asset_id, prime_id=prime_id)
        raw = gap_sweep.total_bad_debt(items, gap_pct)
        rrc_usd = abs(raw).quantize(_USD_CENT, rounding=ROUND_HALF_EVEN)
        # Use the *protocol's own* collateral USD as the basis: gap-sweep
        # already loads each collateral item priced via the protocol-specific
        # reader (Aave/Morpho on-chain reads). This is the same fidelity
        # ``protocol_oracle`` would give us but goes through a code path with
        # broader coverage — ``allocation_repo.get_usd_exposure`` joins the
        # indexer's ``protocol_oracle`` table, which is empty for several
        # protocols today.
        position_usd = sum((item.amount_usd for item in items), Decimal("0")).quantize(
            _USD_CENT, rounding=ROUND_HALF_EVEN
        )
        return RrcResult(
            asset_id=asset_id,
            prime_id=prime_id,
            rrc_usd=rrc_usd,
            comparable_crr_pct=self._compute_comparable_crr_pct(rrc_usd, position_usd),
            risk_model=self.risk_model,
            details=GapSweepDetails(risk_model="gap_sweep", gap_pct=gap_pct, loss_usd=rrc_usd),
        )

    def _resolve_gap_pct(self, overrides: Mapping[str, Any]) -> Decimal:
        """Extract and validate gap_pct from overrides, falling back to the default.

        ``gap_pct`` is a price-drop fraction on collateral, ``0`` ≤ gap ≤ ``1``.
        Coerces from any numeric or string input via ``Decimal(str(raw))``
        because JSON deserialisation hands us ``str``/``int``/``float`` and a
        bare ``Decimal <= str`` comparison would raise ``TypeError`` → 500.
        """
        unknown = set(overrides) - _ALLOWED_OVERRIDES
        if unknown:
            raise InvalidOverrideError(f"unknown override keys: {sorted(unknown)}")

        if "gap_pct" not in overrides:
            return self._default_gap_pct

        raw = overrides["gap_pct"]
        if raw is None:
            raise InvalidOverrideError("invalid gap_pct: expected a finite number in [0, 1], got None")
        if isinstance(raw, str) and len(raw) > _DECIMAL_STR_MAX_LEN:
            raise InvalidOverrideError(f"invalid gap_pct: input string too long ({len(raw)} > {_DECIMAL_STR_MAX_LEN})")
        try:
            gap_pct = raw if isinstance(raw, Decimal) else Decimal(str(raw))
        except Exception as exc:
            raise InvalidOverrideError(f"invalid gap_pct: expected a finite number in [0, 1], got {raw!r}") from exc
        if not gap_pct.is_finite():
            raise InvalidOverrideError(f"invalid gap_pct: expected a finite number in [0, 1], got {gap_pct}")
        if not (Decimal("0") <= gap_pct <= Decimal("1")):
            raise InvalidOverrideError(f"gap_pct must be in [0, 1], got {gap_pct}")

        return gap_pct

    @staticmethod
    def _compute_comparable_crr_pct(rrc_usd: Decimal, usd_exposure: Decimal) -> Decimal:
        # Empty/zero-exposure positions naturally have zero implied CRR;
        # gap-sweep would also have produced zero rrc_usd in that case.
        if usd_exposure <= Decimal("0"):
            return Decimal("0").quantize(_USD_CENT)
        return (rrc_usd / usd_exposure * _HUNDRED).quantize(_USD_CENT, rounding=ROUND_HALF_EVEN)

    async def _load_enriched_items(self, receipt_token_id: int, prime_id: EthAddress) -> list[RiskEnrichedCollateral]:
        info = await self._reader.get_receipt_token(receipt_token_id)
        if info is None:
            raise ValueError(f"receipt token not found: {receipt_token_id}")
        _, items = await self._load_enriched_items_for_info(info, prime_id=prime_id)
        return items

    # ------------------------------------------------------------------
    # Legacy public API — used by old endpoints, will be removed in VEC-183.
    # ------------------------------------------------------------------

    async def get_risk_breakdown(
        self,
        receipt_token_id: int,
        prime_id: EthAddress | None,
    ) -> RiskBreakdown | None:
        """Return the risk breakdown for a receipt token, or ``None`` if unknown.

        With a ``prime_id`` the breakdown is scaled to that prime's position (per-prime,
        pro-rata by pool share); with ``prime_id=None`` it is the pool-level breakdown.
        """
        resolved = await self._load_enriched_items_or_none(receipt_token_id, prime_id=prime_id)
        if resolved is None:
            return None
        backed_asset_id, items = resolved
        return RiskBreakdown(backed_asset_id=backed_asset_id, items=tuple(items))

    async def get_bad_debt_legacy(self, receipt_token_id: int, gap_pct: Decimal) -> Decimal | None:
        """Return legacy bad debt for a receipt token, or ``None`` if unknown."""
        resolved = await self._load_enriched_items_or_none(receipt_token_id, prime_id=None)
        if resolved is None:
            return None
        _, items = resolved
        raw = gap_sweep.total_bad_debt(items, gap_pct)
        return abs(raw)

    async def _load_enriched_items_or_none(
        self,
        receipt_token_id: int,
        prime_id: EthAddress | None,
    ) -> tuple[int, list[RiskEnrichedCollateral]] | None:
        info = await self._reader.get_receipt_token(receipt_token_id)
        if info is None:
            return None
        return await self._load_enriched_items_for_info(info, prime_id=prime_id)

    async def _load_enriched_items_for_info(
        self,
        info: ReceiptTokenInfo,
        prime_id: EthAddress | None,
    ) -> tuple[int, list[RiskEnrichedCollateral]]:
        if not self._reader.requires_liquidation_enrichment(info):
            # Pool-level, USD-valued, symbol-keyed breakdown (e.g. Maple Syrup): no
            # per-asset liquidation params to enrich with. When a prime_id is given,
            # scale each asset by the prime's pool share (pro-rata, pari-passu); with
            # no prime_id keep the pool-level view (share = 1). An empty breakdown is
            # the graceful "no data yet" signal, so skip the share lookup — Maple has
            # no warm-up concept and a prime-not-in-pool share error must not mask it.
            breakdown = await self._reader.get_breakdown(info)
            if not breakdown.items:
                return breakdown.backed_asset_id, []
            share = await self._reader.get_share(info, prime_id) if prime_id is not None else _ONE
            return breakdown.backed_asset_id, self._build_unenriched_items(breakdown, share)

        # Legacy endpoints must validate share availability before returning an
        # empty breakdown so warm-up windows still surface as
        # ``503 share_data_missing`` instead of ``200``.
        if prime_id is None:
            share = await self._reader.get_legacy_share(info)

        breakdown = await self._reader.get_breakdown(info)
        if not breakdown.items:
            return breakdown.backed_asset_id, []

        if prime_id is not None:
            share = await self._reader.get_share(info, prime_id)

        token_ids = [item.token_id for item in breakdown.items if item.token_id is not None]
        liq_params = await self._reader.get_liquidation_params(info, breakdown.backed_asset_id, token_ids)
        return breakdown.backed_asset_id, self._build_enriched_items(breakdown, share, liq_params)

    @staticmethod
    def _build_unenriched_items(breakdown: BackedBreakdown, share: Decimal) -> list[RiskEnrichedCollateral]:
        # Pre-priced, symbol-keyed collateral (token_id is None) with no liquidation
        # params — e.g. Maple Syrup. Scale each asset's USD value (and derived token
        # amount) by the prime's pool ``share`` for a per-prime, pro-rata view; ``share``
        # is 1 for the pool-level (no-prime) case. ``backing_pct`` is a pool property, so
        # it is identical for every prime and left unscaled.
        enriched: list[RiskEnrichedCollateral] = []
        for item in breakdown.items:
            price = item.price_usd
            if not price:
                logger.warning(
                    "Unenriched collateral backed_asset_id=%d symbol=%s has missing or zero price; "
                    "emitting amount=0 and price_usd=null (amount_usd preserved)",
                    breakdown.backed_asset_id,
                    item.symbol,
                )
            scaled_backing_value = item.backing_value * share
            # Surface the missing price as null rather than masking it with 0: a null
            # price is a machine-detectable "unpriced" signal, whereas a 0 would read
            # as a real zero price and silently break amount × price == amount_usd.
            amount = (scaled_backing_value / price) if price else Decimal("0")
            enriched.append(
                RiskEnrichedCollateral(
                    token_id=item.token_id,
                    symbol=item.symbol,
                    amount=amount,
                    backing_pct=item.backing_pct,
                    amount_usd=scaled_backing_value,
                    price_usd=price if price else None,
                    liquidation_threshold=None,
                    liquidation_bonus=None,
                )
            )
        return enriched

    def _build_enriched_items(
        self,
        breakdown: BackedBreakdown,
        share: Decimal,
        liq_params: dict[int, LiquidationParams],
    ) -> list[RiskEnrichedCollateral]:
        enriched: list[RiskEnrichedCollateral] = []
        for item in breakdown.items:
            if item.token_id not in liq_params:
                logger.warning(
                    "Dropping collateral item backed_asset_id=%d token_id=%d symbol=%s: missing liquidation params",
                    breakdown.backed_asset_id,
                    item.token_id,
                    item.symbol,
                )
                continue
            if item.price_usd is None or item.price_usd == 0:
                logger.warning(
                    "Dropping collateral item backed_asset_id=%d token_id=%d symbol=%s: missing or zero price",
                    breakdown.backed_asset_id,
                    item.token_id,
                    item.symbol,
                )
                continue
            enriched.append(self._enrich(item, share, item.price_usd, liq_params[item.token_id]))
        return enriched

    @staticmethod
    def _enrich(
        item: CollateralContribution,
        share: Decimal,
        price_usd: Decimal,
        params: LiquidationParams,
    ) -> RiskEnrichedCollateral:
        scaled_backing_value = item.backing_value * share
        return RiskEnrichedCollateral(
            token_id=item.token_id,
            symbol=item.symbol,
            amount=scaled_backing_value / price_usd,
            backing_pct=item.backing_pct,
            amount_usd=scaled_backing_value,
            price_usd=price_usd,
            liquidation_threshold=params.liquidation_threshold,
            liquidation_bonus=params.liquidation_bonus,
        )
