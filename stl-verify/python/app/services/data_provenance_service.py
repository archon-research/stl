"""Data provenance and source metadata management for transparency."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class SourceAccessModel(str, Enum):
    """Classification of data source accessibility and terms of use."""

    OPEN = "open"  # Publicly accessible, open usage terms
    PUBLIC = "public"  # Publicly accessible, proprietary/restricted terms
    CLOSED = "closed"  # Proprietary/private backend


@dataclass(frozen=True)
class DataSource:
    """Metadata for a data source or API host."""

    name: str  # Display name (e.g., "STL Allocation Index")
    host: str  # Domain/host (e.g., "stl.archonapi.com")
    access_model: SourceAccessModel
    role: str  # What data does it provide (e.g., "prime allocations", "protocol icons")
    caveat: Optional[str] = None  # License/usage restrictions or data quality notes
    attribution_required: bool = False


class DataProvenanceService:
    """Maintains registry of data sources and provides transparency metadata for API responses."""

    def __init__(self) -> None:
        """Initialize with known sources across STL and comparator apps."""
        self._sources = self._default_sources()

    def get_sources(self) -> list[DataSource]:
        """Return all registered data sources."""
        return self._sources.copy()

    def get_source_by_host(self, host: str) -> Optional[DataSource]:
        """Look up source metadata by host domain."""
        for source in self._sources:
            if source.host.lower() == host.lower():
                return source
        return None

    def get_sources_by_role(self, role: str) -> list[DataSource]:
        """Get all sources providing a particular role/data type."""
        return [s for s in self._sources if role.lower() in s.role.lower()]

    @staticmethod
    def _default_sources() -> list[DataSource]:
        """Registry of data sources used by STL."""
        return [
            # STL Internal
            DataSource(
                name="STL Allocation Index",
                host="Same app (internal API)",
                access_model=SourceAccessModel.CLOSED,
                role="Internal allocation snapshots, price feeds, risk calculations",
                caveat="Internal-only backend",
            ),
            # On-chain Oracles
            DataSource(
                name="Chainlink Price Feeds",
                host="onchain (mainnet)",
                access_model=SourceAccessModel.OPEN,
                role="Token oracle prices from onchain contracts",
            ),
            DataSource(
                name="Pyth Network",
                host="onchain + API",
                access_model=SourceAccessModel.OPEN,
                role="Multi-chain token oracle prices and confidence intervals",
            ),
            DataSource(
                name="Self-computed Risk Capital (gap_sweep)",
                host="onchain + model",
                access_model=SourceAccessModel.OPEN,
                role=(
                    "Required and Total Risk Capital and encumbrance shown on the dashboard, "
                    "computed from on-chain allocations (gap_sweep stress) and the on-chain "
                    "SubProxy treasury"
                ),
                caveat="Model-derived and partial; covers on-chain lending positions only.",
            ),
            DataSource(
                name="Star Agents Risk Capital & Requirements Monitor",
                host="https://info-sky.blockanalitica.com/star-monitoring/risk-capital",
                access_model=SourceAccessModel.PUBLIC,
                role=(
                    "Risk capital requirements, the per-allocation breakdown behind them, and monitor "
                    "metrics by star (the `reference` provenance)"
                ),
                caveat=(
                    "Not the source of the dashboard's own risk-capital figures. Read by STL's "
                    "reference-capital indexer every 15 minutes rather than per request, so a served "
                    "figure is as of the last cycle and carries the time it was observed. The monitor "
                    "publishes no history, so figures only exist from the indexer's first cycle "
                    "forward."
                ),
            ),
            DataSource(
                name="Sky Internal Balance-Sheet Feed",
                host="https://sky.data.blockanalitica.com/internal",
                access_model=SourceAccessModel.PUBLIC,
                role=(
                    "Per-position balance sheets and per-prime daily aggregates by star, behind the "
                    "`reference` provenance on the allocation list, total capital and debt"
                ),
                caveat=(
                    "A different host and a different question from the monitor above: the balance "
                    "sheet (every position, summing to the prime's assets), not the priced "
                    "risk-bearing subset. Read by the reference-capital indexer every 15 minutes "
                    "rather than per request; its daily aggregates are also seeded backwards by the "
                    "reference-capital backfill, which is the only source of reference history."
                ),
            ),
            # Off-chain custody
            DataSource(
                name="Anchorage Custody API",
                host="closed backend (Anchorage Digital)",
                access_model=SourceAccessModel.CLOSED,
                role="Off-chain BTC custody package snapshots (collateral, loan exposure, LTV)",
                caveat=(
                    "Polled every 15 minutes; surfaced with the snapshot's own timestamp so a "
                    "frozen upstream feed reads as honestly stale rather than current."
                ),
            ),
        ]
