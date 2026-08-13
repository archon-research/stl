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
                host="https://info.skyeco.com/required-risk-capital",
                access_model=SourceAccessModel.PUBLIC,
                role="Risk capital requirements and monitor metrics by star (reference/parity)",
                caveat="Kept for parity checks; no longer the source of the dashboard's risk-capital figures.",
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
