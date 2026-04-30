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

    name: str  # Display name (e.g., "Observatory")
    host: str  # Domain/host (e.g., "observatory.data.blockanalitica.com")
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
        return self._sources

    def get_source_by_host(self, host: str) -> Optional[DataSource]:
        """Look up source metadata by host domain."""
        for source in self._sources:
            if source.host.lower() == host.lower():
                return source
        return None

    def get_sources_by_role(self, role: str) -> list[DataSource]:
        """Get all sources providing a particular role/data type."""
        return [s for s in self._sources if role.lower() in s.role.lower()]

    def get_methodology_panel_text(self) -> str:
        """Return Markdown text for UI methodology/transparency panel."""
        external = [s for s in self._sources if "internal" not in s.role.lower()]

        lines = [
            "# Data Sources & Methodology",
            "",
            "## Internal Data (STL)",
            "- Onchain allocation positions from Ethereum mainnet",
            "- Risk calculations using Spark lending protocol parameters",
            "- Oracle prices from Chainlink and Pyth networks",
            "",
            "## Sources & Attribution",
            "",
        ]

        for source in external:
            caveat_note = f" — {source.caveat}" if source.caveat else ""
            lines.append(f"- **{source.name}** ({source.access_model.value}): {source.role}{caveat_note}")

        lines.extend(
            [
                "",
                "## Data Quality Notes",
                "- Prices may lag 5–10 minutes depending on oracle update frequency",
                "- Risk calculations are updated on each new block (Ethereum mainnet only)",
                "- Activity/event feed includes only Sparklend and Aave events; Morpho coverage pending",
                "",
                "**Last Updated**: See timestamp in each data response",
            ]
        )

        return "\n".join(lines)

    @staticmethod
    def _default_sources() -> list[DataSource]:
        """Default registry of known data sources."""
        return [
            # STL Internal
            DataSource(
                name="STL Allocation Index",
                host="localhost:8000 (internal)",
                access_model=SourceAccessModel.OPEN,
                role="Internal allocation snapshots, price feeds, risk calculations",
            ),
            # Observatory (Block Analitica)
            DataSource(
                name="Observatory",
                host="observatory.data.blockanalitica.com",
                access_model=SourceAccessModel.PUBLIC,
                role="Prime aggregates, allocation datasets, risk metrics (comparator reference)",
                caveat="Proprietary backend; data may differ from STL due to model assumptions",
            ),
            # Spark Data Layer
            DataSource(
                name="Spark Liquidity Layer",
                host="spark2-api.blockanalitica.com",
                access_model=SourceAccessModel.PUBLIC,
                role="Spark SLL AUM, assets, allocation activity (comparator reference)",
                caveat="Published by Block Analitica; includes forward projections",
            ),
            # Stablewatch
            DataSource(
                name="Stablewatch",
                host="stablewatch-slc-frontend-git-allocationwatch-stablewatch.vercel.app",
                access_model=SourceAccessModel.PUBLIC,
                role="Allocation watch UI and methodology links (comparator reference)",
                caveat="Methodology links include Dune dashboards (public, Dune free/paid tiers)",
            ),
            # SkyEco
            DataSource(
                name="SkyEco",
                host="skyeco-finacial.vercel.app",
                access_model=SourceAccessModel.PUBLIC,
                role="Prime capital stack, risk data (comparator reference)",
                caveat="Published by BA Labs; uses proprietary models",
            ),
            # Icons CDN
            DataSource(
                name="Icons CDN",
                host="icons.blockanalitica.com",
                access_model=SourceAccessModel.PUBLIC,
                role="Protocol and network logos (shared asset CDN across comparators)",
                caveat="License unclear; used across multiple apps",
                attribution_required=True,
            ),
            # On-chain Oracles
            DataSource(
                name="Chainlink Price Feeds",
                host="onchain (mainnet)",
                access_model=SourceAccessModel.OPEN,
                role="Token prices from oracle contracts",
            ),
            DataSource(
                name="Pyth Network",
                host="onchain + API",
                access_model=SourceAccessModel.OPEN,
                role="Multi-chain token prices and confidence intervals",
            ),
            # Public Explorers
            DataSource(
                name="Etherscan",
                host="etherscan.io",
                access_model=SourceAccessModel.PUBLIC,
                role="Address and transaction drill-down links",
            ),
            # Token Metadata
            DataSource(
                name="Trust Wallet Assets",
                host="github.com/trustwallet/assets",
                access_model=SourceAccessModel.OPEN,
                role="Token logos and metadata",
            ),
            DataSource(
                name="CoinGecko",
                host="api.coingecko.com",
                access_model=SourceAccessModel.PUBLIC,
                role="Token metadata and price fallback",
                caveat="Free tier rate-limited; paid tiers available",
            ),
            DataSource(
                name="DefiLlama",
                host="api.llama.fi",
                access_model=SourceAccessModel.PUBLIC,
                role="DeFi token prices and metadata",
            ),
        ]
