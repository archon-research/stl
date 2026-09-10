from enum import Enum
from typing import Annotated

from pydantic import Field

from app.risk_engine._vendored_synome.spec.entities.assets import Asset
from app.risk_engine._vendored_synome.spec.entities.assets_by_prime import ASSETS_BY_PRIME, PrimeName
from app.risk_engine._vendored_synome.spec.entities.networks import Network
from app.risk_engine._vendored_synome.spec.entities.protocol_sets import Protocol
from app.risk_engine._vendored_synome.spec_support.validated_dataclass import validated_dataclass


@validated_dataclass
class PrimeAgentData:
    """A Sky Star Agent (prime) that controls a stablecoin system."""

    name: Annotated[str, Field(min_length=1)]  # e.g., "Spark"
    assets: list[Asset]

    psm_protocols: set[Protocol]
    networks: set[Network]
    alm_proxy: Protocol | None = None
    allocation_buffer_protocol: Protocol | None = None
    asc_exempt: bool = False
    """Whether this prime is exempt from the Minimum ASC requirement.

    Set to True for Keel in the near term due to Solana infrastructure
    limitations; expected to be removed in a future iteration of the Asset
    Liability Management framework.

    :source_uuid: 864611dd-38cd-493e-b594-a85610a9c63e
    """


class PrimeAgent(Enum):
    """All Sky Star Agents (primes)."""

    SPARK = PrimeAgentData(
        name=PrimeName.SPARK,
        assets=ASSETS_BY_PRIME.get(PrimeName.SPARK, []),
        psm_protocols={Protocol.LITE_PSM, Protocol.PSM3},
        networks={
            Network.ARBITRUM,
            Network.AVALANCHE,
            Network.BASE,
            Network.ETHEREUM_MAINNET,
            Network.OPTIMISM,
            Network.UNICHAIN,
        },
    )
    GROVE = PrimeAgentData(
        name=PrimeName.GROVE,
        assets=ASSETS_BY_PRIME.get(PrimeName.GROVE, []),
        psm_protocols={Protocol.PSM3},
        networks={
            Network.ARBITRUM,
            Network.AVALANCHE,
            Network.BASE,
            Network.ETHEREUM_MAINNET,
            Network.PLASMA,
            Network.PLUME,
            Network.MONAD,
        },
    )
    KEEL = PrimeAgentData(name=PrimeName.KEEL, asc_exempt=True, assets=[], psm_protocols=set(), networks=set())
    SKYBASE = PrimeAgentData(
        name=PrimeName.SKYBASE,
        assets=ASSETS_BY_PRIME.get(PrimeName.SKYBASE, []),
        psm_protocols=set(),
        networks=set(),
    )
    PATTERN = PrimeAgentData(
        name=PrimeName.PATTERN,
        assets=ASSETS_BY_PRIME.get(PrimeName.PATTERN, []),
        psm_protocols=set(),
        networks=set(),
    )
    OSERO = PrimeAgentData(
        name=PrimeName.OSERO,
        assets=ASSETS_BY_PRIME.get(PrimeName.OSERO, []),
        psm_protocols=set(),
        networks=set(),
    )
    LAUNCH_AGENT_7 = PrimeAgentData(
        name=PrimeName.LAUNCH_AGENT_7,
        assets=ASSETS_BY_PRIME.get(PrimeName.LAUNCH_AGENT_7, []),
        psm_protocols=set(),
        networks=set(),
    )


class EligiblePrimeAgentASC(Enum):
    """Enumeration of prime agents eligible for ASC calculations.

    Excludes primes that are ASC-exempt (e.g., Keel).

    :source_uuid: 864611dd-38cd-493e-b594-a85610a9c63e
    """

    SPARK = PrimeAgent.SPARK.value
    GROVE = PrimeAgent.GROVE.value
    SKYBASE = PrimeAgent.SKYBASE.value
    PATTERN = PrimeAgent.PATTERN.value
    OSERO = PrimeAgent.OSERO.value
    LAUNCH_AGENT_7 = PrimeAgent.LAUNCH_AGENT_7.value
