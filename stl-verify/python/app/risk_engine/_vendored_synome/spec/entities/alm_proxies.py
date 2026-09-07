"""ALM Proxy contract deployments per (Prime, Network).

The ALM Proxy is the contract authorised to allocate liquidity for a Sky Star
(Prime) on a given chain. Each deployment is defined as a ``Final`` constant
carrying its Atlas provenance via a ``:source_uuid:`` docstring marker; the
``AlmProxy`` enum at the bottom of the module aggregates the constants into
a closed-world set for graph traversal.

The set below is the union of (a) entries currently approved in the Sky
Atlas — each constant's ``:source_uuid:`` is the authoritative reference to
the Atlas document — and (b) entries used by downstream consumers that
maintain their own hard-coded allocation tracker config. Where the two
disagree the divergence is documented in the constant's docstring. The
Atlas is the governance source of truth; entries that exist only on the
consumer side are kept so that axis_synome remains a superset that those
consumers can pull from.
"""

from enum import Enum
from typing import Final

from app.risk_engine._vendored_synome.spec.entities.networks import Network
from app.risk_engine._vendored_synome.spec.entities.primes import PrimeAgent
from app.risk_engine._vendored_synome.spec_support.evm_address import EvmAddress
from app.risk_engine._vendored_synome.spec_support.validated_dataclass import validated_dataclass


@validated_dataclass
class AlmProxyDeployment:
    """A Sky Star's ALM Proxy contract deployment on a specific network."""

    prime: PrimeAgent
    network: Network
    address: EvmAddress


# --- Spark (Atlas ALM Contracts parent :source_uuid: 7db865de-8519-464b-8752-f39ecaf54fd2) ---

SPARK_ETHEREUM_MAINNET_ALM_PROXY: Final[AlmProxyDeployment] = AlmProxyDeployment(
    prime=PrimeAgent.SPARK,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0x1601843c5E9bC251A3272907010AFa41Fa18347E"),
)
"""ALM Proxy contract deployment for Spark on Ethereum Mainnet.

:source_uuid: a29a6751-4809-446c-a659-0dd93ca40379
"""


SPARK_BASE_ALM_PROXY: Final[AlmProxyDeployment] = AlmProxyDeployment(
    prime=PrimeAgent.SPARK,
    network=Network.BASE,
    address=EvmAddress("0x2917956eFF0B5eaF030abDB4EF4296DF775009cA"),
)
"""ALM Proxy contract deployment for Spark on Base.

:source_uuid: 425339ce-8e44-430b-ab8c-6c69f0b757e9
"""


SPARK_ARBITRUM_ALM_PROXY: Final[AlmProxyDeployment] = AlmProxyDeployment(
    prime=PrimeAgent.SPARK,
    network=Network.ARBITRUM,
    address=EvmAddress("0x92afd6F2385a90e44da3a8B60fe36f6cBe1D8709"),
)
"""ALM Proxy contract deployment for Spark on Arbitrum.

:source_uuid: c671b407-fcb2-48eb-8217-2ec156b581ad
"""


SPARK_UNICHAIN_ALM_PROXY: Final[AlmProxyDeployment] = AlmProxyDeployment(
    prime=PrimeAgent.SPARK,
    network=Network.UNICHAIN,
    address=EvmAddress("0x345E368fcCd62266B3f5F37C9a131FD1c39f5869"),
)
"""ALM Proxy contract deployment for Spark on Unichain.

:source_uuid: 6affe08d-0c1c-4cbf-a100-4a04c58220bb
"""


SPARK_OPTIMISM_ALM_PROXY: Final[AlmProxyDeployment] = AlmProxyDeployment(
    prime=PrimeAgent.SPARK,
    network=Network.OPTIMISM,
    address=EvmAddress("0x876664f0c9Ff24D1aa355Ce9f1680AE1A5bf36fB"),
)
"""ALM Proxy contract deployment for Spark on Optimism.

:source_uuid: f1895dfc-a18c-4009-bfd3-1c16c9a62092
"""


SPARK_AVALANCHE_ALM_PROXY: Final[AlmProxyDeployment] = AlmProxyDeployment(
    prime=PrimeAgent.SPARK,
    network=Network.AVALANCHE,
    address=EvmAddress("0xecE6B0E8a54c2f44e066fBb9234e7157B15b7FeC"),
)
"""ALM Proxy contract deployment for Spark on Avalanche.

Discrepancy: the Atlas document below records the address as ``TBD`` while
sentinel stl-verify config.go already uses the address encoded above. The
``:source_uuid:`` still points at the Atlas document so the entry remains
auditable once the Atlas value is filled in.

:source_uuid: 179f186a-079b-4663-b06c-b21f9dec85ca
"""


# --- Grove (Atlas ALM Contracts parent :source_uuid: f233a46b-8dff-4335-8ccf-dc3f1c18a96f) ---

GROVE_ETHEREUM_MAINNET_ALM_PROXY: Final[AlmProxyDeployment] = AlmProxyDeployment(
    prime=PrimeAgent.GROVE,
    network=Network.ETHEREUM_MAINNET,
    address=EvmAddress("0x491EDFB0B8b608044e227225C715981a30F3A44E"),
)
"""ALM Proxy contract deployment for Grove on Ethereum Mainnet.

:source_uuid: fda13ac2-b3ed-4b2a-9be6-9247632dafe3
"""


GROVE_AVALANCHE_ALM_PROXY: Final[AlmProxyDeployment] = AlmProxyDeployment(
    prime=PrimeAgent.GROVE,
    network=Network.AVALANCHE,
    address=EvmAddress("0x7107DD8F56642327945294a18A4280C78e153644"),
)
"""ALM Proxy contract deployment for Grove on Avalanche.

:source_uuid: 0704f4b5-ee5c-455c-932f-94591b8a6594
"""


GROVE_BASE_ALM_PROXY: Final[AlmProxyDeployment] = AlmProxyDeployment(
    prime=PrimeAgent.GROVE,
    network=Network.BASE,
    address=EvmAddress("0x9B746dBC5269e1DF6e4193Bcb441C0FbBF1CeCEe"),
)
"""ALM Proxy contract deployment for Grove on Base.

Discrepancy: sentinel stl-verify config.go does not currently include this
deployment; the address below is sourced from the Atlas document referenced
by ``:source_uuid:`` and should be added to the Go side.

:source_uuid: 5c382a94-ce36-4ffa-862b-4718382450fe
"""


GROVE_PLASMA_ALM_PROXY: Final[AlmProxyDeployment] = AlmProxyDeployment(
    prime=PrimeAgent.GROVE,
    network=Network.PLASMA,
    address=EvmAddress("0x0C462Fff7Cc975bC9F2B0aEB8270febA5FD71e1B"),
)
"""ALM Proxy contract deployment for Grove on Plasma.

Discrepancy: sentinel stl-verify config.go does not currently include this
deployment; the address below is sourced from the Atlas document referenced
by ``:source_uuid:`` and should be added to the Go side.

:source_uuid: 9d0bcc23-02d4-4389-9c85-707acf900dee
"""


GROVE_PLUME_ALM_PROXY: Final[AlmProxyDeployment] = AlmProxyDeployment(
    prime=PrimeAgent.GROVE,
    network=Network.PLUME,
    address=EvmAddress("0x1DB91ad50446a671e2231f77e00948E68876F812"),
)
"""ALM Proxy contract deployment for Grove on Plume.

:source_uuid: dcf0beac-b93e-41a7-b8b6-98c1d4cc819b
"""


GROVE_MONAD_ALM_PROXY: Final[AlmProxyDeployment] = AlmProxyDeployment(
    prime=PrimeAgent.GROVE,
    network=Network.MONAD,
    address=EvmAddress("0x94B398ACb2fcE988871218221EA6a4a2b26CcCbC"),
)
"""ALM Proxy contract deployment for Grove on Monad.

Discrepancy: no Atlas ALM Proxy Contract document exists for Grove on Monad
yet, so ``:source_uuid:`` is omitted. The address is sourced from sentinel
stl-verify config.go and should be backfilled in the Atlas.
"""


GROVE_ROBINHOOD_ALM_PROXY: Final[AlmProxyDeployment] = AlmProxyDeployment(
    prime=PrimeAgent.GROVE,
    network=Network.ROBINHOOD,
    address=EvmAddress("0x29626c2d8Ca49A51E4dECEEc5499e52983c42BD5"),
)
"""ALM Proxy contract deployment for Grove on Robinhood Chain.

Discrepancy: no Atlas ALM Proxy Contract document exists for Grove on
Robinhood yet, so ``:source_uuid:`` is omitted. The address was verified
on-chain (ARCT-374): the contract holds the Grove USDG position (~14.9M USDG)
and ~99.9% of the groveUSDG vault supply.
"""


class AlmProxy(Enum):
    """Closed-world set of all known ALM Proxy deployments.

    Each member's value is one of the ``Final[AlmProxyDeployment]`` constants
    defined above. The constants carry the canonical ``:source_uuid:`` markers
    used by the synome formula extractor; this enum exists so that downstream
    consumers can traverse the deployments as a closed set.
    """

    SPARK_ETHEREUM_MAINNET = SPARK_ETHEREUM_MAINNET_ALM_PROXY
    SPARK_BASE = SPARK_BASE_ALM_PROXY
    SPARK_ARBITRUM = SPARK_ARBITRUM_ALM_PROXY
    SPARK_UNICHAIN = SPARK_UNICHAIN_ALM_PROXY
    SPARK_OPTIMISM = SPARK_OPTIMISM_ALM_PROXY
    SPARK_AVALANCHE = SPARK_AVALANCHE_ALM_PROXY

    GROVE_ETHEREUM_MAINNET = GROVE_ETHEREUM_MAINNET_ALM_PROXY
    GROVE_AVALANCHE = GROVE_AVALANCHE_ALM_PROXY
    GROVE_BASE = GROVE_BASE_ALM_PROXY
    GROVE_PLASMA = GROVE_PLASMA_ALM_PROXY
    GROVE_PLUME = GROVE_PLUME_ALM_PROXY
    GROVE_MONAD = GROVE_MONAD_ALM_PROXY
    GROVE_ROBINHOOD = GROVE_ROBINHOOD_ALM_PROXY


STL_ADDITIONAL_PROXIES: Final[dict[tuple[PrimeAgent, Network], list[EvmAddress]]] = {
    (PrimeAgent.SPARK, Network.ETHEREUM_MAINNET): [
        EvmAddress("0x3300f198988e4C9C63F75dF86De36421f06af8c4"),
    ],
    (PrimeAgent.GROVE, Network.ETHEREUM_MAINNET): [
        EvmAddress("0x1369f7b2b38c76B6478c0f0E66D94923421891Ba"),
    ],
}
"""Additional ALM-controlled wallets that are NOT the canonical ALM Proxy but
must still be tracked by downstream consumers. The canonical ALM Proxy holds the
operational allocation positions; these SubProxy ("risk capital" / treasury)
wallets hold capital separately and are exported alongside the ALM Proxy for the
same (Prime, Network) rather than replacing it.

Atlas references: the Spark SubProxy is A.6.1.1.1.2.1.1.3.1.1.2 and the Grove
SubProxy is A.6.1.1.2.2.1.1.3.1.1.2."""
