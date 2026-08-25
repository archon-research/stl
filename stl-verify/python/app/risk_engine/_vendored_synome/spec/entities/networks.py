from enum import Enum


class Network(Enum):
    ETHEREUM_MAINNET = "Ethereum Mainnet"
    BASE = "Base"
    ARBITRUM = "Arbitrum"
    OPTIMISM = "Optimism"
    UNICHAIN = "Unichain"
    AVALANCHE = "Avalanche"
    PLASMA = "Plasma"
    PLUME = "Plume"
    MONAD = "Monad"


L2_PSM_NETWORKS = {
    Network.BASE,
    Network.ARBITRUM,
    Network.UNICHAIN,
    Network.OPTIMISM,
}
"""Networks on which an L2 PSM (PSM3) position qualifies as Resting ASC.

The Atlas enumerates these explicitly: Resting ASC includes "USDC in the PSM3
on Base, Arbitrum, Unichain, Optimism". The set is this closed list, not every
non-mainnet network — other chains a prime operates on (e.g. Avalanche,
Plasma, Plume, Monad) are deliberately not included until the Atlas lists them.

:source_uuid: 4e8cd2d1-4c74-49fd-b3fe-f8b6ccc1a79f
"""


# Chain labels consumed by STL compatibility checks.
STL_CHAIN_BY_NETWORK = {
    Network.ETHEREUM_MAINNET: "mainnet",
    Network.BASE: "base",
    Network.ARBITRUM: "arbitrum",
    Network.OPTIMISM: "optimism",
    Network.UNICHAIN: "unichain",
    Network.AVALANCHE: "avalanche-c",
    Network.PLASMA: "plasma",
    Network.PLUME: "plume",
    Network.MONAD: "monad",
}
