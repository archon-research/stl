from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.risk_engine._vendored_synome.spec.entities.alm_proxies import (
    STL_ADDITIONAL_PROXIES,
    AlmProxy,
)
from app.risk_engine._vendored_synome.spec.entities.assets_by_prime import ASSETS_BY_PRIME, PrimeName
from app.risk_engine._vendored_synome.spec.entities.assets_missing_from_atlas import (
    MISSING_FROM_ATLAS_BY_PRIME,
)
from app.risk_engine._vendored_synome.spec.entities.networks import STL_CHAIN_BY_NETWORK
from app.risk_engine._vendored_synome.spec.entities.protocol_sets import (
    ATOKEN_PROTOCOLS,
    ERC4626_PROTOCOLS,
    L1_PSM_PROTOCOLS,
    L2_PSM_PROTOCOLS,
    UNISWAP_STYLE_PROTOCOLS,
    AllocationType,
    Protocol,
    TokenType,
)

# Proxy roles. "alm" is the canonical ALM Proxy holding operational allocation
# positions; "subproxy" is an additional SubProxy/treasury wallet tracked for the
# same (star, chain).
PROXY_ROLE_ALM = "alm"
PROXY_ROLE_SUBPROXY = "subproxy"


class ProxyEntry(BaseModel):
    """A single ALM-controlled wallet exported for a (star, chain).

    More than one may exist per (star, chain): the canonical ALM Proxy plus any
    additional SubProxy/treasury wallets. ``role`` lets consumers tell them apart
    without relying on list position.
    """

    model_config = ConfigDict(extra="forbid")

    star: str
    chain: str
    address: str
    role: str


class TokenEntryModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_address: str
    wallet_address: str
    asset_address: str | None
    star: str
    chain: str
    protocol: str
    allocation_type: AllocationType
    token_type: TokenType


class AssetsByPrimeContainer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # star -> list of token entries.
    entries: dict[str, list[TokenEntryModel]] = Field(serialization_alias="ASSETS_BY_PRIME")


class AlmProxiesContainer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # star -> chain -> list of proxies.
    proxies: dict[str, dict[str, list[ProxyEntry]]] = Field(serialization_alias="AlmProxy")


class EntitiesModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    assets_by_prime: AssetsByPrimeContainer
    alm_proxies: AlmProxiesContainer


class SpecModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entities: EntitiesModel


class AxisSynomeModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    spec: SpecModel


class AxisSynomeContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    axis_synome_git_commit: str
    axis_synome: AxisSynomeModel


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def _prime_to_star(prime: PrimeName | str) -> str:
    text = prime.value if isinstance(prime, PrimeName) else str(prime)
    return text.lower().replace(" ", "-")


def _token_type(protocol: Protocol | None, chain: str, token_name: str) -> TokenType:
    if protocol in ATOKEN_PROTOCOLS:
        return TokenType.ATOKEN

    if protocol in ERC4626_PROTOCOLS:
        return TokenType.ERC4626

    if protocol == Protocol.CURVE:
        return TokenType.CURVE

    if protocol in UNISWAP_STYLE_PROTOCOLS:
        if "LP" in token_name.upper():
            return TokenType.UNI_V3_LP
        return TokenType.UNI_V3_POOL

    if protocol == Protocol.CENTRIFUGE:
        if chain == "plume":
            return TokenType.CENTRIFUGE_FEEDER
        return TokenType.CENTRIFUGE

    if protocol == Protocol.ANCHORAGE:
        return TokenType.ANCHORAGE

    if protocol in L1_PSM_PROTOCOLS | L2_PSM_PROTOCOLS:
        return TokenType.PSM3

    return TokenType.ERC20


def _allocation_type(protocol: Protocol | None) -> AllocationType:
    if protocol in L1_PSM_PROTOCOLS | L2_PSM_PROTOCOLS:
        return AllocationType.PSM3
    return AllocationType.ALLOCATION


# Vendoring baseline: the last published axis-synome wheel and the upstream
# commit it was built from. The source repo is deprecated; changes now land
# directly in this vendored copy, tracked by stl's own git history.
VENDORED_WHEEL_VERSION = "0.2.0.dev202607240944"
VENDORED_UPSTREAM_COMMIT = "d136e461f177cad555e0c5fc3f497fc37df9dc5f"


def build_axis_synome_contract(
    version: str | None = None,
    axis_synome_git_commit: str | None = None,
) -> AxisSynomeContract:
    # Canonical ALM Proxy per (star, chain). This is the wallet that holds the
    # operational allocation positions, and the wallet that every token entry on
    # that (star, chain) binds to.
    alm_proxy_by_key: dict[tuple[str, str], str] = {}
    for member in AlmProxy:
        deployment = member.value
        chain = STL_CHAIN_BY_NETWORK.get(deployment.network)
        if chain is None:
            continue

        star = _prime_to_star(deployment.prime.name)
        alm_proxy_by_key[(star, chain)] = str(deployment.address).lower()

    # All wallets to export per (star, chain): the canonical ALM Proxy first,
    # then any additional SubProxy/treasury wallets. Additional wallets are
    # appended rather than replacing the ALM Proxy, so both stay tracked.
    # Value is a list of (address, role) so consumers can distinguish the
    # canonical ALM proxy from additional SubProxy wallets.
    proxies_by_key: dict[tuple[str, str], list[tuple[str, str]]] = {
        key: [(address, PROXY_ROLE_ALM)] for key, address in alm_proxy_by_key.items()
    }
    for (prime, network), addresses in STL_ADDITIONAL_PROXIES.items():
        chain = STL_CHAIN_BY_NETWORK.get(network)
        if chain is None:
            continue

        star = _prime_to_star(prime.name)
        for address in addresses:
            address = str(address).lower()
            existing = proxies_by_key.setdefault((star, chain), [])
            if any(addr == address for addr, _ in existing):
                raise ValueError(
                    f"additional proxy {address} for {star}/{chain} duplicates a "
                    "canonical ALM proxy; remove it from STL_ADDITIONAL_PROXIES"
                )
            existing.append((address, PROXY_ROLE_SUBPROXY))

    entries: list[dict[str, Any]] = []
    seen_entry_keys: set[tuple[str, str, str]] = set()
    missing_proxy_keys: set[tuple[str, str]] = set()
    for prime_name, assets in ASSETS_BY_PRIME.items():
        star = _prime_to_star(prime_name)
        if star not in {"spark", "grove"}:
            continue

        for asset in assets:
            chain = STL_CHAIN_BY_NETWORK.get(asset.network)
            if chain is None:
                continue

            wallet = alm_proxy_by_key.get((star, chain))
            if wallet is None:
                # Assets exist for this (star, chain) but no ALM proxy is
                # defined, so they would silently vanish from the export. Fail
                # loudly instead, mirroring the duplicate-entry guard below.
                missing_proxy_keys.add((star, chain))
                continue

            contract_address = str(asset.address).lower()
            entry_key = (chain, contract_address, wallet)
            if entry_key in seen_entry_keys:
                raise ValueError(
                    "duplicate token entry for "
                    f"chain={chain} contract={contract_address} wallet={wallet}; "
                    "resolve the conflicting Asset definitions in assets_by_prime"
                )
            seen_entry_keys.add(entry_key)

            entries.append(
                {
                    "contract_address": contract_address,
                    "wallet_address": wallet,
                    "asset_address": str(asset.underlying_asset_address).lower(),
                    "star": star,
                    "chain": chain,
                    "protocol": _slug(asset.protocol.value) if asset.protocol is not None else "",
                    "allocation_type": _allocation_type(asset.protocol),
                    "token_type": _token_type(asset.protocol, chain, asset.token.value),
                }
            )

    # STL positions with no Atlas backing. These bind to the same ALM Proxy and
    # share the duplicate / missing-proxy guards above, but carry their STL
    # protocol, allocation_type and token_type verbatim rather than deriving
    # them — the legacy pol/asset/proxy values are not a function of protocol.
    for prime_name, missing_assets in MISSING_FROM_ATLAS_BY_PRIME.items():
        star = _prime_to_star(prime_name)
        if star not in {"spark", "grove"}:
            continue

        for missing in missing_assets:
            chain = STL_CHAIN_BY_NETWORK.get(missing.network)
            if chain is None:
                continue

            # risk_capital holdings bind to an explicit SubProxy wallet; all
            # other rows bind to the canonical ALM Proxy for the (star, chain).
            if missing.wallet_address is not None:
                wallet = str(missing.wallet_address).lower()
            else:
                wallet = alm_proxy_by_key.get((star, chain))
                if wallet is None:
                    missing_proxy_keys.add((star, chain))
                    continue

            contract_address = str(missing.address).lower()
            entry_key = (chain, contract_address, wallet)
            if entry_key in seen_entry_keys:
                raise ValueError(
                    "duplicate token entry for "
                    f"chain={chain} contract={contract_address} wallet={wallet}; "
                    "resolve the conflicting Asset definitions in assets_by_prime "
                    "and assets_missing_from_atlas"
                )
            seen_entry_keys.add(entry_key)

            entries.append(
                {
                    "contract_address": contract_address,
                    "wallet_address": wallet,
                    "asset_address": (
                        str(missing.underlying_asset_address).lower()
                        if missing.underlying_asset_address is not None
                        else None
                    ),
                    "star": star,
                    "chain": chain,
                    "protocol": _slug(missing.protocol.value) if missing.protocol is not None else "",
                    "allocation_type": missing.allocation_type,
                    "token_type": missing.token_type,
                }
            )

    if missing_proxy_keys:
        raise ValueError(
            "assets defined for (star, chain) with no canonical ALM proxy: "
            f"{sorted(missing_proxy_keys)}; add the ALM proxy to the AlmProxy enum"
        )

    assets_by_prime_export: dict[str, list[TokenEntryModel]] = {}
    for entry in entries:
        assets_by_prime_export.setdefault(entry["star"], []).append(TokenEntryModel(**entry))

    alm_proxy_export: dict[str, dict[str, list[ProxyEntry]]] = {}
    for (star, chain), addr_roles in proxies_by_key.items():
        alm_proxy_export.setdefault(star, {})[chain] = [
            ProxyEntry(star=star, chain=chain, address=address, role=role) for address, role in addr_roles
        ]

    return AxisSynomeContract(
        version=version or VENDORED_WHEEL_VERSION,
        axis_synome_git_commit=axis_synome_git_commit or VENDORED_UPSTREAM_COMMIT,
        axis_synome=AxisSynomeModel(
            spec=SpecModel(
                entities=EntitiesModel(
                    assets_by_prime=AssetsByPrimeContainer(entries=assets_by_prime_export),
                    alm_proxies=AlmProxiesContainer(proxies=alm_proxy_export),
                )
            )
        ),
    )


def export_axis_synome_contract(
    data_path: Path,
    schema_path: Path,
    version: str | None,
    axis_synome_git_commit: str | None,
) -> None:
    config = build_axis_synome_contract(
        version=version,
        axis_synome_git_commit=axis_synome_git_commit,
    )

    data_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.parent.mkdir(parents=True, exist_ok=True)

    data_path.write_text(config.model_dump_json(indent=2, by_alias=True) + "\n", encoding="utf-8")

    # Generate the schema in serialization mode so its property names use the
    # serialization aliases (ASSETS_BY_PRIME, AlmProxy) that the emitted data
    # actually uses. With the default (validation) mode the schema would use the
    # Python field names (entries, proxies) and the data would fail to validate
    # against its own schema.
    schema = AxisSynomeContract.model_json_schema(by_alias=True, mode="serialization")
    schema_path.write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export axis-synome entities contract JSON and pydantic schema.")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("generated/stl/axis_synome_entities.json"),
        help="Output JSON data path",
    )
    parser.add_argument(
        "--schema-out",
        type=Path,
        default=Path("generated/stl/axis_synome_entities.schema.json"),
        help="Output JSON schema path",
    )
    parser.add_argument(
        "--version",
        type=str,
        default=None,
        help="Version string embedded in exported config (default: the vendoring-baseline wheel version)",
    )
    parser.add_argument(
        "--git-commit",
        type=str,
        default=None,
        help="axis-synome git commit embedded in exported config (default: the vendoring-baseline upstream commit)",
    )

    args = parser.parse_args()
    export_axis_synome_contract(
        args.out,
        args.schema_out,
        version=args.version,
        axis_synome_git_commit=args.git_commit,
    )


if __name__ == "__main__":
    main()
