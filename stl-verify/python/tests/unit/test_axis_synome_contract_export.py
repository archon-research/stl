"""Consumer-contract guard for the axis-synome entities export.

The Go allocation/psm3 workers load the exported contract into a struct that
decodes with ``DisallowUnknownFields`` (see
``stl-verify/internal/pkg/axis_synome_contract/loader.go``). That makes the JSON
*shape* — the nesting, the serialization aliases, and the exact field set of
each entry — part of the cross-language contract, not an implementation detail.

These tests pin that shape against the vendored axis-synome registry, so a
breaking change to ``export_entities`` (e.g. the ``spec.asc`` wrapper that was
silently dropped in 0.2.0) fails the Python sentinel compatibility gate instead
of shipping and breaking the Go workers at runtime. Keep the expected field sets
below in lockstep with the Go ``TokenEntry`` / ``ProxyConfig`` structs.
"""

import json
from pathlib import Path

from app.risk_engine._vendored_synome.export_entities import build_axis_synome_contract
from app.risk_engine._vendored_synome.spec.entities.protocol_sets import AllocationType, TokenType

# Mirror of the Go `TokenEntry` struct json tags in loader.go. Go decodes with
# DisallowUnknownFields, so an added or renamed field breaks the workers.
EXPECTED_TOKEN_ENTRY_FIELDS = {
    "contract_address",
    "wallet_address",
    "asset_address",
    "star",
    "chain",
    "protocol",
    "allocation_type",
    "token_type",
}

# Mirror of the Go `ProxyConfig` struct json tags in loader.go.
EXPECTED_PROXY_FIELDS = {"star", "chain", "address", "role"}

# The contract file the Go workers load at runtime, committed to this repo.
_COMMITTED_CONTRACT = Path(__file__).resolve().parents[3] / "contracts" / "axis-synome" / "axis_synome_entities.json"

# The Go loader decodes allocation_type/token_type as plain strings (loader.go),
# so a renamed or newly-added enum value slips past strict decoding and reaches
# workers that switch on it. Pin the full value sets here: a change upstream
# fails the gate and forces a conscious decision to teach the workers the new
# value. Keep in lockstep with axis_synome's AllocationType / TokenType enums.
EXPECTED_ALLOCATION_TYPES = {"allocation", "asset", "pol", "psm3", "risk_capital"}
EXPECTED_TOKEN_TYPES = {
    "anchorage",
    "atoken",
    "centrifuge",
    "centrifuge_feeder",
    "curve",
    "erc20",
    "erc4626",
    "proxy",
    "psm3",
    "superstate",
    "uni_v3_lp",
    "uni_v3_pool",
}


def _export() -> dict:
    # mode="json" serializes enums to the strings the Go loader actually reads,
    # rather than leaving them as Python enum members.
    return build_axis_synome_contract().model_dump(by_alias=True, mode="json")


def test_export_has_the_nesting_the_go_loader_expects():
    contract = _export()

    spec = contract["axis_synome"]["spec"]
    assert "asc" not in spec, (
        "spec.asc wrapper is back; the Go loader expects entities directly under "
        "spec — a wrapper change here breaks the workers"
    )
    entities = spec["entities"]
    assert set(entities) == {"assets_by_prime", "alm_proxies"}
    assert "ASSETS_BY_PRIME" in entities["assets_by_prime"]
    assert "AlmProxy" in entities["alm_proxies"]


def test_export_top_level_keys_match_go_contract():
    contract = _export()

    assert set(contract) == {"version", "axis_synome_git_commit", "axis_synome"}
    assert set(contract["axis_synome"]) == {"spec"}


def test_token_entries_carry_exactly_the_fields_go_decodes():
    assets_by_prime = _export()["axis_synome"]["spec"]["entities"]["assets_by_prime"]["ASSETS_BY_PRIME"]
    assert assets_by_prime, "ASSETS_BY_PRIME is empty; the export produced no entries"

    for star, entries in assets_by_prime.items():
        assert entries, f"star {star!r} has no token entries"
        for entry in entries:
            assert set(entry) == EXPECTED_TOKEN_ENTRY_FIELDS, (
                f"token entry field set drifted from the Go TokenEntry struct: {set(entry)}"
            )


def test_allocation_and_token_type_enums_match_expected():
    assert {m.value for m in AllocationType} == EXPECTED_ALLOCATION_TYPES
    assert {m.value for m in TokenType} == EXPECTED_TOKEN_TYPES


def test_exported_entries_use_only_known_enum_values():
    assets_by_prime = _export()["axis_synome"]["spec"]["entities"]["assets_by_prime"]["ASSETS_BY_PRIME"]
    for star, entries in assets_by_prime.items():
        for entry in entries:
            assert entry["allocation_type"] in EXPECTED_ALLOCATION_TYPES, (
                f"star {star!r}: unknown allocation_type {entry['allocation_type']!r}"
            )
            assert entry["token_type"] in EXPECTED_TOKEN_TYPES, (
                f"star {star!r}: unknown token_type {entry['token_type']!r}"
            )


def test_proxy_entries_carry_exactly_the_fields_go_decodes():
    alm_proxy = _export()["axis_synome"]["spec"]["entities"]["alm_proxies"]["AlmProxy"]
    assert alm_proxy, "AlmProxy is empty; the export produced no proxies"

    for star, by_chain in alm_proxy.items():
        for chain, proxies in by_chain.items():
            assert proxies, f"{star}/{chain} has no proxies"
            for proxy in proxies:
                assert set(proxy) == EXPECTED_PROXY_FIELDS, (
                    f"proxy field set drifted from the Go ProxyConfig struct: {set(proxy)}"
                )


def test_the_committed_contract_entities_match_this_packages_export():
    """Ties the file Go reads to the registry Python computes from.

    Go loads ``contracts/axis-synome/axis_synome_entities.json`` at runtime while
    the API recomputes the same topology in-process from the vendored registry.
    Nothing else connects the two, so a registry edit that re-points a proxy —
    or a re-export nobody committed — would leave the trackers indexing one
    address set while the API attributes figures to another, silently and in
    opposite directions.
    """
    committed = json.loads(_COMMITTED_CONTRACT.read_text())

    assert committed["axis_synome"] == _export()["axis_synome"], (
        "committed axis_synome_entities.json is stale against the vendored "
        "registry — run 'make export-axis-synome-contract' and commit the result"
    )


def test_the_committed_contract_version_matches_the_vendored_baseline():
    committed = json.loads(_COMMITTED_CONTRACT.read_text())

    assert committed["version"] == _export()["version"]
