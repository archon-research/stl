"""Pins the Python chain declarations against the Go sources of truth.

Two hand-maintained ports, both read from the Go files here so a chain added on
one side cannot silently go missing on the other:

- ``chain_names.CHAIN_ID_TO_NAME`` ports ``entity.ChainIDToName``
  (``internal/domain/entity/chain.go``) — the names the trackers are configured
  with and the API reports to consumers.
- ``chain_names.SERVED_TRACKER_CHAINS`` ports ``servedTrackerChains``
  (``internal/services/allocation_tracker/chains.go``) — which of those chains a
  tracker is actually deployed for, and so which contribute to a prime-wide total
  rather than reading ``null``.
"""

import re
from pathlib import Path

import pytest

from app.domain.chain_names import (
    CHAIN_ID_TO_NAME,
    SERVED_TRACKER_CHAINS,
    chain_is_served,
    chain_name_for,
)

_GO_CHAIN_FILE = Path(__file__).resolve().parents[3] / "internal" / "domain" / "entity" / "chain.go"
_GO_ENTRY = re.compile(r"^\s*(\d+):\s*\"([a-z0-9-]+)\",", re.MULTILINE)
# Any `key: value` line in the literal, whatever form the key and value take.
_GO_CANDIDATE_ENTRY = re.compile(r"^\s*\S+:\s*\S+", re.MULTILINE)


_GO_TRACKER_CHAINS_FILE = (
    Path(__file__).resolve().parents[3] / "internal" / "services" / "allocation_tracker" / "chains.go"
)
_GO_SERVED_ENTRY = re.compile(r"^\s*\"([a-z0-9-]+)\":\s*true,", re.MULTILINE)


def _go_map_literal(path: Path, declaration: str) -> str:
    source = path.read_text()
    start = source.index(declaration)
    end = source.index("}", start)
    return source[start:end]


def _go_chain_literal() -> str:
    return _go_map_literal(_GO_CHAIN_FILE, "var ChainIDToName = map[int64]string{")


def _go_served_literal() -> str:
    return _go_map_literal(_GO_TRACKER_CHAINS_FILE, "var servedTrackerChains = map[string]bool{")


def _go_chain_map() -> dict[int, str]:
    return {int(cid): name for cid, name in _GO_ENTRY.findall(_go_chain_literal())}


def _go_served_chains() -> set[str]:
    return set(_GO_SERVED_ENTRY.findall(_go_served_literal()))


def test_the_go_chain_file_is_readable():
    # Guards the parser itself: an empty parse would make the equality test below
    # pass vacuously if chain.go moved or its literal was reformatted.
    assert _go_chain_map() != {}


def test_the_go_chain_parse_covers_every_entry():
    # Non-empty is not enough: _GO_ENTRY only matches a decimal key, a
    # lowercase-hyphen value and a trailing comma, so a new Go entry in any other
    # form (a named ChainID constant, a hex key, an underscore in the name, a
    # missing final comma) would be skipped while both maps stayed equal at their
    # old size — precisely the drift this file exists to catch. Counting candidate
    # lines makes an unparsed entry fail instead of vanish.
    assert len(_GO_CANDIDATE_ENTRY.findall(_go_chain_literal())) == len(_go_chain_map())


def test_python_chain_vocabulary_matches_go():
    assert CHAIN_ID_TO_NAME == _go_chain_map()


def test_python_served_chains_match_go():
    # Drift here is silent and one-directional in the dangerous way: a tracker
    # deployed on the Go side without this update leaves the API nulling real
    # per-chain figures and understating the prime's encumbrance.
    assert set(SERVED_TRACKER_CHAINS) == _go_served_chains()


def test_the_go_served_chain_parse_covers_every_entry():
    # Same totality guard as the vocabulary parse above: an entry the regex cannot
    # read would shrink both sides equally and pass.
    assert len(_GO_CANDIDATE_ENTRY.findall(_go_served_literal())) == len(_go_served_chains())


def test_chain_is_served_rejects_a_chain_no_tracker_serves():
    # plume carries a contract ALM proxy but is absent from the vocabulary, so no
    # tracker can be configured for it — the case a proxy-to-chain lookup answers
    # and a chain-id lookup cannot.
    assert chain_is_served("plume") is False


def test_chain_is_served_rejects_a_proxy_with_no_discoverable_chain():
    assert chain_is_served(None) is False


@pytest.mark.parametrize(
    ("chain_id", "expected"),
    [(1, "mainnet"), (8453, "base"), (43114, "avalanche-c"), (42161, "arbitrum")],
)
def test_chain_name_for_resolves_known_chain_ids(chain_id, expected):
    assert chain_name_for(chain_id) == expected


def test_chain_name_for_returns_none_for_an_unknown_chain_id():
    assert chain_name_for(999999) is None
