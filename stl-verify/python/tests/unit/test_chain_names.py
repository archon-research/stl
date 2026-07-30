"""Pins the Python chain vocabulary against the Go source of truth.

``chain_names.CHAIN_ID_TO_NAME`` is a hand-maintained port of
``entity.ChainIDToName`` in ``stl-verify/internal/domain/entity/chain.go``. The
two languages must agree: the Go map names the chains the allocation trackers are
configured with, and the API reports the same names to consumers. This test reads
the Go file and fails if the maps diverge, so a chain added on one side cannot
silently go missing on the other.
"""

import re
from pathlib import Path

import pytest

from app.domain.chain_names import CHAIN_ID_TO_NAME, chain_name_for

_GO_CHAIN_FILE = Path(__file__).resolve().parents[3] / "internal" / "domain" / "entity" / "chain.go"
_GO_ENTRY = re.compile(r"^\s*(\d+):\s*\"([a-z0-9-]+)\",", re.MULTILINE)
# Any `key: value` line in the literal, whatever form the key and value take.
_GO_CANDIDATE_ENTRY = re.compile(r"^\s*\S+:\s*\S+", re.MULTILINE)


def _go_chain_literal() -> str:
    source = _GO_CHAIN_FILE.read_text()
    start = source.index("var ChainIDToName = map[int64]string{")
    end = source.index("}", start)
    return source[start:end]


def _go_chain_map() -> dict[int, str]:
    return {int(cid): name for cid, name in _GO_ENTRY.findall(_go_chain_literal())}


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


@pytest.mark.parametrize(
    ("chain_id", "expected"),
    [(1, "mainnet"), (8453, "base"), (43114, "avalanche-c"), (42161, "arbitrum")],
)
def test_chain_name_for_resolves_known_chain_ids(chain_id, expected):
    assert chain_name_for(chain_id) == expected


def test_chain_name_for_returns_none_for_an_unknown_chain_id():
    assert chain_name_for(999999) is None
