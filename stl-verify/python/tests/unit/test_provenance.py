import pytest

from app.domain.provenance import (
    Provenance,
    legacy_reference_flag_as_provenance,
    resolve_provenance,
)

_BOTH = frozenset(Provenance)
_INDEXED_ONLY = frozenset({Provenance.INDEXED})


def test_defaults_to_both_when_nothing_was_requested():
    assert resolve_provenance(None, available=_BOTH) is Provenance.BOTH


@pytest.mark.parametrize("requested", list(Provenance))
def test_serves_any_provenance_the_resource_has(requested: Provenance):
    assert resolve_provenance(requested, available=_BOTH) is requested


def test_narrows_both_to_the_only_provenance_a_resource_has():
    # Allocation activity has no reference feed, and `both` is the default that
    # every request carries -- rejecting it would break the default page load.
    assert resolve_provenance(Provenance.BOTH, available=_INDEXED_ONLY) is Provenance.INDEXED


def test_rejects_a_provenance_asked_for_by_name_that_cannot_be_served():
    with pytest.raises(ValueError, match="source=reference is not available"):
        resolve_provenance(Provenance.REFERENCE, available=_INDEXED_ONLY)


def test_names_what_the_resource_does_serve():
    with pytest.raises(ValueError, match="serves: indexed"):
        resolve_provenance(Provenance.REFERENCE, available=_INDEXED_ONLY)


def test_rejects_both_when_the_resource_serves_nothing():
    with pytest.raises(ValueError, match="no provenance is available"):
        resolve_provenance(Provenance.BOTH, available=frozenset())


def test_honours_a_caller_supplied_default():
    assert resolve_provenance(None, available=_BOTH, default=Provenance.INDEXED) is Provenance.INDEXED


@pytest.mark.parametrize(
    ("flag", "expected"),
    [
        (None, None),
        (True, Provenance.REFERENCE),
        # Not the `both` default: it asked for STL's own figures by name.
        (False, Provenance.INDEXED),
    ],
)
def test_maps_the_superseded_reference_flag(flag: bool | None, expected: Provenance | None):
    assert legacy_reference_flag_as_provenance(flag) is expected
