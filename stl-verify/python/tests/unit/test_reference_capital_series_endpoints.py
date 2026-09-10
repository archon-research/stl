"""Provenance selection on the two per-prime capital time series.

Both endpoints project the same stored snapshot onto their own field, so they
are covered together: a divergence between them is exactly the bug worth
catching.
"""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from app.api.deps import get_reference_capital_repository_factory
from app.api.v1 import exposure, total_capital
from app.domain.entities.reference_risk_capital import ReferenceCapitalBucket
from app.main import app
from app.services.allocation_service import AllocationService

_VALID_ADDR = "0x" + "ab" * 20
_BUCKET = datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc)


def _buckets() -> list[ReferenceCapitalBucket]:
    return [
        ReferenceCapitalBucket(
            bucket_start=_BUCKET,
            total_capital_usd=Decimal("48142491.08"),
            exposure_usd=Decimal("2098090654.81"),
        ),
        # A leading bucket before the syncer's first observation: not yet seen,
        # which must stay null rather than becoming zero.
        ReferenceCapitalBucket(bucket_start=_BUCKET, total_capital_usd=None, exposure_usd=None),
    ]


@pytest.fixture(params=["total-capital", "exposure"])
def series(request):
    """A client plus the field each endpoint projects from the shared snapshot."""
    module, path, field = {
        "total-capital": (total_capital, "total-capital", "total_capital_usd"),
        "exposure": (exposure, "exposure", "exposure_usd"),
    }[request.param]

    service = AsyncMock(spec=AllocationService)
    service.prime_exists.return_value = True
    repository = AsyncMock()
    repository.list_reference_capital_buckets.return_value = _buckets()

    async def _service_dep():
        yield service

    app.dependency_overrides[module._get_service] = _service_dep
    app.dependency_overrides[get_reference_capital_repository_factory] = lambda: lambda: repository
    try:
        yield TestClient(app), path, field, service, repository
    finally:
        app.dependency_overrides.clear()


def test_reference_series_reports_its_provenance(series):
    client, path, _, _, _ = series

    body = client.get(f"/v1/primes/{_VALID_ADDR}/{path}?reference=true").json()

    assert body["source"] == "reference"


def test_source_reference_selects_the_same_series_as_the_superseded_flag(series):
    client, path, _, _, _ = series

    by_source = client.get(f"/v1/primes/{_VALID_ADDR}/{path}?source=reference").json()
    by_flag = client.get(f"/v1/primes/{_VALID_ADDR}/{path}?reference=true").json()

    # Not the whole body: a defaulted window is now-relative, so two calls
    # disagree by microseconds.
    assert (by_source["source"], by_source["data"]) == (by_flag["source"], by_flag["data"])


def test_source_indexed_reads_stl_own_figures(series):
    client, path, _, _, repository = series

    body = client.get(f"/v1/primes/{_VALID_ADDR}/{path}?source=indexed").json()

    assert body["source"] == "indexed"
    repository.list_reference_capital_buckets.assert_not_called()


def test_both_carries_each_provenance_on_the_same_bucket(series):
    client, path, field, _, _ = series

    body = client.get(f"/v1/primes/{_VALID_ADDR}/{path}?source=both").json()

    assert body["source"] == "both"
    # Aligned, not concatenated: one bucket carries both figures, so a chart can
    # overlay them without matching timestamps itself.
    assert body["data"], "expected buckets"
    for bucket in body["data"]:
        assert "bucket_start" in bucket
        assert f"reference_{field}" in bucket


def test_rejects_a_source_that_contradicts_the_superseded_flag(series):
    client, path, _, _, _ = series

    response = client.get(f"/v1/primes/{_VALID_ADDR}/{path}?source=indexed&reference=true")

    assert response.status_code == 422


def test_reference_series_serves_the_stored_upstream_figure(series):
    client, path, field, _, _ = series
    expected = {"total_capital_usd": "48142491.08", "exposure_usd": "2098090654.81"}[field]

    body = client.get(f"/v1/primes/{_VALID_ADDR}/{path}?reference=true").json()

    assert body["data"][0][field] == expected


def test_reference_series_keeps_an_unobserved_bucket_null(series):
    client, path, field, _, _ = series

    body = client.get(f"/v1/primes/{_VALID_ADDR}/{path}?reference=true").json()

    assert body["data"][1][field] is None


def test_reference_series_never_reads_the_self_series(series):
    client, path, _, service, _ = series

    client.get(f"/v1/primes/{_VALID_ADDR}/{path}?reference=true")

    service.list_total_capital_buckets.assert_not_awaited()
    service.list_exposure_buckets.assert_not_awaited()


def test_self_mode_never_reads_the_reference_store(series):
    client, path, _, service, repository = series
    service.list_total_capital_buckets.return_value = []
    service.list_exposure_buckets.return_value = []

    body = client.get(f"/v1/primes/{_VALID_ADDR}/{path}").json()

    assert body["source"] == "indexed"
    repository.list_reference_capital_buckets.assert_not_awaited()


def test_reference_series_still_404s_for_an_unknown_prime(series):
    client, path, _, service, _ = series
    service.prime_exists.return_value = False

    response = client.get(f"/v1/primes/{_VALID_ADDR}/{path}?reference=true")

    assert response.status_code == 404
