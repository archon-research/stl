"""The ``reference=true`` branch of ``/v1/primes/{id}/debt``.

Reference debt is aggregate-only: upstream publishes one figure per prime per
day and carries no ilk or block identity, so a raw snapshot cannot be filled
without inventing those fields.
"""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from app.api.v1 import prime_debts
from app.domain.entities.time_series_bucket import PrimeDebtBucket
from app.main import app
from app.services.prime_debt_service import PrimeDebtService

_VALID_ADDR = "0x" + "ab" * 20
_PRIME_ID = 7
_BUCKET = datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def client():
    service = AsyncMock(spec=PrimeDebtService)
    service.resolve_prime_id.return_value = _PRIME_ID
    service.list_reference_debt_buckets.return_value = [
        PrimeDebtBucket(bucket_start=_BUCKET, debt_wad=Decimal("2645260280720000000000000000"))
    ]
    service.list_debt_buckets.return_value = [PrimeDebtBucket(bucket_start=_BUCKET, debt_wad=Decimal("1"))]

    async def _dep():
        yield service

    app.dependency_overrides[prime_debts._get_prime_debt_service] = _dep
    try:
        yield TestClient(app), service
    finally:
        app.dependency_overrides.clear()


def test_reference_debt_reports_its_provenance(client):
    test_client, service = client

    body = test_client.get(f"/v1/primes/{_VALID_ADDR}/debt?reference=true&aggregate=true").json()

    assert body["source"] == "reference"
    assert body["mode"] == "aggregated"
    assert service.list_reference_debt_buckets.await_args.args[0] == _PRIME_ID


def test_reference_debt_serves_the_upstream_figure_in_wad(client):
    # Same unit in both provenances, so a consumer dividing by 1e18 gets USDS
    # units either way.
    test_client, _ = client

    body = test_client.get(f"/v1/primes/{_VALID_ADDR}/debt?reference=true&aggregate=true").json()

    assert body["data"][0]["debt_wad"] == "2645260280720000000000000000"


def test_reference_debt_never_reads_the_onchain_series(client):
    test_client, service = client

    test_client.get(f"/v1/primes/{_VALID_ADDR}/debt?reference=true&aggregate=true")

    service.list_debt_buckets.assert_not_awaited()


def test_reference_debt_rejects_a_raw_request(client):
    # Filling a raw snapshot would mean inventing an ilk and a block identity
    # upstream does not have.
    test_client, service = client

    response = test_client.get(f"/v1/primes/{_VALID_ADDR}/debt?reference=true")

    assert response.status_code == 400
    assert "aggregate=true" in response.json()["detail"]
    service.list_debt_snapshots.assert_not_awaited()


def test_self_mode_is_unchanged_and_never_reads_the_reference_series(client):
    test_client, service = client

    body = test_client.get(f"/v1/primes/{_VALID_ADDR}/debt?aggregate=true").json()

    assert body["source"] == "indexed"
    assert body["data"][0]["debt_wad"] == "1"
    service.list_reference_debt_buckets.assert_not_awaited()


def test_reference_debt_still_404s_for_an_unknown_prime(client):
    test_client, service = client
    service.resolve_prime_id.return_value = None

    response = test_client.get(f"/v1/primes/{_VALID_ADDR}/debt?reference=true&aggregate=true")

    assert response.status_code == 404


def test_source_reference_selects_the_upstream_debt(client):
    client, service = client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/debt?aggregate=true&source=reference").json()

    assert body["source"] == "reference"
    service.list_reference_debt_buckets.assert_awaited()


def test_source_indexed_selects_the_on_chain_debt(client):
    client, service = client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/debt?aggregate=true&source=indexed").json()

    assert body["source"] == "indexed"
    service.list_reference_debt_buckets.assert_not_awaited()


def test_both_carries_each_provenance_on_the_same_bucket(client):
    client, service = client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/debt?aggregate=true&source=both").json()

    assert body["source"] == "both"
    service.list_debt_buckets.assert_awaited()
    service.list_reference_debt_buckets.assert_awaited()
    for bucket in body["data"]:
        assert "reference_debt_wad" in bucket


def test_both_still_requires_an_aggregated_window(client):
    # The reference half cannot fill a raw snapshot, so neither can the union.
    client, _ = client

    response = client.get(f"/v1/primes/{_VALID_ADDR}/debt?source=both")

    assert response.status_code == 400


def test_source_reference_still_requires_an_aggregated_window(client):
    # Upstream carries no ilk or block identity, so it cannot fill a raw
    # snapshot -- the same refusal the superseded flag got.
    client, _ = client

    response = client.get(f"/v1/primes/{_VALID_ADDR}/debt?source=reference")

    assert response.status_code == 400
