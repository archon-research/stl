"""Router-level tests for the unified ``/v1/risk/rrc`` endpoints.

These tests pin the FastAPI translation contract on top of a fake
``ModelRegistry`` populated with stubbed ``RiskModel`` instances. They
cover the VEC-183 acceptance list:

* SURAF-only applicability vs. multi-model applicability
* GET uses defaults; POST forwards overrides per model
* unknown top-level override model key -> 422
* unknown per-model override key (service raises ``ValueError``) -> 422
* no applicable models -> 404
* missing ``prime_id`` -> 422
* ``AllocationShareError`` subtypes -> 503 with matching ``share_data_*`` code
"""

from collections.abc import Mapping
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from app.api.deps import get_model_registry, get_receipt_token_lookup
from app.domain.entities.allocation import EthAddress
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.risk import GapSweepDetails, ModelName, RrcResult, SurafDetails
from app.domain.exceptions import InvalidOverrideError, MissingShareError, StaleShareError
from app.main import app
from app.ports.allocation_repository import AllocationRepository
from app.ports.crypto_lending_reader import CryptoLendingReader
from app.risk_engine.suraf.result import SurafResult
from app.services.crypto_lending_risk_service import CryptoLendingRiskService
from app.services.model_registry import ModelRegistry
from app.services.suraf_rrc_service import SurafRrcService

_PRIME_ID = "0x" + "ab" * 20
_PRIME = EthAddress(_PRIME_ID)
_ASSET_ID = 1234


class _FakeRiskModel:
    """Test double that records calls and returns a canned ``RrcResult``."""

    def __init__(
        self,
        risk_model: ModelName,
        result: RrcResult | None = None,
        *,
        applies: bool = True,
        raises: Exception | None = None,
    ) -> None:
        self.risk_model: ModelName = risk_model
        self._result = result
        self._applies = applies
        self._raises = raises
        self.calls: list[tuple[int, EthAddress, Mapping[str, Any]]] = []

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:  # noqa: ARG002
        return self._applies

    async def compute(
        self,
        asset_id: int,
        prime_id: EthAddress,
        overrides: Mapping[str, Any],
    ) -> RrcResult:
        self.calls.append((asset_id, prime_id, dict(overrides)))
        if self._raises is not None:
            raise self._raises
        assert self._result is not None
        return self._result


def _suraf_result(rrc_usd: Decimal = Decimal("337.0")) -> RrcResult:
    return RrcResult(
        asset_id=_ASSET_ID,
        prime_id=_PRIME_ID,
        rrc_usd=rrc_usd,
        comparable_crr_pct=Decimal("33.7"),
        risk_model="suraf",
        details=SurafDetails(
            risk_model="suraf",
            rating_id="aave_ausdc",
            rating_version="v7",
            crr_pct=Decimal("33.7"),
            unadjusted_crr_pct=Decimal("32.7"),
            penalty_pp=Decimal("1.0"),
            source_commit_sha="abc123",
        ),
    )


def _gap_sweep_result(rrc_usd: Decimal = Decimal("1200.5")) -> RrcResult:
    return RrcResult(
        asset_id=_ASSET_ID,
        prime_id=_PRIME_ID,
        rrc_usd=rrc_usd,
        comparable_crr_pct=Decimal("12.00"),
        risk_model="gap_sweep",
        details=GapSweepDetails(
            risk_model="gap_sweep",
            gap_pct=Decimal("0.15"),
            loss_usd=rrc_usd,
        ),
    )


def _override_registry(registry: ModelRegistry):
    def _dep() -> ModelRegistry:
        return registry

    return _dep


_RECEIPT_TOKEN_INFO = ReceiptTokenInfo(
    receipt_token_id=_ASSET_ID,
    protocol_id=10,
    underlying_token_id=20,
    receipt_token_address=bytes.fromhex("01" * 20),
    chain_id=1,
    protocol_name="aave_v3",
    receipt_token_token_id=30,
)


def _override_lookup(info: ReceiptTokenInfo | None = _RECEIPT_TOKEN_INFO):
    lookup = AsyncMock()
    lookup.get = AsyncMock(return_value=info)

    def _dep():
        return lookup

    return _dep


@pytest.fixture
def client():
    """TestClient that cleans up dependency overrides between tests."""
    # Default the lookup so tests focused on registry behavior don't have
    # to wire it up explicitly. Tests that need a different lookup result
    # (e.g. unknown asset_id) can re-install the override.
    app.dependency_overrides[get_receipt_token_lookup] = _override_lookup()
    yield TestClient(app)
    app.dependency_overrides.pop(get_model_registry, None)
    app.dependency_overrides.pop(get_receipt_token_lookup, None)


# ---------------------------------------------------------------------------
# Applicability and dispatch
# ---------------------------------------------------------------------------


def test_get_returns_single_result_when_only_one_model_applies(client: TestClient) -> None:
    suraf = _FakeRiskModel("suraf", _suraf_result())
    gap = _FakeRiskModel("gap_sweep", applies=False)
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([suraf, gap]))

    response = client.get(f"/v1/risk/rrc?asset_id={_ASSET_ID}&prime_id={_PRIME_ID}")

    assert response.status_code == 200
    body = response.json()
    assert body["asset_id"] == _ASSET_ID
    assert body["prime_id"] == _PRIME_ID
    assert len(body["results"]) == 1
    assert body["results"][0]["risk_model"] == "suraf"
    assert body["results"][0]["details"]["risk_model"] == "suraf"
    assert gap.calls == []
    assert suraf.calls == [(_ASSET_ID, _PRIME, {})]


def test_get_returns_multiple_results_when_models_overlap(client: TestClient) -> None:
    suraf = _FakeRiskModel("suraf", _suraf_result())
    gap = _FakeRiskModel("gap_sweep", _gap_sweep_result())
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([suraf, gap]))

    response = client.get(f"/v1/risk/rrc?asset_id={_ASSET_ID}&prime_id={_PRIME_ID}")

    assert response.status_code == 200
    models = [r["risk_model"] for r in response.json()["results"]]
    assert models == ["suraf", "gap_sweep"]


def test_get_returns_404_when_no_models_apply(client: TestClient) -> None:
    suraf = _FakeRiskModel("suraf", applies=False)
    gap = _FakeRiskModel("gap_sweep", applies=False)
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([suraf, gap]))

    response = client.get(f"/v1/risk/rrc?asset_id={_ASSET_ID}&prime_id={_PRIME_ID}")

    assert response.status_code == 404
    assert "no risk models apply" in response.json()["detail"]
    assert suraf.calls == []
    assert gap.calls == []


def test_get_returns_404_when_asset_id_unknown(client: TestClient) -> None:
    """Receipt-token lookup returns None -> 404 before registry is consulted."""
    suraf = _FakeRiskModel("suraf", _suraf_result())
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([suraf]))
    app.dependency_overrides[get_receipt_token_lookup] = _override_lookup(info=None)

    response = client.get(f"/v1/risk/rrc?asset_id={_ASSET_ID}&prime_id={_PRIME_ID}")

    assert response.status_code == 404
    assert f"unknown asset_id={_ASSET_ID}" in response.json()["detail"]
    assert suraf.calls == []  # short-circuit before registry


def test_get_envelope_includes_chain_id_and_receipt_token_address(client: TestClient) -> None:
    suraf = _FakeRiskModel("suraf", _suraf_result())
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([suraf]))

    response = client.get(f"/v1/risk/rrc?asset_id={_ASSET_ID}&prime_id={_PRIME_ID}")

    body = response.json()
    assert body["chain_id"] == _RECEIPT_TOKEN_INFO.chain_id
    assert body["receipt_token_address"] == _RECEIPT_TOKEN_INFO.receipt_token_address_hex


def test_envelope_max_rrc_and_max_crr_take_max_across_models(client: TestClient) -> None:
    """Multi-model: max_rrc_usd is the largest USD; max_crr_pct is the largest comparable CRR.

    SURAF result: rrc_usd=337.0, crr_pct=33.7.
    Gap-sweep result: rrc_usd=1200.5, comparable_crr_pct=12.00.
    Expected: max_rrc_usd=1200.5 (gap-sweep wins), max_crr_pct=33.7 (SURAF wins).
    """
    suraf = _FakeRiskModel("suraf", _suraf_result())
    gap = _FakeRiskModel("gap_sweep", _gap_sweep_result())
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([suraf, gap]))

    response = client.get(f"/v1/risk/rrc?asset_id={_ASSET_ID}&prime_id={_PRIME_ID}")

    body = response.json()
    assert Decimal(body["max_rrc_usd"]) == Decimal("1200.5")
    assert Decimal(body["max_crr_pct"]) == Decimal("33.7")


def test_envelope_gap_sweep_only_uses_model_comparable_crr(client: TestClient) -> None:
    """When only gap-sweep applies, max_crr_pct comes from the model result."""
    gap = _FakeRiskModel("gap_sweep", _gap_sweep_result())
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([gap]))

    response = client.get(f"/v1/risk/rrc?asset_id={_ASSET_ID}&prime_id={_PRIME_ID}")

    body = response.json()
    assert Decimal(body["max_rrc_usd"]) == Decimal("1200.5")
    assert Decimal(body["max_crr_pct"]) == Decimal("12.00")


# ---------------------------------------------------------------------------
# GET defaults vs POST overrides
# ---------------------------------------------------------------------------


def test_get_passes_empty_overrides(client: TestClient) -> None:
    suraf = _FakeRiskModel("suraf", _suraf_result())
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([suraf]))

    response = client.get(f"/v1/risk/rrc?asset_id={_ASSET_ID}&prime_id={_PRIME_ID}")

    assert response.status_code == 200
    assert suraf.calls == [(_ASSET_ID, _PRIME, {})]


def test_post_forwards_per_model_overrides(client: TestClient) -> None:
    suraf = _FakeRiskModel("suraf", _suraf_result())
    gap = _FakeRiskModel("gap_sweep", _gap_sweep_result())
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([suraf, gap]))

    response = client.post(
        "/v1/risk/rrc",
        json={
            "asset_id": _ASSET_ID,
            "prime_id": _PRIME_ID,
            "overrides": {
                "suraf": {"usd_exposure": "1000"},
                "gap_sweep": {"gap_pct": "0.25"},
            },
        },
    )

    assert response.status_code == 200
    assert suraf.calls == [(_ASSET_ID, _PRIME, {"usd_exposure": "1000"})]
    assert gap.calls == [(_ASSET_ID, _PRIME, {"gap_pct": "0.25"})]


def test_post_omits_overrides_for_unmentioned_models(client: TestClient) -> None:
    suraf = _FakeRiskModel("suraf", _suraf_result())
    gap = _FakeRiskModel("gap_sweep", _gap_sweep_result())
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([suraf, gap]))

    response = client.post(
        "/v1/risk/rrc",
        json={
            "asset_id": _ASSET_ID,
            "prime_id": _PRIME_ID,
            "overrides": {"suraf": {"usd_exposure": "1000"}},
        },
    )

    assert response.status_code == 200
    assert suraf.calls == [(_ASSET_ID, _PRIME, {"usd_exposure": "1000"})]
    assert gap.calls == [(_ASSET_ID, _PRIME, {})]


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------


def test_post_returns_422_on_unknown_top_level_override_model(client: TestClient) -> None:
    suraf = _FakeRiskModel("suraf", _suraf_result())
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([suraf]))

    response = client.post(
        "/v1/risk/rrc",
        json={
            "asset_id": _ASSET_ID,
            "prime_id": _PRIME_ID,
            "overrides": {"typo_model": {"foo": "bar"}},
        },
    )

    assert response.status_code == 422
    assert "typo_model" in response.json()["detail"]
    assert suraf.calls == []


def test_post_returns_422_when_service_raises_on_unknown_per_model_key(client: TestClient) -> None:
    suraf = _FakeRiskModel(
        "suraf",
        raises=InvalidOverrideError("unknown override keys: ['nonsense']"),
    )
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([suraf]))

    response = client.post(
        "/v1/risk/rrc",
        json={
            "asset_id": _ASSET_ID,
            "prime_id": _PRIME_ID,
            "overrides": {"suraf": {"nonsense": 1}},
        },
    )

    assert response.status_code == 422
    assert "nonsense" in response.json()["detail"]


def test_post_lets_invariant_breach_value_error_become_500() -> None:
    """Bare ValueError (not InvalidOverrideError) is treated as a service bug.

    Installs the same registry + lookup overrides as the shared fixture so the
    only path to a 500 is the configured ``compute()`` raising — otherwise a
    missing dependency would also produce a 500 and the assertion would pass
    for the wrong reason.
    """
    suraf = _FakeRiskModel("suraf", raises=ValueError("registry let through unsupported asset"))
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([suraf]))
    app.dependency_overrides[get_receipt_token_lookup] = _override_lookup()
    try:
        # raise_server_exceptions=False so the unhandled error becomes a 500
        # response we can assert on, rather than re-raising into the test.
        client = TestClient(app, raise_server_exceptions=False)

        response = client.post(
            "/v1/risk/rrc",
            json={"asset_id": _ASSET_ID, "prime_id": _PRIME_ID, "overrides": {}},
        )

        assert response.status_code == 500
        # Sanity: the fake compute() was the source. If the lookup had been
        # unwired, we'd hit a 500 before ever calling compute().
        assert len(suraf.calls) == 1
    finally:
        app.dependency_overrides.pop(get_model_registry, None)
        app.dependency_overrides.pop(get_receipt_token_lookup, None)


def test_get_returns_422_when_prime_id_missing(client: TestClient) -> None:
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([]))

    response = client.get(f"/v1/risk/rrc?asset_id={_ASSET_ID}")

    assert response.status_code == 422


def test_get_returns_422_when_prime_id_malformed(client: TestClient) -> None:
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([]))

    response = client.get(f"/v1/risk/rrc?asset_id={_ASSET_ID}&prime_id=not-an-address")

    assert response.status_code == 422


def test_post_returns_422_when_asset_id_missing(client: TestClient) -> None:
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([]))

    response = client.post("/v1/risk/rrc", json={"prime_id": _PRIME_ID})

    assert response.status_code == 422


@pytest.mark.parametrize("bad_id", [0, -1, -1234])
def test_get_returns_422_when_asset_id_not_positive(client: TestClient, bad_id: int) -> None:
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([]))

    response = client.get(f"/v1/risk/rrc?asset_id={bad_id}&prime_id={_PRIME_ID}")

    assert response.status_code == 422


@pytest.mark.parametrize("bad_id", [0, -1])
def test_post_returns_422_when_asset_id_not_positive(client: TestClient, bad_id: int) -> None:
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([]))

    response = client.post(
        "/v1/risk/rrc",
        json={"asset_id": bad_id, "prime_id": _PRIME_ID},
    )

    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Share-data errors -> 503 with code
# ---------------------------------------------------------------------------


def test_get_returns_503_share_data_missing_when_compute_raises(client: TestClient) -> None:
    gap = _FakeRiskModel("gap_sweep", raises=MissingShareError("no active allocation"))
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([gap]))

    response = client.get(f"/v1/risk/rrc?asset_id={_ASSET_ID}&prime_id={_PRIME_ID}")

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "share_data_missing"
    assert body["detail"]["message"] == "no active allocation"


def test_get_returns_503_share_data_stale_when_compute_raises(client: TestClient) -> None:
    gap = _FakeRiskModel("gap_sweep", raises=StaleShareError("supply too old"))
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([gap]))

    response = client.get(f"/v1/risk/rrc?asset_id={_ASSET_ID}&prime_id={_PRIME_ID}")

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "share_data_stale"


# ---------------------------------------------------------------------------
# Real CryptoLendingRiskService — exercises override coercion + validation.
# Uses the production service with only the outbound CryptoLendingReader port
# mocked; covers paths the FakeRiskModel above necessarily skips.
# ---------------------------------------------------------------------------


def _real_gap_sweep_service() -> tuple[CryptoLendingRiskService, AsyncMock]:
    reader = AsyncMock(spec=CryptoLendingReader)
    service = CryptoLendingRiskService(
        reader=reader,
        default_gap_pct=Decimal("0.15"),
        supported_asset_ids=[_ASSET_ID],
    )
    return service, reader


@pytest.mark.parametrize(
    "raw_value",
    ["0.5", 0.5, "1", 1, "0", 0],
    ids=["str-decimal", "float", "str-int", "int", "str-zero", "zero"],
)
def test_post_real_service_coerces_gap_pct_from_json_types(
    client: TestClient,
    raw_value: object,
) -> None:
    """Regression: JSON gap_pct as str/int/float must coerce, not crash with TypeError.

    The reader is rigged to raise MissingShareError so we assert a 503 — proving
    the request reached the reader (i.e. coercion + range-check succeeded). A
    TypeError from a bad ``Decimal <= str`` comparison would short-circuit
    before the reader was ever called.
    """
    service, reader = _real_gap_sweep_service()
    reader.get_receipt_token = AsyncMock(side_effect=MissingShareError("seeded"))
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([service]))

    response = client.post(
        "/v1/risk/rrc",
        json={
            "asset_id": _ASSET_ID,
            "prime_id": _PRIME_ID,
            "overrides": {"gap_sweep": {"gap_pct": raw_value}},
        },
    )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "share_data_missing"
    reader.get_receipt_token.assert_awaited_once()


def test_post_real_service_returns_422_on_out_of_range_gap_pct(client: TestClient) -> None:
    service, _ = _real_gap_sweep_service()
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([service]))

    response = client.post(
        "/v1/risk/rrc",
        json={
            "asset_id": _ASSET_ID,
            "prime_id": _PRIME_ID,
            "overrides": {"gap_sweep": {"gap_pct": "1.5"}},
        },
    )

    assert response.status_code == 422
    assert "gap_pct" in response.json()["detail"]


def test_post_real_service_returns_422_on_unparseable_gap_pct(client: TestClient) -> None:
    service, _ = _real_gap_sweep_service()
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([service]))

    response = client.post(
        "/v1/risk/rrc",
        json={
            "asset_id": _ASSET_ID,
            "prime_id": _PRIME_ID,
            "overrides": {"gap_sweep": {"gap_pct": "not-a-number"}},
        },
    )

    assert response.status_code == 422


def test_post_real_service_returns_422_on_unknown_override_key(client: TestClient) -> None:
    service, _ = _real_gap_sweep_service()
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([service]))

    response = client.post(
        "/v1/risk/rrc",
        json={
            "asset_id": _ASSET_ID,
            "prime_id": _PRIME_ID,
            "overrides": {"gap_sweep": {"nonsense": "x"}},
        },
    )

    assert response.status_code == 422
    assert "nonsense" in response.json()["detail"]


def test_post_real_service_returns_422_on_oversize_gap_pct_string(client: TestClient) -> None:
    """CPU-burn guard: the string is rejected before Decimal parses it."""
    service, _ = _real_gap_sweep_service()
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([service]))

    response = client.post(
        "/v1/risk/rrc",
        json={
            "asset_id": _ASSET_ID,
            "prime_id": _PRIME_ID,
            "overrides": {"gap_sweep": {"gap_pct": "0." + "1" * 100}},
        },
    )

    assert response.status_code == 422
    assert "too long" in response.json()["detail"]


# ---------------------------------------------------------------------------
# Real SurafRrcService — exposure overrides + bounds.
# ---------------------------------------------------------------------------


def _real_suraf_service() -> SurafRrcService:
    from datetime import datetime, timezone

    rating = SurafResult(
        rating_id="aave_ausdc",
        version="v7",
        crr_pct=Decimal("33.7"),
        unadjusted_crr_pct=Decimal("32.7"),
        penalty_pp=Decimal("1.0"),
        avg_score=Decimal("50"),
        source_commit_sha="abc123",
        loaded_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    return SurafRrcService(
        asset_to_rating={_ASSET_ID: "aave_ausdc"},
        suraf_ratings={"aave_ausdc": rating},
        allocation_repo=AsyncMock(spec=AllocationRepository),
    )


def test_post_real_suraf_returns_422_on_oversize_usd_exposure_string(client: TestClient) -> None:
    """CPU-burn guard for SURAF override."""
    service = _real_suraf_service()
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([service]))

    response = client.post(
        "/v1/risk/rrc",
        json={
            "asset_id": _ASSET_ID,
            "prime_id": _PRIME_ID,
            "overrides": {"suraf": {"usd_exposure": "1" + "0" * 100}},
        },
    )

    assert response.status_code == 422
    assert "too long" in response.json()["detail"]


def test_post_real_suraf_returns_422_when_usd_exposure_above_max(client: TestClient) -> None:
    """1e16 > 1e15 cap."""
    service = _real_suraf_service()
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([service]))

    response = client.post(
        "/v1/risk/rrc",
        json={
            "asset_id": _ASSET_ID,
            "prime_id": _PRIME_ID,
            "overrides": {"suraf": {"usd_exposure": "1e16"}},
        },
    )

    assert response.status_code == 422
    assert "usd_exposure" in response.json()["detail"]


def test_post_real_suraf_quantizes_rrc_to_usd_cents(client: TestClient) -> None:
    """rrc_usd is rendered with at most 2 decimal places (USD cents)."""
    service = _real_suraf_service()
    app.dependency_overrides[get_model_registry] = _override_registry(ModelRegistry([service]))

    # 333.333... * 33.7 / 100 = 112.333... — would have a long decimal tail
    # without quantization.
    response = client.post(
        "/v1/risk/rrc",
        json={
            "asset_id": _ASSET_ID,
            "prime_id": _PRIME_ID,
            "overrides": {"suraf": {"usd_exposure": "333.333333333333"}},
        },
    )

    assert response.status_code == 200
    rrc_usd = response.json()["results"][0]["rrc_usd"]
    # Decimal serialised as a string; assert the value has at most 2 decimal places.
    assert "." in rrc_usd
    decimals = rrc_usd.split(".", 1)[1]
    assert len(decimals) <= 2, f"expected 2 decimals max, got {rrc_usd!r}"
