from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.api.v1.allocations import AllocationResponse
from app.domain.entities.allocation import ChainMetadata, EthAddress, Prime, ProtocolMetadata
from app.domain.entities.allocation_activity import AllocationActivityEvent
from app.domain.entities.allocation_category import AllocationCategory
from app.domain.entities.time_series_bucket import AllocationActivityBucket
from app.main import app
from app.services.allocation_service import AllocationService
from tests.factories import (
    ANCHORAGE_FROZEN_AS_OF,
    make_anchorage_custody_holding,
    make_direct_asset_holding,
    make_receipt_token_position,
)

_VALID_ADDR = "0x" + "ab" * 20

_SPARK_MAINNET_ALM = "0x1601843c5e9bc251a3272907010afa41fa18347e"
_SPARK_BASE_ALM = "0x2917956eff0b5eaf030abdb4ef4296df775009ca"
_SPARK_AVALANCHE_ALM = "0xece6b0e8a54c2f44e066fbb9234e7157b15b7fec"

_SPARK_VAULT = "0x691a6c29e9e96dd897718305427ad5d534db16ba"


def _vault_address_for(name: str) -> str:
    """Default vault address for `_prime()`, keyed by prime name.

    `prime.vault_address` is UNIQUE in the schema, so a single shared default
    across differently-named primes (e.g. spark and grove in the same test)
    would encode a state the schema forbids. `spark` keeps the realistic
    `_SPARK_VAULT` constant other tests assert against; every other name gets
    a value derived from itself so no two names collide.
    """
    if name == "spark":
        return _SPARK_VAULT
    return "0x" + name.encode().hex().ljust(40, "0")[:40]


def _prime(
    address: str,
    *,
    name: str = "spark",
    chain_id: int = 1,
    chain: str | None = "mainnet",
    prime_vault_address: str | None = None,
) -> Prime:
    return Prime(
        id=address,
        name=name,
        address=address,
        chain_id=chain_id,
        chain=chain,
        role="alm",
        prime_vault_address=prime_vault_address if prime_vault_address is not None else _vault_address_for(name),
    )


@pytest.fixture(autouse=True)
def _clear_dependency_overrides():
    yield
    app.dependency_overrides.clear()


def _make_service(
    primes=None,
    positions=None,
    direct_holdings=None,
    anchorage_holdings=None,
    *,
    exists: bool = True,
    primary_proxy: str | None = _SPARK_MAINNET_ALM,
) -> AsyncMock:
    service = AsyncMock(spec=AllocationService)
    service.list_primes.return_value = primes or []
    service.list_receipt_token_positions.return_value = positions or []
    service.list_direct_asset_holdings.return_value = direct_holdings or []
    service.list_anchorage_custody_holdings.return_value = anchorage_holdings or []
    service.prime_exists.return_value = exists
    service.list_activity_buckets.return_value = []
    # Which proxy carries the prime's prime-scoped rows is a fact about the
    # indexed data, so the repository answers it; tests state the answer.
    service.primary_proxy_address.return_value = primary_proxy
    return service


def _override_service(service: AsyncMock):
    async def _dep():
        yield service

    return _dep


def test_list_primes_labels_each_proxy_with_chain_and_role():
    from app.api.v1 import allocations

    service = _make_service(
        primes=[
            _prime(_SPARK_MAINNET_ALM, chain_id=1, chain="mainnet"),
            _prime(_SPARK_AVALANCHE_ALM, chain_id=43114, chain="avalanche-c"),
        ]
    )
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    response = TestClient(app).get("/v1/primes")

    assert response.status_code == 200
    assert [(row["name"], row["chain_id"], row["chain"], row["role"]) for row in response.json()] == [
        ("spark", 1, "mainnet", "alm"),
        ("spark", 43114, "avalanche-c", "alm"),
    ]


def test_list_primes_keeps_the_existing_id_name_address_fields():
    from app.api.v1 import allocations

    service = _make_service(primes=[_prime(_SPARK_MAINNET_ALM)])
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    response = TestClient(app).get("/v1/primes")

    row = response.json()[0]
    assert row["id"] == _SPARK_MAINNET_ALM
    assert row["name"] == "spark"
    assert row["address"] == _SPARK_MAINNET_ALM


def test_list_primes_exposes_the_prime_vault_address_as_a_grouping_key():
    from app.api.v1 import allocations

    service = _make_service(
        primes=[
            _prime(_SPARK_MAINNET_ALM, chain_id=1, chain="mainnet"),
            _prime(_SPARK_AVALANCHE_ALM, chain_id=43114, chain="avalanche-c"),
        ]
    )
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    response = TestClient(app).get("/v1/primes")

    assert {row["prime_vault_address"] for row in response.json()} == {_SPARK_VAULT}


def test_list_primes_marks_the_redundant_id_field_deprecated():
    schema = app.openapi()["components"]["schemas"]["PrimeResponse"]["properties"]

    assert schema["id"]["deprecated"] is True


def test_list_primes_does_not_deprecate_the_address_field():
    schema = app.openapi()["components"]["schemas"]["PrimeResponse"]["properties"]

    assert "deprecated" not in schema["address"]


def test_list_primes_returns_200_with_prime_names():
    from app.api.v1 import allocations

    service = _make_service(
        primes=[
            _prime("0xaaa", name="grove", chain=None),
            _prime("0xbbb", name="spark", chain=None),
        ]
    )
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/primes")

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": "0xaaa",
            "name": "grove",
            "address": "0xaaa",
            "chain_id": 1,
            "chain": None,
            "role": "alm",
            "prime_vault_address": _vault_address_for("grove"),
        },
        {
            "id": "0xbbb",
            "name": "spark",
            "address": "0xbbb",
            "chain_id": 1,
            "chain": None,
            "role": "alm",
            "prime_vault_address": _SPARK_VAULT,
        },
    ]


def test_list_primes_returns_empty_list_when_no_primes():
    from app.api.v1 import allocations

    service = _make_service(primes=[])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/primes")

    assert response.status_code == 200
    assert response.json() == []


def test_list_allocations_returns_200_with_enriched_holdings():
    from app.api.v1 import allocations

    position = make_receipt_token_position()
    service = _make_service(positions=[position])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    data = response.json()
    assert data == [
        {
            "chain_id": 1,
            "position_keys": ["token:1", "position:1:0x" + "a" * 40],
            "source": "indexed",
            "network": None,
            "wallet_address": None,
            "receipt_token_id": 1,
            "receipt_token_address": "0x" + "a" * 40,
            "held_token_address": None,
            "underlying_token_id": 10,
            "underlying_token_address": "0x" + "b" * 40,
            "symbol": "aUSDC",
            "underlying_symbol": "USDC",
            "protocol_name": "aave_v3",
            "balance": "100.0",
            "amount_usd": None,
            "reference_amount_usd": None,
            "reference_synced_at": None,
            "latest_activity_at": None,
            "latest_activity_action": None,
            "latest_activity_amount": None,
            "category": "allocation",
            "scope": "proxy",
        }
    ]
    service.list_receipt_token_positions.assert_awaited_once_with(EthAddress(_VALID_ADDR))


def test_a_wrapper_priced_through_its_underlying_is_not_keyed_on_it():
    """A wrapper STL has no registry entry for keys on itself, not its underlying.

    `sparkPrimeUSDC1` is held as a direct asset priced through USDC, so its row
    reports USDC as its underlying. Keying it there matched it to Sky's own
    plain-USDC row — which reports $0 — so the merged row claimed Sky valued a
    $20.3M position at nothing while Sky's real row for it went unjoined.

    Its own address is what it answers to, whether or not an underlying is
    projected: a genuine direct holding *is* the token it holds too (pinned by
    the test below), so both shapes key the same way.
    """
    from app.api.v1 import allocations

    holding = make_direct_asset_holding(
        symbol="sparkPrimeUSDC1",
        token_address="0x" + "d" * 40,
        underlying_token_id=3,
        underlying_token_address="0x" + "e" * 40,
        underlying_symbol="USDC",
    )
    service = _make_service(direct_holdings=[holding])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    (row,) = response.json()
    # Still reports the underlying it is priced through; only the key ignores it.
    assert row["underlying_token_address"] == "0x" + "e" * 40
    assert row["held_token_address"] == "0x" + "d" * 40
    assert row["position_keys"] == ["position:1:0x" + "d" * 40]


def test_a_wrapper_and_the_asset_it_is_priced_through_never_share_a_key():
    """The wrapper and a plain holding of its underlying are two positions.

    Both are direct holdings on the same chain naming USDC as their underlying,
    which is exactly the pair that collapsed into one when the underlying's
    address keyed both.
    """
    from app.api.v1 import allocations

    usdc_address = "0x" + "e" * 40
    wrapper = make_direct_asset_holding(
        symbol="sparkPrimeUSDC1",
        token_id=3,
        token_address="0x" + "d" * 40,
        underlying_token_id=4,
        underlying_token_address=usdc_address,
        underlying_symbol="USDC",
    )
    plain_usdc = make_direct_asset_holding(symbol="USDC", token_id=4, token_address=usdc_address)
    service = _make_service(direct_holdings=[wrapper, plain_usdc])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    wrapper_row, plain_row = response.json()
    assert wrapper_row["position_keys"] == ["position:1:0x" + "d" * 40]
    assert plain_row["position_keys"] == [f"position:1:{usdc_address}"]
    assert not set(wrapper_row["position_keys"]) & set(plain_row["position_keys"])


def test_list_allocations_returns_direct_asset_rows_with_null_receipt_fields():
    """Direct holdings (e.g. raw PYUSD in a proxy) surface as their own rows.
    receipt_token_id / receipt_token_address / protocol_name are null; symbol
    and underlying_symbol both name the held asset; category defaults to ASSET.
    A holding with no oracle price carries a null amount_usd.
    """
    from app.api.v1 import allocations

    holding = make_direct_asset_holding()
    service = _make_service(direct_holdings=[holding])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    assert response.json() == [
        {
            "chain_id": 1,
            # A direct holding is the asset itself, so the asset's address keys it.
            "position_keys": ["position:1:0x" + "c" * 40],
            "source": "indexed",
            "network": None,
            "wallet_address": None,
            "receipt_token_id": None,
            "receipt_token_address": None,
            "held_token_address": "0x" + "c" * 40,
            "underlying_token_id": 99,
            "underlying_token_address": "0x" + "c" * 40,
            "symbol": "PYUSD",
            "underlying_symbol": "PYUSD",
            "protocol_name": None,
            "balance": "250.0",
            "amount_usd": None,
            "reference_amount_usd": None,
            "reference_synced_at": None,
            "latest_activity_at": None,
            "latest_activity_action": None,
            "latest_activity_amount": None,
            "category": "asset",
            "scope": "proxy",
        }
    ]
    service.list_direct_asset_holdings.assert_awaited_once_with(EthAddress(_VALID_ADDR))


def test_list_allocations_prices_direct_asset_holding_from_oracle():
    """A direct holding with an oracle price surfaces its USD value rather than null."""
    from app.api.v1 import allocations

    holding = make_direct_asset_holding(balance=Decimal("250.0"), amount_usd=Decimal("249.5"))
    service = _make_service(direct_holdings=[holding])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    rows = response.json()
    assert rows[0]["symbol"] == "PYUSD"
    assert rows[0]["amount_usd"] == "249.5"


def test_list_allocations_surfaces_latest_activity_action_and_amount():
    """The most recent flow's direction and token-unit magnitude ride along the
    same row that supplies ``latest_activity_at``.
    """
    from app.api.v1 import allocations

    position = make_receipt_token_position(
        latest_activity_at=datetime(2026, 5, 7, 12, 0, tzinfo=UTC),
        latest_activity_action="out",
        latest_activity_amount=Decimal("12.5"),
    )
    service = _make_service(positions=[position])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    row = response.json()[0]
    assert row["latest_activity_action"] == "out"
    assert row["latest_activity_amount"] == "12.5"


def test_list_allocations_surfaces_underlying_metadata_when_holding_carries_it():
    """A direct holding priced from its underlying (allowlisted, e.g. a Uni V3
    pool position valued in USDC) reports the underlying's identity in the
    underlying_* fields; ``symbol`` stays the held token's own.
    """
    from app.api.v1 import allocations

    holding = make_direct_asset_holding(
        amount_usd=Decimal("249.5"),
        underlying_token_id=10,
        underlying_token_address="0x" + "d" * 40,
        underlying_symbol="USDC",
    )
    service = _make_service(direct_holdings=[holding])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    row = response.json()[0]
    assert row["underlying_token_id"] == 10
    assert row["underlying_token_address"] == "0x" + "d" * 40
    assert row["underlying_symbol"] == "USDC"
    assert row["symbol"] == "PYUSD"


def test_list_allocations_falls_back_to_held_token_when_no_underlying_metadata():
    """A direct holding without underlying metadata keeps the current behavior:
    underlying_* mirror the held token itself.
    """
    from app.api.v1 import allocations

    holding = make_direct_asset_holding()
    service = _make_service(direct_holdings=[holding])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    row = response.json()[0]
    assert row["underlying_token_id"] == 99
    assert row["underlying_token_address"] == "0x" + "c" * 40
    assert row["underlying_symbol"] == "PYUSD"


def test_list_allocations_partial_underlying_metadata_falls_back_as_a_unit():
    """The underlying identity is atomic: a holding carrying only part of it
    (the repository projects all-or-nothing, so a partial set means a bug or a
    hand-built entity) must fall back to the held token for ALL three fields,
    never compose a hybrid such as the underlying's id with the held symbol.
    """
    from app.api.v1 import allocations

    holding = make_direct_asset_holding(
        underlying_token_id=10,
        underlying_token_address="0x" + "d" * 40,
        underlying_symbol=None,
    )
    service = _make_service(direct_holdings=[holding])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    row = response.json()[0]
    assert row["underlying_token_id"] == 99
    assert row["underlying_token_address"] == "0x" + "c" * 40
    assert row["underlying_symbol"] == "PYUSD"


def test_list_allocations_combines_receipt_and_direct_rows():
    from app.api.v1 import allocations

    service = _make_service(
        positions=[make_receipt_token_position()],
        direct_holdings=[make_direct_asset_holding()],
    )
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 2
    by_symbol = {row["symbol"]: row for row in rows}
    assert by_symbol["aUSDC"]["receipt_token_id"] == 1
    assert by_symbol["PYUSD"]["receipt_token_id"] is None


def test_list_allocations_surfaces_anchorage_custody_row():
    """Off-chain Anchorage BTC custody is a third row shape: BTC symbol, null
    token/receipt fields, ``anchorage`` protocol, CUSTODY category, chain_id 0
    (off-chain sentinel), and the loan (exposure) as ``amount_usd``.
    """
    from app.api.v1 import allocations

    holding = make_anchorage_custody_holding()
    service = _make_service(anchorage_holdings=[holding])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_SPARK_MAINNET_ALM}/allocations")

    assert response.status_code == 200
    assert response.json() == [
        {
            "chain_id": 0,
            # Off-chain custody keys on its protocol: it is the one thing the two
            # provenances describe the same way.
            "position_keys": ["custody:anchorage"],
            "source": "indexed",
            "network": None,
            "wallet_address": None,
            "receipt_token_id": None,
            "receipt_token_address": None,
            "held_token_address": None,
            "underlying_token_id": None,
            "underlying_token_address": None,
            "symbol": "BTC",
            "underlying_symbol": "BTC",
            "protocol_name": "anchorage",
            "balance": "4722.61",
            "amount_usd": "250000000",
            "reference_amount_usd": None,
            "reference_synced_at": None,
            "latest_activity_at": ANCHORAGE_FROZEN_AS_OF.isoformat(),
            "latest_activity_action": None,
            "latest_activity_amount": None,
            "category": "custody",
            "scope": "prime",
        }
    ]
    service.list_anchorage_custody_holdings.assert_awaited_once_with(EthAddress(_SPARK_MAINNET_ALM))


def test_list_allocations_combines_receipt_direct_and_custody_rows():
    """All three sources union into one response."""
    from app.api.v1 import allocations

    service = _make_service(
        positions=[make_receipt_token_position()],
        direct_holdings=[make_direct_asset_holding()],
        anchorage_holdings=[make_anchorage_custody_holding()],
    )
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_SPARK_MAINNET_ALM}/allocations")

    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 3
    by_symbol = {row["symbol"]: row for row in rows}
    assert by_symbol["aUSDC"]["receipt_token_id"] == 1
    assert by_symbol["PYUSD"]["receipt_token_id"] is None
    assert by_symbol["BTC"]["category"] == "custody"
    assert by_symbol["BTC"]["amount_usd"] == "250000000"


def test_list_allocations_custody_row_surfaces_frozen_snapshot_time_verbatim():
    """The feed is frozen upstream; the stale snapshot_time must surface as-is
    (honest staleness), not be hidden or replaced with 'now'.
    """
    from app.api.v1 import allocations

    holding = make_anchorage_custody_holding(as_of=ANCHORAGE_FROZEN_AS_OF)
    service = _make_service(anchorage_holdings=[holding])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_SPARK_MAINNET_ALM}/allocations")

    assert response.status_code == 200
    row = response.json()[0]
    assert row["latest_activity_at"] == ANCHORAGE_FROZEN_AS_OF.isoformat()
    assert row["latest_activity_action"] is None
    assert row["latest_activity_amount"] is None


def test_list_allocations_includes_the_custody_leg_for_the_primary_proxy():
    from app.api.v1 import allocations

    service = _make_service(anchorage_holdings=[make_anchorage_custody_holding()])
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    response = TestClient(app).get(f"/v1/primes/{_SPARK_MAINNET_ALM}/allocations")

    assert response.status_code == 200
    rows = [row for row in response.json() if row["symbol"] == "BTC"]
    assert len(rows) == 1


def test_list_allocations_tags_the_custody_leg_as_prime_scoped():
    from app.api.v1 import allocations

    service = _make_service(anchorage_holdings=[make_anchorage_custody_holding()])
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    response = TestClient(app).get(f"/v1/primes/{_SPARK_MAINNET_ALM}/allocations")

    row = next(row for row in response.json() if row["symbol"] == "BTC")
    assert row["scope"] == "prime"


def test_list_allocations_omits_the_custody_leg_for_a_non_primary_proxy():
    from app.api.v1 import allocations

    service = _make_service(anchorage_holdings=[make_anchorage_custody_holding()])
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    response = TestClient(app).get(f"/v1/primes/{_SPARK_AVALANCHE_ALM}/allocations")

    assert response.status_code == 200
    assert [row for row in response.json() if row["symbol"] == "BTC"] == []


def test_list_allocations_does_not_query_custody_for_a_non_primary_proxy():
    from app.api.v1 import allocations

    service = _make_service(anchorage_holdings=[make_anchorage_custody_holding()])
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    TestClient(app).get(f"/v1/primes/{_SPARK_AVALANCHE_ALM}/allocations")

    service.list_anchorage_custody_holdings.assert_not_called()


def test_list_allocations_includes_the_custody_leg_for_a_primary_proxy_unknown_to_the_contract():
    """Attribution follows the indexed data, not the contract pin.

    A proxy the pinned axis-synome contract has not been told about — the state
    during a chain onboarding — still carries the prime-scoped leg when it is the
    prime's primary, because withholding it there would make the row unreachable.
    """
    from app.api.v1 import allocations

    service = _make_service(anchorage_holdings=[make_anchorage_custody_holding()], primary_proxy=_VALID_ADDR)
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    response = TestClient(app).get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    rows = [row for row in response.json() if row["symbol"] == "BTC"]
    assert len(rows) == 1


def test_list_allocations_omits_the_custody_leg_for_a_non_primary_proxy_unknown_to_the_contract():
    """The double-count this gate exists to prevent.

    A proxy absent from the contract but present in the data used to be treated
    as its own primary, so it served a second copy of the $250M leg while the
    prime's real primary served the first — and a consumer unioning a prime's
    proxies counted it twice.
    """
    from app.api.v1 import allocations

    service = _make_service(anchorage_holdings=[make_anchorage_custody_holding()], primary_proxy=_SPARK_MAINNET_ALM)
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    response = TestClient(app).get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    assert [row for row in response.json() if row["symbol"] == "BTC"] == []


def test_list_allocations_withholds_the_custody_leg_when_no_primary_resolves():
    """Unreachable after the prime_exists gate, so it is logged rather than guessed."""
    from app.api.v1 import allocations

    service = _make_service(anchorage_holdings=[make_anchorage_custody_holding()], primary_proxy=None)
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    with patch("app.api.v1.allocations.logger") as mock_logger:
        response = TestClient(app).get(f"/v1/primes/{_SPARK_MAINNET_ALM}/allocations")

    assert [row for row in response.json() if row["symbol"] == "BTC"] == []
    mock_logger.error.assert_called_once()


def test_list_allocations_matches_the_primary_proxy_case_insensitively():
    """`/v1/primes` serves lowercase addresses; a caller may checksum-case the path."""
    from app.api.v1 import allocations

    service = _make_service(
        anchorage_holdings=[make_anchorage_custody_holding()],
        primary_proxy=_SPARK_MAINNET_ALM,
    )
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    response = TestClient(app).get(f"/v1/primes/{_SPARK_MAINNET_ALM.upper().replace('0X', '0x')}/allocations")

    assert [row for row in response.json() if row["symbol"] == "BTC"] != []


def test_list_allocations_tags_on_chain_rows_as_proxy_scoped():
    from app.api.v1 import allocations

    service = _make_service(direct_holdings=[make_direct_asset_holding()])
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    response = TestClient(app).get(f"/v1/primes/{_SPARK_MAINNET_ALM}/allocations")

    assert response.json()[0]["scope"] == "proxy"


@pytest.mark.parametrize(
    ("underlying_token_id", "underlying_token_address"),
    [
        (10, None),  # id set, address null
        (None, "0x" + "d" * 40),  # address set, id null
    ],
)
def test_allocation_response_rejects_half_set_underlying_identity(underlying_token_id, underlying_token_address):
    """The underlying id/address are two halves of one identity; a row with only
    one set is contradictory and rejected at construction.
    """
    with pytest.raises(ValidationError, match="must be set or null together"):
        AllocationResponse(
            chain_id=1,
            symbol="X",
            underlying_symbol="X",
            balance=Decimal("1"),
            category=AllocationCategory.ASSET,
            underlying_token_id=underlying_token_id,
            underlying_token_address=underlying_token_address,
        )


@pytest.mark.parametrize(
    ("underlying_token_id", "underlying_token_address"),
    [
        (10, "0x" + "d" * 40),  # both set: receipt/direct shape
        (None, None),  # both null: off-chain custody shape
    ],
)
def test_allocation_response_accepts_paired_underlying_identity(underlying_token_id, underlying_token_address):
    """Both-set (receipt/direct) and both-null (off-chain custody) are valid."""
    response = AllocationResponse(
        chain_id=1,
        symbol="X",
        underlying_symbol="X",
        balance=Decimal("1"),
        category=AllocationCategory.ASSET,
        underlying_token_id=underlying_token_id,
        underlying_token_address=underlying_token_address,
    )
    assert response.underlying_token_id == underlying_token_id
    assert response.underlying_token_address == underlying_token_address


def test_list_allocations_returns_empty_when_prime_exists_with_no_holdings():
    """A registered prime that has fully exited all positions returns 200+[],
    not 404 — only unknown primes (no history at all) trigger 404.
    """
    from app.api.v1 import allocations

    service = _make_service(positions=[], exists=True)
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    assert response.json() == []
    service.prime_exists.assert_awaited_once_with(EthAddress(_VALID_ADDR))


def test_list_allocations_returns_404_when_prime_missing():
    from app.api.v1 import allocations

    service = _make_service(exists=False)
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 404
    assert response.json()["detail"] == "Prime not found"
    service.prime_exists.assert_awaited_once_with(EthAddress(_VALID_ADDR))
    service.list_receipt_token_positions.assert_not_awaited()
    service.list_direct_asset_holdings.assert_not_awaited()


def test_list_allocations_returns_422_for_invalid_prime_id():
    from app.api.v1 import allocations

    service = _make_service(positions=[])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/primes/0xdeadbeef/allocations")

    assert response.status_code == 422
    service.prime_exists.assert_not_awaited()
    service.list_receipt_token_positions.assert_not_awaited()


def test_list_chains_returns_200_with_chain_rows():
    from app.api.v1 import allocations

    service = _make_service()
    service.list_chains.return_value = [
        ChainMetadata(chain_id=1, name="Ethereum"),
        ChainMetadata(chain_id=10, name="Optimism"),
    ]
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/chains")

    assert response.status_code == 200
    assert response.json() == [
        {"chain_id": 1, "name": "Ethereum"},
        {"chain_id": 10, "name": "Optimism"},
    ]
    service.list_chains.assert_awaited_once()


def test_list_protocols_returns_200_with_protocol_rows():
    from app.api.v1 import allocations

    service = _make_service()
    service.list_protocols.return_value = [
        ProtocolMetadata(id=1, chain_id=1, encode="aave_v3", name="Aave V3"),
        ProtocolMetadata(id=2, chain_id=1, encode="spark", name="SparkLend"),
    ]
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/protocols")

    assert response.status_code == 200
    assert response.json() == [
        {"id": 1, "chain_id": 1, "encode": "aave_v3", "name": "Aave V3"},
        {"id": 2, "chain_id": 1, "encode": "spark", "name": "SparkLend"},
    ]
    service.list_protocols.assert_awaited_once()


def test_list_allocation_activity_returns_rows_and_forwards_filters():
    from app.api.v1 import allocations

    from_ts = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    to_ts = datetime(2026, 1, 2, 0, 0, tzinfo=UTC)

    service = _make_service()
    service.list_allocation_activity.return_value = [
        AllocationActivityEvent(
            chain_id=1,
            prime_address=_VALID_ADDR,
            prime_name="spark",
            protocol_name="Aave V3",
            token_id=1,
            token_symbol="USDC",
            action_type="in",
            tx_amount=Decimal("100.0"),
            balance=Decimal("200.0"),
            tx_hash="0x" + "ab" * 32,
            log_index=1,
            block_number=100,
            block_version=0,
            created_at=from_ts,
        )
    ]
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(
        "/v1/allocations/activity",
        params={
            "prime_id": _VALID_ADDR,
            "chain_id": 1,
            "protocol_name": "aave",
            "action_type": "in",
            "token_symbol": "usdc",
            "tx_hash": "0x" + "ab" * 32,
            "from_timestamp": from_ts.isoformat().replace("+00:00", "Z"),
            "to_timestamp": to_ts.isoformat().replace("+00:00", "Z"),
            "limit": 50,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "raw"
    assert len(payload["data"]) == 1
    assert payload["data"][0]["token_symbol"] == "USDC"

    kwargs = service.list_allocation_activity.await_args.kwargs
    assert kwargs["prime_id"] == EthAddress(_VALID_ADDR)
    assert kwargs["chain_id"] == 1
    assert kwargs["protocol_name"] == "aave"
    assert kwargs["action_type"] == "in"
    assert kwargs["token_symbol"] == "usdc"
    assert kwargs["tx_hash"] == "0x" + "ab" * 32
    assert kwargs["from_timestamp"] == from_ts
    assert kwargs["to_timestamp"] == to_ts
    assert kwargs["limit"] == 50


def test_list_allocation_activity_returns_aggregated_buckets():
    from app.api.v1 import allocations

    service = _make_service()
    service.list_activity_buckets.return_value = [
        AllocationActivityBucket(
            bucket_start=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
            event_count=3,
            total_tx_amount=Decimal("450.5"),
            net_flow_usd=Decimal("-120.25"),
        )
    ]
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(
        "/v1/allocations/activity",
        params={
            "from_timestamp": "2026-01-01T00:00:00Z",
            "to_timestamp": "2026-01-02T00:00:00Z",
            "aggregate": "true",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "aggregated"
    assert payload["data"] == [
        {
            "bucket_start": "2026-01-01T12:00:00Z",
            "event_count": 3,
            "total_tx_amount": "450.5",
            "net_flow_usd": "-120.25",
        }
    ]
    kwargs = service.list_activity_buckets.await_args.kwargs
    assert kwargs["bucket_seconds"] == 5 * 60  # 24h window -> PT5M default
    service.list_allocation_activity.assert_not_awaited()


def test_list_allocation_activity_returns_422_for_invalid_prime_id():
    from app.api.v1 import allocations

    service = _make_service()
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(
        "/v1/allocations/activity",
        params={"prime_id": "0xdeadbeef"},
    )

    assert response.status_code == 422
    service.list_allocation_activity.assert_not_awaited()


def test_list_allocation_activity_hides_synthetic_sweep_tx_hash():
    from app.api.v1 import allocations

    created_at = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    service = _make_service()
    service.list_allocation_activity.return_value = [
        AllocationActivityEvent(
            chain_id=1,
            prime_address=_VALID_ADDR,
            prime_name="spark",
            protocol_name="SparkLend",
            token_id=1,
            token_symbol="spUSDC",
            action_type="sweep",
            tx_amount=Decimal("0"),
            balance=Decimal("200.0"),
            tx_hash="0x" + "cd" * 32,
            log_index=0,
            block_number=100,
            block_version=0,
            created_at=created_at,
        )
    ]
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/allocations/activity", params={"action_type": "sweep"})

    assert response.status_code == 200
    assert response.json()["data"][0]["tx_hash"] is None


def test_list_allocation_activity_returns_200_empty_for_unknown_valid_prime_id():
    """Valid-format prime_id with no rows is a filter miss, not a missing resource → 200 []."""
    from app.api.v1 import allocations

    service = _make_service()
    service.list_allocation_activity.return_value = []
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    unknown_addr = "0x" + "ee" * 20
    response = client.get(
        "/v1/allocations/activity",
        params={"prime_id": unknown_addr},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["mode"] == "raw"
    assert body["data"] == []
    service.list_allocation_activity.assert_awaited_once()
    assert service.list_allocation_activity.await_args.kwargs["prime_id"] == EthAddress(unknown_addr)


def test_list_allocation_activity_returns_422_for_limit_out_of_range():
    from app.api.v1 import allocations

    service = _make_service()
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    too_small = client.get("/v1/allocations/activity", params={"limit": 0})
    too_large = client.get("/v1/allocations/activity", params={"limit": 1001})

    assert too_small.status_code == 422
    assert too_large.status_code == 422
    service.list_allocation_activity.assert_not_awaited()


def test_list_allocation_activity_returns_500_when_service_raises_value_error():
    from app.api.v1 import allocations

    service = _make_service()
    service.list_allocation_activity.side_effect = ValueError("query failed")
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/v1/allocations/activity")

    assert response.status_code == 500
    assert response.json() == {"detail": "Failed to retrieve allocation activity"}


def test_list_allocation_activity_returns_422_for_wide_window_without_filter():
    from app.api.v1 import allocations

    service = _make_service()
    service.list_allocation_activity.return_value = []
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(
        "/v1/allocations/activity",
        params={
            "from_timestamp": "2026-01-01T00:00:00Z",
            "to_timestamp": "2026-03-15T00:00:00Z",  # > 30d, no selective filter
        },
    )

    assert response.status_code == 422
    assert "selective filter" in response.json()["detail"]
    service.list_allocation_activity.assert_not_awaited()


def test_list_allocation_activity_allows_wide_window_with_prime_id_filter():
    from app.api.v1 import allocations

    service = _make_service()
    service.list_allocation_activity.return_value = []
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(
        "/v1/allocations/activity",
        params={
            "prime_id": _VALID_ADDR,
            "from_timestamp": "2026-01-01T00:00:00Z",
            "to_timestamp": "2026-03-15T00:00:00Z",
            "resolution": "PT6H",
        },
    )

    assert response.status_code == 200
    service.list_allocation_activity.assert_awaited_once()


def test_list_allocation_activity_returns_422_for_invalid_tx_hash():
    from app.api.v1 import allocations

    service = _make_service()
    service.list_allocation_activity.return_value = []
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/allocations/activity", params={"tx_hash": "not-a-hash"})

    assert response.status_code == 422
    service.list_allocation_activity.assert_not_awaited()


def test_list_allocation_activity_accepts_uppercase_0x_tx_hash():
    from app.api.v1 import allocations

    service = _make_service()
    service.list_allocation_activity.return_value = []
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/allocations/activity", params={"tx_hash": "0X" + "AB" * 32})

    assert response.status_code == 200
    assert service.list_allocation_activity.await_args.kwargs["tx_hash"] == "0x" + "AB" * 32


def test_list_allocation_activity_sets_public_cache_control_on_pinned_window():
    from app.api.v1 import allocations

    service = _make_service()
    service.list_allocation_activity.return_value = []
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(
        "/v1/allocations/activity",
        params={
            "from_timestamp": "2026-03-05T00:00:00Z",
            "to_timestamp": "2026-03-05T12:00:00Z",
        },
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "public, max-age=300"


def test_list_allocation_activity_sets_no_store_when_bounds_not_pinned():
    from app.api.v1 import allocations

    service = _make_service()
    service.list_allocation_activity.return_value = []
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/allocations/activity")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"


def test_get_data_sources_returns_200_with_sources():
    client = TestClient(app)

    response = client.get("/v1/data-sources")

    assert response.status_code == 200
    data = response.json()
    assert "sources" in data
    assert isinstance(data["sources"], list)
    assert len(data["sources"]) > 0
