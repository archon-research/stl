import pytest

from app.domain.position_identity import PositionFacts, position_identity

_SP_USDS = "0x" + "c0" * 20
_SPARK_USDT_BC = "0x" + "b0" * 20
_OTHER_USDT_VAULT = "0x" + "c7" * 20
_USDT = "0x" + "da" * 20
_V4_POOL_ID = "0x" + "e6" * 32


def facts(
    *,
    chain_id: int | None = 1,
    network: str | None = "ethereum",
    position_address: str | None = _SP_USDS,
    receipt_token_id: int | None = None,
    protocol_name: str | None = "sparklend",
    symbol: str = "spUSDS",
) -> PositionFacts:
    return PositionFacts(
        chain_id=chain_id,
        network=network,
        position_address=position_address,
        receipt_token_id=receipt_token_id,
        protocol_name=protocol_name,
        symbol=symbol,
    )


def test_the_same_position_from_either_provenance_shares_a_key():
    # STL resolves the registry id, upstream does not; both carry the address.
    indexed = facts(receipt_token_id=736)
    reference = facts(receipt_token_id=None)

    assert position_identity(indexed) == position_identity(reference)


def test_address_case_does_not_split_a_position():
    assert position_identity(facts(position_address=_SP_USDS.upper())) == position_identity(
        facts(position_address=_SP_USDS)
    )


def test_two_vaults_lending_the_same_asset_stay_distinct():
    # The trap: `sparkUSDTbc` is two Morpho v2 vaults sharing a symbol, and both
    # lend USDT. Keying on the underlying, or on the symbol, merges them.
    first = facts(position_address=_SPARK_USDT_BC, symbol="sparkUSDTbc", protocol_name="morpho")
    second = facts(position_address=_OTHER_USDT_VAULT, symbol="sparkUSDTbc", protocol_name="morpho")

    assert position_identity(first) != position_identity(second)


def test_the_same_token_on_two_chains_stays_distinct():
    assert position_identity(facts(chain_id=1)) != position_identity(facts(chain_id=8453))


def test_off_chain_custody_matches_across_its_two_descriptions():
    # STL files the leg off-chain with no address; upstream reports it on
    # ethereum with one. Nothing but the protocol is shared.
    indexed = facts(chain_id=0, network=None, position_address=None, symbol="BTC", protocol_name="anchorage")
    reference = facts(
        chain_id=1, network="ethereum", position_address="0x" + "49" * 20, symbol="ANCHORAGE", protocol_name="anchorage"
    )

    assert position_identity(indexed) == position_identity(reference)


@pytest.mark.parametrize("protocol", ["anchorage", "Anchorage", "ANCHORAGE"])
def test_custody_is_recognised_whatever_its_case(protocol: str):
    assert position_identity(facts(protocol_name=protocol)) == "custody:anchorage"


def test_a_pool_id_is_not_treated_as_an_address():
    # Uniswap V4 carries a 32-byte pool id in the address field. Two different
    # pools must not collapse just because neither is an address.
    first = facts(position_address=_V4_POOL_ID, symbol="UNI-V4-USDT-USDS", protocol_name="uniswap")
    second = facts(position_address="0x" + "3b" * 32, symbol="UNI-V4-PYUSD-USDS", protocol_name="uniswap")

    assert not position_identity(first).startswith("position:")
    assert position_identity(first) != position_identity(second)


def test_the_registry_id_carries_a_position_with_no_usable_address():
    both_sides = facts(position_address=_V4_POOL_ID, receipt_token_id=42)

    assert position_identity(both_sides) == "token:42"


def test_a_chain_with_no_id_keys_on_its_network_name():
    plume = facts(chain_id=None, network="plume", position_address=None, receipt_token_id=None, symbol="ACRDX")
    robinhood = facts(chain_id=None, network="robinhood", position_address=None, receipt_token_id=None, symbol="ACRDX")

    assert position_identity(plume) != position_identity(robinhood)


def test_an_underlying_address_is_never_the_key():
    # Both rows lend USDT; if the underlying leaked into the key they would
    # merge. The position address is what identifies a position.
    lending_usdt = facts(position_address=_SPARK_USDT_BC, symbol="sparkUSDTbc")
    holding_usdt = facts(position_address=_USDT, symbol="USDT", protocol_name=None)

    assert position_identity(lending_usdt) != position_identity(holding_usdt)
