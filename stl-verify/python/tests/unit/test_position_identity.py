import pytest

from app.domain.position_identity import PositionFacts, position_identities


def matches(left: PositionFacts, right: PositionFacts) -> bool:
    """Two rows are the same position when they share any candidate key."""
    return bool(set(position_identities(left)) & set(position_identities(right)))


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
    assert matches(facts(receipt_token_id=736), facts(receipt_token_id=None))


def test_one_side_carrying_only_an_id_and_the_other_only_an_address_still_matches():
    # The risk-capital breakdown is exactly this: STL's rows have the registry
    # id and no address, Sky's have both. An address-first single key sent them
    # to different buckets and nothing merged.
    indexed = facts(receipt_token_id=736, position_address=None)
    reference = facts(receipt_token_id=736)

    assert matches(indexed, reference)


def test_a_shared_id_outranks_a_differing_address():
    # The registry id is STL's own, so two rows carrying the same one are the
    # same token however each side spells its address.
    assert matches(
        facts(receipt_token_id=736),
        facts(receipt_token_id=736, position_address="0x" + "ff" * 20),
    )


def test_address_case_does_not_split_a_position():
    assert matches(facts(position_address=_SP_USDS.upper()), facts(position_address=_SP_USDS))


def test_two_vaults_lending_the_same_asset_stay_distinct():
    # The trap: `sparkUSDTbc` is two Morpho v2 vaults sharing a symbol, and both
    # lend USDT. Keying on the underlying, or on the symbol, merges them.
    first = facts(position_address=_SPARK_USDT_BC, symbol="sparkUSDTbc", protocol_name="morpho")
    second = facts(position_address=_OTHER_USDT_VAULT, symbol="sparkUSDTbc", protocol_name="morpho")

    assert not matches(first, second)


def test_the_same_token_on_two_chains_stays_distinct():
    assert not matches(facts(chain_id=1), facts(chain_id=8453))


def test_off_chain_custody_matches_across_its_two_descriptions():
    # STL files the leg off-chain with no address; upstream reports it on
    # ethereum with one. Nothing but the protocol is shared.
    indexed = facts(chain_id=0, network=None, position_address=None, symbol="BTC", protocol_name="anchorage")
    reference = facts(
        chain_id=1, network="ethereum", position_address="0x" + "49" * 20, symbol="ANCHORAGE", protocol_name="anchorage"
    )

    assert matches(indexed, reference)


@pytest.mark.parametrize("protocol", ["anchorage", "Anchorage", "ANCHORAGE"])
def test_custody_is_recognised_whatever_its_case(protocol: str):
    assert position_identities(facts(protocol_name=protocol)) == ["custody:anchorage"]


def test_a_pool_id_is_not_treated_as_an_address():
    # Uniswap V4 carries a 32-byte pool id in the address field. Two different
    # pools must not collapse just because neither is an address.
    first = facts(position_address=_V4_POOL_ID, symbol="UNI-V4-USDT-USDS", protocol_name="uniswap")
    second = facts(position_address="0x" + "3b" * 32, symbol="UNI-V4-PYUSD-USDS", protocol_name="uniswap")

    assert not position_identities(first)[0].startswith("position:")
    assert not matches(first, second)


def test_the_registry_id_carries_a_position_with_no_usable_address():
    both_sides = facts(position_address=_V4_POOL_ID, receipt_token_id=42)

    assert position_identities(both_sides)[0] == "token:42"


def test_a_chain_with_no_id_keys_on_its_network_name():
    plume = facts(chain_id=None, network="plume", position_address=None, receipt_token_id=None, symbol="ACRDX")
    robinhood = facts(chain_id=None, network="robinhood", position_address=None, receipt_token_id=None, symbol="ACRDX")

    assert not matches(plume, robinhood)


def test_an_underlying_address_is_never_the_key():
    # Both rows lend USDT; if the underlying leaked into the key they would
    # merge. The position address is what identifies a position.
    lending_usdt = facts(position_address=_SPARK_USDT_BC, symbol="sparkUSDTbc")
    holding_usdt = facts(position_address=_USDT, symbol="USDT", protocol_name=None)

    assert not matches(lending_usdt, holding_usdt)


def test_a_position_with_neither_a_chain_id_nor_a_network_cannot_match_anything():
    # Nothing but a symbol left, and no chain to qualify it. Keying these alike
    # would merge two positions that share only a symbol; the union must carry
    # them as the separate rows they are.
    first = facts(chain_id=None, network=None, position_address=None, receipt_token_id=None, symbol="ACRDX")
    second = facts(chain_id=None, network=None, position_address=None, receipt_token_id=None, symbol="ACRDX")

    assert position_identities(first) == []
    assert not matches(first, second)


def test_a_registry_id_identifies_a_position_even_with_no_chain_at_all():
    # The risk-capital breakdown is this case: STL's rows carry the id and
    # neither a chain id nor a chain name.
    assert position_identities(facts(chain_id=None, network=None, position_address=None, receipt_token_id=736)) == [
        "token:736"
    ]
