from app.domain.entities.positions import AssetAmount, UserLatestPositions


def test_user_latest_positions_accepts_assets():
    asset = AssetAmount(token_address="0xabc", symbol="USDS", amount=123, decimals=18, chain_id=1)
    position = UserLatestPositions(user_address="0xuser", chain_id=1, debt=[asset], collateral=[])

    assert position.user_address == "0xuser"
    assert position.chain_id == 1
    assert position.debt[0].symbol == "USDS"
    assert position.debt[0].decimals == 18
    assert position.debt[0].chain_id == 1
