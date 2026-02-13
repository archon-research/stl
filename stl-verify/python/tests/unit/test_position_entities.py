from app.domain.entities.positions import AssetAmount, UserLatestPositions


def test_user_latest_positions_accepts_assets():
    asset = AssetAmount(token_address="0xabc", symbol="USDS", amount=123)
    position = UserLatestPositions(user_address="0xuser", debt=[asset], collateral=[])

    assert position.user_address == "0xuser"
    assert position.debt[0].symbol == "USDS"
