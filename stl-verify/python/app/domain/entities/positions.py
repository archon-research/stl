from pydantic import BaseModel


class AssetAmount(BaseModel):
    token_address: str
    symbol: str
    amount: int


class UserLatestPositions(BaseModel):
    user_address: str
    debt: list[AssetAmount]
    collateral: list[AssetAmount]
