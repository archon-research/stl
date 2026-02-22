from pydantic import BaseModel


class AssetAmount(BaseModel):
    token_address: str
    symbol: str
    amount: int
    decimals: int
    chain_id: int


class UserLatestPositions(BaseModel):
    user_address: str
    chain_id: int
    debt: list[AssetAmount]
    collateral: list[AssetAmount]
