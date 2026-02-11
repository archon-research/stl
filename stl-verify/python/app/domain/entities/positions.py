from pydantic import BaseModel


class AssetAmount(BaseModel):
    token_address: str
    symbol: str
    amount: int  # Wei amount, will use Decimal later


class UserLatestPositions(BaseModel):
    user_address: str
    debt: list[AssetAmount]
    collateral: list[AssetAmount]
