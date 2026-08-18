from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncEngine

from app.ports.receipt_token_lookup import ReceiptTokenLookup
from app.risk_engine.suraf.result import SurafResult
from app.services.core_model_risk_service import CoreModelRiskService
from app.services.crypto_lending_risk_service import CryptoLendingRiskService
from app.services.model_registry import ModelRegistry


def get_engine(request: Request) -> AsyncEngine:
    """Extract the shared SQLAlchemy engine from application state."""
    return request.app.state.engine


def get_suraf_ratings(request: Request) -> dict[str, SurafResult]:
    """Extract the SURAF rating_id -> result lookup built at startup."""
    return request.app.state.suraf_ratings


def get_asset_to_rating(request: Request) -> dict[int, str]:
    """Extract the receipt_token_id -> rating_id mapping built at startup."""
    return request.app.state.asset_to_rating


def get_crypto_lending_risk_service(request: Request) -> CryptoLendingRiskService:
    """Extract the crypto-lending risk service built at startup."""
    return request.app.state.crypto_lending_risk_service


def get_model_registry(request: Request) -> ModelRegistry:
    """Extract the model registry built at startup."""
    return request.app.state.model_registry


def get_core_model_risk_service(request: Request) -> CoreModelRiskService:
    """Extract the core-model risk service built at startup."""
    return request.app.state.core_model_risk_service


def get_receipt_token_lookup(request: Request) -> ReceiptTokenLookup:
    """Extract the receipt-token lookup built at startup."""
    return request.app.state.receipt_token_lookup
