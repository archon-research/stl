from typing import Protocol
from unittest.mock import AsyncMock

import pytest


class ProtocolScopedBackedBreakdownRepository(Protocol):
    async def get_backed_breakdown(self, backed_asset_id: int): ...


class ProtocolMetadataRepository(Protocol):
    """Spec-only dependency for looking up protocol routing metadata."""

    async def get_protocol_type(self, protocol_id: int) -> str: ...


@pytest.fixture
def mock_protocol_metadata_repository() -> AsyncMock:
    return AsyncMock(spec=ProtocolMetadataRepository)


@pytest.fixture
def mock_sparklend_repository() -> AsyncMock:
    return AsyncMock(spec=ProtocolScopedBackedBreakdownRepository)


@pytest.fixture
def mock_morpho_repository() -> AsyncMock:
    return AsyncMock(spec=ProtocolScopedBackedBreakdownRepository)


@pytest.fixture
def resolver(
    mock_protocol_metadata_repository: AsyncMock,
    mock_sparklend_repository: AsyncMock,
    mock_morpho_repository: AsyncMock,
):
    from app.services.backed_breakdown_repository_resolver import BackedBreakdownRepositoryResolver

    return BackedBreakdownRepositoryResolver(
        protocol_metadata_repository=mock_protocol_metadata_repository,
        aave_like_repository=mock_sparklend_repository,
        morpho_repository=mock_morpho_repository,
    )


# ---------------------------------------------------------------------------
# Spec section: repository selection is explicit from protocol_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolves_morpho_repository_from_morpho_blue_protocol_type(
    resolver,
    mock_protocol_metadata_repository: AsyncMock,
    mock_morpho_repository: AsyncMock,
    mock_sparklend_repository: AsyncMock,
) -> None:
    mock_protocol_metadata_repository.get_protocol_type.return_value = "morpho_blue"

    result = await resolver.resolve(protocol_id=7)

    assert result is mock_morpho_repository
    mock_protocol_metadata_repository.get_protocol_type.assert_awaited_once_with(7)
    mock_sparklend_repository.get_backed_breakdown.assert_not_called()


@pytest.mark.asyncio
async def test_resolves_default_repository_from_sparklend_protocol_type(
    resolver,
    mock_protocol_metadata_repository: AsyncMock,
    mock_sparklend_repository: AsyncMock,
) -> None:
    mock_protocol_metadata_repository.get_protocol_type.return_value = "sparklend"

    result = await resolver.resolve(protocol_id=1)

    assert result is mock_sparklend_repository
    mock_protocol_metadata_repository.get_protocol_type.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_resolves_default_repository_from_aave_protocol_type(
    resolver,
    mock_protocol_metadata_repository: AsyncMock,
    mock_sparklend_repository: AsyncMock,
) -> None:
    mock_protocol_metadata_repository.get_protocol_type.return_value = "aave_v3"

    result = await resolver.resolve(protocol_id=2)

    assert result is mock_sparklend_repository
    mock_protocol_metadata_repository.get_protocol_type.assert_awaited_once_with(2)


@pytest.mark.asyncio
async def test_propagates_protocol_metadata_lookup_errors_unchanged(
    resolver,
    mock_protocol_metadata_repository: AsyncMock,
) -> None:
    mock_protocol_metadata_repository.get_protocol_type.side_effect = RuntimeError("protocol lookup failed")

    with pytest.raises(RuntimeError, match="protocol lookup failed"):
        await resolver.resolve(protocol_id=999)


@pytest.mark.asyncio
async def test_rejects_unsupported_protocol_type(
    resolver,
    mock_protocol_metadata_repository: AsyncMock,
) -> None:
    mock_protocol_metadata_repository.get_protocol_type.return_value = "compound_v2"

    with pytest.raises(ValueError, match="unsupported protocol"):
        await resolver.resolve(protocol_id=3)
