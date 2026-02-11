import pytest
import pytest_asyncio
from sqlalchemy import text

from app.adapters.postgres.position_repository import PostgresPositionRepository


@pytest_asyncio.fixture
async def repository(db_sessionmaker):
    """Create repository instance for testing."""
    return PostgresPositionRepository(db_sessionmaker)


@pytest.mark.asyncio
async def test_list_latest_user_positions(repository, db_sessionmaker):
    """Test ListLatestUserPositions returns latest non-orphaned positions with limit."""
    
    async with db_sessionmaker() as session:
        # Create test users
        result = await session.execute(
            text("INSERT INTO \"user\" (chain_id, address, first_seen_block) VALUES (1, decode('1111111111111111111111111111111111111111', 'hex'), 100) RETURNING id")
        )
        user_id_1 = result.scalar_one()
        
        result = await session.execute(
            text("INSERT INTO \"user\" (chain_id, address, first_seen_block) VALUES (1, decode('2222222222222222222222222222222222222222', 'hex'), 100) RETURNING id")
        )
        user_id_2 = result.scalar_one()
        
        # Create test tokens
        result = await session.execute(
            text("INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, decode('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'hex'), 'USDS', 18) RETURNING id")
        )
        token_id_usds = result.scalar_one()
        
        result = await session.execute(
            text("INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, decode('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'hex'), 'WBTC', 8) RETURNING id")
        )
        token_id_wbtc = result.scalar_one()
        
        # Get protocol_id (assuming SparkLend is already seeded)
        result = await session.execute(
            text("SELECT id FROM protocol WHERE name = 'SparkLend' LIMIT 1")
        )
        protocol_id = result.scalar_one()
        
        # Create block states (including orphaned block)
        await session.execute(
            text("""
                INSERT INTO block_states (number, version, is_orphaned, hash, parent_hash, received_at)
                VALUES
                    (9, 0, false, decode('09', 'hex'), decode('08', 'hex'), 9),
                    (10, 0, false, decode('0a', 'hex'), decode('09', 'hex'), 10),
                    (10, 1, true, decode('0b', 'hex'), decode('09', 'hex'), 10)
                ON CONFLICT (number, version) DO NOTHING
            """)
        )
        
        # Insert borrower positions at different blocks
        await session.execute(
            text("""
                INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
                VALUES
                    (:user1, :protocol, :usds, 9, 0, '100', '100', 'Borrow', decode('01', 'hex')),
                    (:user1, :protocol, :usds, 10, 0, '150', '50', 'Borrow', decode('02', 'hex')),
                    (:user1, :protocol, :usds, 10, 1, '999', '849', 'Borrow', decode('03', 'hex')),
                    (:user2, :protocol, :usds, 9, 0, '50', '50', 'Borrow', decode('04', 'hex')),
                    (:user2, :protocol, :usds, 10, 0, '0', '-50', 'Repay', decode('05', 'hex'))
            """),
            {
                "user1": user_id_1,
                "user2": user_id_2,
                "protocol": protocol_id,
                "usds": token_id_usds,
            }
        )
        
        # Insert collateral positions
        await session.execute(
            text("""
                INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, collateral_enabled)
                VALUES
                    (:user1, :protocol, :usds, 9, 0, '2', '2', 'Supply', decode('11', 'hex'), true),
                    (:user1, :protocol, :usds, 10, 0, '4', '2', 'Supply', decode('12', 'hex'), true),
                    (:user1, :protocol, :usds, 10, 1, '9', '5', 'Supply', decode('13', 'hex'), true),
                    (:user1, :protocol, :wbtc, 9, 0, '1', '1', 'Supply', decode('14', 'hex'), true),
                    (:user1, :protocol, :wbtc, 10, 0, '0', '-1', 'Withdraw', decode('15', 'hex'), true),
                    (:user2, :protocol, :wbtc, 9, 0, '1', '1', 'Supply', decode('16', 'hex'), true),
                    (:user2, :protocol, :wbtc, 10, 0, '5', '4', 'Supply', decode('17', 'hex'), false)
            """),
            {
                "user1": user_id_1,
                "user2": user_id_2,
                "protocol": protocol_id,
                "usds": token_id_usds,
                "wbtc": token_id_wbtc,
            }
        )
        
        await session.commit()
    
    # Test with limit=1
    positions = await repository.list_latest_user_positions(protocol_id, limit=1)
    
    assert len(positions) == 1, f"Expected 1 user with limit=1, got {len(positions)}"
    
    position = positions[0]
    assert position.user_address == "1111111111111111111111111111111111111111"
    
    # Should have 1 debt entry (USDS with amount 150 from block 10 version 0, not version 1)
    assert len(position.debt) == 1, f"Expected 1 debt entry, got {len(position.debt)}"
    debt = position.debt[0]
    assert debt.token_address == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert debt.symbol == "USDS"
    assert debt.amount == 150
    
    # Should have 1 collateral entry (USDS with amount 4)
    # WBTC should be excluded (amount 0)
    assert len(position.collateral) == 1, f"Expected 1 collateral entry, got {len(position.collateral)}"
    collateral = position.collateral[0]
    assert collateral.token_address == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert collateral.symbol == "USDS"
    assert collateral.amount == 4
    
    # Verify no zero amounts in result
    for asset in position.debt + position.collateral:
        assert asset.amount != 0, f"Found zero amount for {asset.symbol}"
    
    # Test without limit - should return only user1 (user2 has no debt and collateral is disabled)
    positions_all = await repository.list_latest_user_positions(protocol_id, limit=0)
    assert len(positions_all) == 1, f"Expected 1 user total, got {len(positions_all)}"
