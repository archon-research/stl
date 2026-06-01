"""Test domain entity validation to ensure business invariants are enforced."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.domain.entities.prime_debt import PrimeDebtSnapshot
from app.domain.entities.protocol_event import ProtocolEvent
from app.domain.entities.token_catalog import TokenMetadata, TokenPriceQuote


class TestPrimeDebtSnapshotValidation:
    """Test PrimeDebtSnapshot validation."""

    def test_valid_snapshot_succeeds(self):
        snapshot = PrimeDebtSnapshot(
            prime_address="0x" + "ab" * 20,
            prime_name="spark",
            ilk_name="ETH-A",
            debt_wad=Decimal("1000.0"),
            block_number=123,
            block_version=0,
            synced_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        assert snapshot.prime_address == "0x" + "ab" * 20

    def test_invalid_prime_address_raises(self):
        with pytest.raises(ValueError, match="prime_address must be a valid Ethereum address"):
            PrimeDebtSnapshot(
                prime_address="not-an-address",
                prime_name="spark",
                ilk_name="ETH-A",
                debt_wad=Decimal("1000.0"),
                block_number=123,
                block_version=0,
                synced_at=datetime(2026, 1, 1, tzinfo=UTC),
            )

    def test_empty_prime_name_raises(self):
        with pytest.raises(ValueError, match="prime_name must be non-empty"):
            PrimeDebtSnapshot(
                prime_address="0x" + "ab" * 20,
                prime_name="",
                ilk_name="ETH-A",
                debt_wad=Decimal("1000.0"),
                block_number=123,
                block_version=0,
                synced_at=datetime(2026, 1, 1, tzinfo=UTC),
            )

    def test_negative_debt_raises(self):
        with pytest.raises(ValueError, match="debt_wad must be non-negative"):
            PrimeDebtSnapshot(
                prime_address="0x" + "ab" * 20,
                prime_name="spark",
                ilk_name="ETH-A",
                debt_wad=Decimal("-100.0"),
                block_number=123,
                block_version=0,
                synced_at=datetime(2026, 1, 1, tzinfo=UTC),
            )

    def test_zero_block_number_raises(self):
        with pytest.raises(ValueError, match="block_number must be positive"):
            PrimeDebtSnapshot(
                prime_address="0x" + "ab" * 20,
                prime_name="spark",
                ilk_name="ETH-A",
                debt_wad=Decimal("1000.0"),
                block_number=0,
                block_version=0,
                synced_at=datetime(2026, 1, 1, tzinfo=UTC),
            )


class TestProtocolEventValidation:
    """Test ProtocolEvent validation."""

    def test_valid_event_succeeds(self):
        event = ProtocolEvent(
            tx_hash="0x" + "ab" * 32,
            log_index=5,
            chain_id=1,
            block_number=123,
            block_version=0,
            protocol_name="spark",
            event_name="Borrow",
            contract_address="0x" + "cd" * 20,
            event_data={"amount": "100"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        assert event.tx_hash == "0x" + "ab" * 32

    def test_invalid_tx_hash_raises(self):
        with pytest.raises(ValueError, match="tx_hash must be a valid transaction hash"):
            ProtocolEvent(
                tx_hash="not-a-hash",
                log_index=5,
                chain_id=1,
                block_number=123,
                block_version=0,
                protocol_name="spark",
                event_name="Borrow",
                contract_address="0x" + "cd" * 20,
                event_data=None,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )

    def test_negative_log_index_raises(self):
        with pytest.raises(ValueError, match="log_index must be non-negative"):
            ProtocolEvent(
                tx_hash="0x" + "ab" * 32,
                log_index=-1,
                chain_id=1,
                block_number=123,
                block_version=0,
                protocol_name="spark",
                event_name="Borrow",
                contract_address="0x" + "cd" * 20,
                event_data=None,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )

    def test_empty_protocol_name_raises(self):
        with pytest.raises(ValueError, match="protocol_name must be non-empty"):
            ProtocolEvent(
                tx_hash="0x" + "ab" * 32,
                log_index=5,
                chain_id=1,
                block_number=123,
                block_version=0,
                protocol_name="",
                event_name="Borrow",
                contract_address="0x" + "cd" * 20,
                event_data=None,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )

    def test_invalid_contract_address_raises(self):
        with pytest.raises(ValueError, match="contract_address must be a valid Ethereum address"):
            ProtocolEvent(
                tx_hash="0x" + "ab" * 32,
                log_index=5,
                chain_id=1,
                block_number=123,
                block_version=0,
                protocol_name="spark",
                event_name="Borrow",
                contract_address="bad-address",
                event_data=None,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )


class TestTokenMetadataValidation:
    """Test TokenMetadata validation."""

    def test_valid_token_succeeds(self):
        token = TokenMetadata(
            id=1,
            chain_id=1,
            address="0x" + "ab" * 20,
            symbol="USDC",
            decimals=6,
            updated_at=datetime(2026, 1, 1, tzinfo=UTC),
            metadata={"kind": "stable"},
        )
        assert token.id == 1

    def test_non_positive_id_raises(self):
        with pytest.raises(ValueError, match="id must be positive"):
            TokenMetadata(
                id=0,
                chain_id=1,
                address="0x" + "ab" * 20,
                symbol="USDC",
                decimals=6,
                updated_at=datetime(2026, 1, 1, tzinfo=UTC),
                metadata=None,
            )

    def test_invalid_address_raises(self):
        with pytest.raises(ValueError, match="address must be a valid Ethereum address"):
            TokenMetadata(
                id=1,
                chain_id=1,
                address="not-an-address",
                symbol="USDC",
                decimals=6,
                updated_at=datetime(2026, 1, 1, tzinfo=UTC),
                metadata=None,
            )

    def test_empty_symbol_raises(self):
        with pytest.raises(ValueError, match="symbol must be non-empty when present"):
            TokenMetadata(
                id=1,
                chain_id=1,
                address="0x" + "ab" * 20,
                symbol="   ",
                decimals=6,
                updated_at=datetime(2026, 1, 1, tzinfo=UTC),
                metadata=None,
            )

    def test_decimals_out_of_range_raises(self):
        with pytest.raises(ValueError, match="decimals must be between 0 and 255"):
            TokenMetadata(
                id=1,
                chain_id=1,
                address="0x" + "ab" * 20,
                symbol="BAD",
                decimals=300,
                updated_at=datetime(2026, 1, 1, tzinfo=UTC),
                metadata=None,
            )


class TestTokenPriceQuoteValidation:
    """Test TokenPriceQuote validation."""

    def test_valid_price_succeeds(self):
        price = TokenPriceQuote(
            token_id=1,
            source_type="onchain",
            source_id=7,
            source_name="chainlink",
            source_display_name="Chainlink",
            price_usd=Decimal("1.00"),
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            staleness_seconds=10,
        )
        assert price.token_id == 1

    def test_invalid_source_type_raises(self):
        with pytest.raises(ValueError, match="source_type must be 'onchain' or 'offchain'"):
            TokenPriceQuote(
                token_id=1,
                source_type="invalid",  # type: ignore
                source_id=7,
                source_name="chainlink",
                source_display_name="Chainlink",
                price_usd=Decimal("1.00"),
                timestamp=datetime(2026, 1, 1, tzinfo=UTC),
                staleness_seconds=10,
            )

    def test_negative_price_raises(self):
        with pytest.raises(ValueError, match="price_usd must be non-negative"):
            TokenPriceQuote(
                token_id=1,
                source_type="onchain",
                source_id=7,
                source_name="chainlink",
                source_display_name="Chainlink",
                price_usd=Decimal("-1.00"),
                timestamp=datetime(2026, 1, 1, tzinfo=UTC),
                staleness_seconds=10,
            )

    def test_empty_source_name_raises(self):
        with pytest.raises(ValueError, match="source_name must be non-empty"):
            TokenPriceQuote(
                token_id=1,
                source_type="onchain",
                source_id=7,
                source_name="",
                source_display_name="Chainlink",
                price_usd=Decimal("1.00"),
                timestamp=datetime(2026, 1, 1, tzinfo=UTC),
                staleness_seconds=10,
            )

    def test_negative_staleness_raises(self):
        with pytest.raises(ValueError, match="staleness_seconds must be non-negative"):
            TokenPriceQuote(
                token_id=1,
                source_type="onchain",
                source_id=7,
                source_name="chainlink",
                source_display_name="Chainlink",
                price_usd=Decimal("1.00"),
                timestamp=datetime(2026, 1, 1, tzinfo=UTC),
                staleness_seconds=-5,
            )


def test_core_model_details_in_rrc_result():
    from decimal import Decimal
    from app.domain.entities.risk import CoreModelDetails, RrcResult

    details = CoreModelDetails(
        risk_model="core_model",
        crr_el_pct=Decimal("12.5"),
        crr_es_pct=Decimal("15.0"),
        crr_var_pct=Decimal("10.0"),
        hhi=Decimal("22.3"),
        protocol="MORPHO",
        forecast_step=14,
        n_mc=10000,
        copula_type="T-COPULA",
    )
    result = RrcResult(
        asset_id=1,
        prime_id="0xBcca60bB61934080951369a648Fb03DF4F96263C",
        rrc_usd=Decimal("1250.00"),
        comparable_crr_pct=Decimal("12.5"),
        risk_model="core_model",
        details=details,
    )
    assert result.risk_model == "core_model"
    assert result.details.crr_el_pct == Decimal("12.5")


def test_core_model_details_null_hhi():
    from decimal import Decimal
    from app.domain.entities.risk import CoreModelDetails

    d = CoreModelDetails(
        risk_model="core_model",
        crr_el_pct=Decimal("5"),
        crr_es_pct=Decimal("6"),
        crr_var_pct=Decimal("4"),
        hhi=None,
        protocol="SPARKLEND",
        forecast_step=7,
        n_mc=1000,
        copula_type="GAUSSIAN",
    )
    assert d.hhi is None
