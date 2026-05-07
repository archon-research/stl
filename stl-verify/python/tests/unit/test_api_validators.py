import pytest

from app.api._validators import TX_HASH_PATTERN, _validate_tx_hash


def test_validate_tx_hash_accepts_prefixed_hash():
    value = "0x" + "ab" * 32

    assert _validate_tx_hash(value) == value


def test_validate_tx_hash_accepts_unprefixed_hash():
    value = "ab" * 32

    assert _validate_tx_hash(value) == value


def test_tx_hash_pattern_accepts_uppercase_prefix():
    value = "0X" + "AB" * 32

    assert _validate_tx_hash(value) == "0x" + "AB" * 32


@pytest.mark.parametrize(
    "value",
    [
        "0xdeadbeef",
        "not-a-hash",
        "0x" + "ab" * 31,
        "0x" + "ab" * 33,
    ],
)
def test_validate_tx_hash_rejects_invalid_values(value: str):
    with pytest.raises(ValueError, match="Invalid transaction hash format"):
        _validate_tx_hash(value)


def test_tx_hash_pattern_is_anchored_and_exact_length():
    assert TX_HASH_PATTERN.startswith("^")
    assert TX_HASH_PATTERN.endswith("$")
