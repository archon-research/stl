"""EIP-55 mixed-case checksum validation for EVM addresses.

Reference: https://eips.ethereum.org/EIPS/eip-55
"""

from eth_hash.auto import keccak

from app.risk_engine._vendored_synome.spec_support.validated_str import ValidatedStr

_HEX_CHARS = frozenset("0123456789abcdefABCDEF")
_ETH_BURN_ADDRESS = "0x0000000000000000000000000000000000000000"


def validate_eip55(v: str) -> str:
    """Validate and return an EIP-55 checksummed Ethereum address & raise ValueError if invalid.

    Also raises ``ValueError`` for the all-zero burn address
    (``0x0000000000000000000000000000000000000000``).
    """
    if not (v.startswith("0x") and len(v) == 42 and all(c in _HEX_CHARS for c in v[2:])):
        raise ValueError(f"Invalid EVM address format: {v!r}")
    if v.lower() == _ETH_BURN_ADDRESS:
        raise ValueError(f"Burn address is not allowed: {v!r}")

    hex_addr = v[2:].lower()
    hashed_address = keccak(hex_addr.encode("ascii")).hex()

    checksummed = "0x" + "".join(
        c if c in "0123456789" else c.upper() if int(hashed_address[i], 16) >= 8 else c for i, c in enumerate(hex_addr)
    )

    if v != checksummed:
        raise ValueError(f"Invalid EIP-55 checksum for address: {v!r}")
    return v


class EvmAddress(ValidatedStr):
    """An EIP-55 checksummed Ethereum address.

    Validates on construction unless ``VALIDATION_DISABLED=1`` is set::

        addr = EvmAddress("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed")
    """

    @classmethod
    def _validate(cls, value: str) -> None:
        validate_eip55(value)
