"""Unit tests for the data.spark.fi cross-check comparison logic.

Pure-function tests only: no network, no DB. Both sides of every comparison are
injected. The Spark fixture (``fixtures/spark_sparkusdtbc_response.json``) is the
schema DERIVED FROM THE TICKET — the live ``data.spark.fi`` host serves a SPA
shell and its BlockAnalitica backend returned 404 for this vault/wallet from the
build sandbox, so the true payload is pinned once the endpoint is reachable.
"""

import json
from decimal import Decimal
from pathlib import Path

from scripts.crosscheck_sparkusdtbc import (
    Snapshot,
    compare_snapshots,
    parse_spark_response,
    rel_tolerance_from_blocks,
)

_FIXTURE = Path(__file__).parent / "fixtures" / "spark_sparkusdtbc_response.json"


def _spark_payload() -> dict:
    return json.loads(_FIXTURE.read_text())


def test_parse_spark_response_extracts_balance_and_underlying() -> None:
    snapshot = parse_spark_response(_spark_payload())

    assert snapshot.balance == Decimal("20000000.00")
    assert snapshot.underlying == {
        "USDT": Decimal("10000000.00"),
        "WETH": Decimal("6000000.00"),
        "WBTC": Decimal("4000000.00"),
    }


def test_rel_tolerance_from_blocks() -> None:
    # 2 blocks × 0.1%/block documented drift = 0.2% relative tolerance.
    assert rel_tolerance_from_blocks(2) == Decimal("0.002")
    assert rel_tolerance_from_blocks(0) == Decimal("0")


def test_compare_snapshots_passes_within_tolerance() -> None:
    spark = parse_spark_response(_spark_payload())
    # Our side drifts < 0.2% on every field (a couple of blocks of accrual).
    ours = Snapshot(
        balance=Decimal("20010000.00"),  # +0.05%
        underlying={
            "USDT": Decimal("10005000.00"),  # +0.05%
            "WETH": Decimal("5994000.00"),  # -0.10%
            "WBTC": Decimal("4004000.00"),  # +0.10%
        },
    )

    result = compare_snapshots(spark, ours, rel_tolerance=rel_tolerance_from_blocks(2))

    assert result.passed
    assert all(f.within_tolerance for f in result.fields)


def test_compare_snapshots_flags_out_of_tolerance_field() -> None:
    spark = parse_spark_response(_spark_payload())
    ours = Snapshot(
        balance=Decimal("20000000.00"),
        underlying={
            "USDT": Decimal("9000000.00"),  # -10%: far outside tolerance
            "WETH": Decimal("6000000.00"),
            "WBTC": Decimal("4000000.00"),
        },
    )

    result = compare_snapshots(spark, ours, rel_tolerance=rel_tolerance_from_blocks(2))

    assert not result.passed
    failed = [f for f in result.fields if not f.within_tolerance]
    assert [f.name for f in failed] == ["USDT"]


def test_compare_snapshots_flags_symbol_missing_on_our_side() -> None:
    spark = parse_spark_response(_spark_payload())
    ours = Snapshot(
        balance=Decimal("20000000.00"),
        underlying={
            "USDT": Decimal("10000000.00"),
            "WETH": Decimal("6000000.00"),
            # WBTC absent on our side → treated as 0 → out of tolerance, surfaced.
        },
    )

    result = compare_snapshots(spark, ours, rel_tolerance=rel_tolerance_from_blocks(2))

    assert not result.passed
    wbtc = next(f for f in result.fields if f.name == "WBTC")
    assert wbtc.our_value == Decimal("0")
    assert not wbtc.within_tolerance


def test_compare_snapshots_zero_vs_zero_passes() -> None:
    spark = Snapshot(balance=Decimal("0"), underlying={"USDT": Decimal("0")})
    ours = Snapshot(balance=Decimal("0"), underlying={"USDT": Decimal("0")})

    result = compare_snapshots(spark, ours, rel_tolerance=Decimal("0"))

    assert result.passed
