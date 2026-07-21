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
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from sqlalchemy.exc import OperationalError

from scripts import crosscheck_sparkusdtbc as cc
from scripts.crosscheck_sparkusdtbc import (
    Snapshot,
    compare_snapshots,
    main,
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


def test_parse_spark_response_refuses_balance_with_no_assets() -> None:
    """The exact false-pass the script guards: a balance but no parseable asset rows
    would compare equal to an empty our-side and report a spurious PASS."""
    with pytest.raises(ValueError, match="no parseable asset rows"):
        parse_spark_response({"balance": "100", "assets": []})


def test_compare_snapshots_tolerance_boundary_is_inclusive() -> None:
    """A field whose divergence exactly equals tolerance × max passes; just beyond fails."""
    spark = Snapshot(balance=Decimal("1000"), underlying={"USDT": Decimal("1000")})
    # diff == 0.001 * max(1000, 999) == 1.0 exactly → inclusive pass.
    at_edge = Snapshot(balance=Decimal("1000"), underlying={"USDT": Decimal("999")})
    just_over = Snapshot(balance=Decimal("1000"), underlying={"USDT": Decimal("998.999")})

    assert compare_snapshots(spark, at_edge, rel_tolerance=Decimal("0.001")).passed
    assert not compare_snapshots(spark, just_over, rel_tolerance=Decimal("0.001")).passed


def test_main_returns_0_when_comparison_passes() -> None:
    with patch.object(cc, "run", AsyncMock(return_value=0)):
        assert main(["--database-url", "postgresql://x/db"]) == 0


def test_main_returns_1_when_comparison_mismatches() -> None:
    with patch.object(cc, "run", AsyncMock(return_value=1)):
        assert main(["--database-url", "postgresql://x/db"]) == 1


def test_main_returns_2_when_no_database_url() -> None:
    assert main(["--database-url", ""]) == 2


def test_main_returns_2_on_db_error() -> None:
    with patch.object(cc, "run", AsyncMock(side_effect=OperationalError("stmt", {}, Exception("down")))):
        assert main(["--database-url", "postgresql://x/db"]) == 2


def test_main_returns_2_on_network_error() -> None:
    with patch.object(cc, "run", AsyncMock(side_effect=httpx.ConnectError("unreachable"))):
        assert main(["--database-url", "postgresql://x/db"]) == 2
