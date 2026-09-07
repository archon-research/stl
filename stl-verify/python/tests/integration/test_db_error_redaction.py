"""A failed prime-filtered query must not put the allow-list in a log line.

``allowed_vaults`` is the caller's whole authorization set, and it travels into
the SQL as a bind parameter. A ``StatementError`` renders its bind parameters
into its own string, the repositories log that string (and its traceback), and
the structured formatter serialises the lot — so without ``hide_parameters`` on
the engine the allow-list ships to Loki on every database error. That is the
disclosure ``_loggable_params`` exists to prevent, arriving by another route.

Asserting the engine kwarg is set proves nothing about what SQLAlchemy renders,
so this drives a real statement error through the real dialect and inspects the
line the production ``JsonFormatter`` actually emits. The marker assertion is
the control: it fails if the query never bound its parameters, which would make
the "no address in the line" assertion vacuous.
"""

import json
import logging
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.adapters.postgres.engine import create_db_engine
from app.adapters.postgres.reference_as_of import utc_now
from app.domain.entities.allocation import EthAddress
from app.logging import JsonFormatter

_APP_LOGGER_NAME = "app"

# Addresses that appear nowhere else in the schema or the seed data, so any
# occurrence in a log line can only have come from the bind parameters.
ALLOW_LIST = [EthAddress("0x" + pair * 20) for pair in ("a1", "b2", "c3")]

# What SQLAlchemy writes in place of the parameters when they are hidden.
_HIDDEN_MARKER = "[SQL parameters hidden due to hide_parameters=True]"


def _renderings(vault: EthAddress) -> tuple[str, ...]:
    """Every shape the address takes between the bind parameter and the line.

    The parameter is raw ``BYTEA``, so the leak is a bytes repr rather than the
    0x form a reader would grep for; all three are checked so the test cannot
    pass merely because the address was re-encoded on the way out.
    """
    raw = vault.to_bytes()
    return (str(vault), raw.hex(), repr(raw))


class _Collector(logging.Handler):
    """Keeps the formatted line, which is what actually leaves the process."""

    def __init__(self) -> None:
        super().__init__()
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(self.format(record))


@contextmanager
def _emitted_lines() -> Iterator[list[str]]:
    """Collect what the app logger tree emits, formatted as production does."""
    handler = _Collector()
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger(_APP_LOGGER_NAME)
    original_level = logger.level
    logger.setLevel(logging.ERROR)
    logger.addHandler(handler)
    try:
        yield handler.lines
    finally:
        logger.removeHandler(handler)
        logger.setLevel(original_level)


def _flatten(line: str) -> str:
    """The line's own values, unescaped, as one searchable string."""
    return " ".join(str(value) for value in json.loads(line).values())


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def broken_engine(async_db_url: str) -> Any:
    """An engine over a schema whose ``prime`` table is gone.

    Both queries under test join it, so every read fails with a real 42P01 from
    the driver rather than a hand-built exception. The module owns its own
    database, so the rename is not visible to anything else.
    """
    engine = create_db_engine(async_db_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("ALTER TABLE prime RENAME TO prime_renamed_by_test"))
        yield engine
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "run_query",
    [
        pytest.param(lambda repo: repo.list_primes(allowed_vaults=ALLOW_LIST), id="list_primes"),
        pytest.param(
            lambda repo: repo.list_allocation_activity(allowed_vaults=ALLOW_LIST, limit=10),
            id="list_allocation_activity",
        ),
    ],
)
@pytest.mark.asyncio(loop_scope="module")
async def test_a_failed_prime_filtered_query_logs_no_vault_address(
    broken_engine: AsyncEngine,
    run_query: Callable[[AllocationRepository], Awaitable[object]],
) -> None:
    repository = AllocationRepository(broken_engine, utc_now)

    with _emitted_lines() as lines, pytest.raises(ValueError) as raised:
        await run_query(repository)

    assert lines, "the failure logged nothing, so the redaction was never exercised"
    # The error string is re-raised to the API layer, which logs it in turn.
    haystacks = [_flatten(line) for line in lines] + [str(raised.value)]
    for haystack in haystacks:
        for vault in ALLOW_LIST:
            for rendering in _renderings(vault):
                assert rendering not in haystack, f"allow-list entry {vault} reached a log line as {rendering!r}"
    assert any(_HIDDEN_MARKER in haystack for haystack in haystacks), (
        f"no rendered bind parameters in {haystacks}; the query failed before binding, "
        "so the assertions above passed vacuously"
    )
