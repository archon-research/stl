"""Shared SQL fragments for time-windowed and time-bucketed queries.

Centralizes the timestamp-window predicate and bucket expression so the three
time-series repositories don't each hand-roll (and risk diverging on) the same
``TIMESTAMPTZ`` casts. Bind parameters are fixed by name: ``from_timestamp``,
``to_timestamp`` (both ``TIMESTAMPTZ``) and ``bucket_seconds`` (float seconds).
"""


def optional_time_window_clause(column: str) -> str:
    """Predicate filtering ``column`` to an optional ``[from, to]`` window.

    Either bound may be NULL (the corresponding side is then unbounded). Used by
    the raw-row queries, where the bounds flow through unchanged.
    """
    lower = "CAST(:from_timestamp AS TIMESTAMPTZ)"
    upper = "CAST(:to_timestamp AS TIMESTAMPTZ)"
    return f"AND ({lower} IS NULL OR {column} >= {lower})\n            AND ({upper} IS NULL OR {column} <= {upper})"


def required_time_window_clause(column: str) -> str:
    """Predicate filtering ``column`` to a required ``[from, to]`` window.

    Both bounds must be non-NULL. Used by the aggregated queries, where the
    window always has explicit bounds.
    """
    return (
        f"AND {column} >= CAST(:from_timestamp AS TIMESTAMPTZ)\n"
        f"            AND {column} <= CAST(:to_timestamp AS TIMESTAMPTZ)"
    )


_BUCKET_ORIGIN = "'2000-01-01'::timestamptz"


def time_bucket_expr(column: str) -> str:
    """``date_bin`` expression over ``column`` using the ``bucket_seconds`` bind.

    Anchored at the Postgres epoch (2000-01-01) to match ``gap_policy._BUCKET_ORIGIN``.
    """
    return f"date_bin(make_interval(secs => :bucket_seconds), {column}, {_BUCKET_ORIGIN})"


def gapfill_series_cte(alias: str = "gs") -> str:
    """CTE generating a complete bucket grid for gapfill queries.

    Callers LEFT JOIN their aggregated data onto this grid and use the count-group
    LOCF pattern (``count(d.bucket) OVER ... AS grp`` then ``first_value(agg) OVER
    (PARTITION BY ..., grp ORDER BY bucket)``) for last-observation-carried-forward.
    Bind parameters: ``:from_timestamp``, ``:to_timestamp``, ``:bucket_seconds``.
    """
    return (
        f"{alias} AS (\n"
        f"    SELECT bucket FROM generate_series(\n"
        f"        date_bin(make_interval(secs => :bucket_seconds), CAST(:from_timestamp AS TIMESTAMPTZ), {_BUCKET_ORIGIN}),\n"
        f"        CAST(:to_timestamp AS TIMESTAMPTZ),\n"
        f"        make_interval(secs => :bucket_seconds)\n"
        f"    ) AS bucket\n"
        f")"
    )


def clamp_limit(limit: int, maximum: int) -> int:
    """Clamp a limit into ``[1, maximum]``."""
    return min(max(limit, 1), maximum)
