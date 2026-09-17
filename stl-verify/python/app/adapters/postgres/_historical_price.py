"""Per-bucket historical oracle price SQL (VEC-763).

PR #728 sped up the exposure-buckets read by resolving the (then
bucket-invariant) *latest* price once per ``(token, protocol)`` key instead of
re-probing ``onchain_token_price`` inside a lateral join per bucket. VEC-763
generalizes that same once-per-key shape to a price *series*: each needed
key's price history is pulled as a single bounded scan and locf-gapfilled onto
the query's own bucket grid, rather than a lateral re-probe per bucket (which
was tried and measured at ~17s/request -- the per-bucket-per-token lateral
count that made the pre-728 bug expensive in the first place).

Bind parameters are fixed by name, matching ``_time_window``: ``from_timestamp``,
``to_timestamp`` (TIMESTAMPTZ) and ``bucket_seconds`` (float seconds).
"""


def historical_price_buckets_cte(
    *,
    prefix: str,
    keys_cte: str,
    key_columns: tuple[str, ...],
    token_id_column: str,
    protocol_id_column: str | None,
    oracle_asset_as_of: str,
) -> str:
    """Return a CTE chain resolving ``{prefix}_buckets(<key_columns>, bucket, price_usd)``.

    ``keys_cte`` (aliased ``pk``) must already carry the distinct needed keys.
    When ``protocol_id_column`` is set the price is resolved through that
    protocol's bound oracle (``protocol_oracle``) -- the receipt-token basis;
    otherwise through any oracle with an enabled mapping for the token -- the
    direct-holdings basis (rationale on ``_DIRECT_ASSET_HOLDINGS_SQL``).

    Emits, in order: ``{prefix}_seed`` (one backward probe per key -- same
    once-per-key shape as the series scan below, giving the price carried into
    the window's first bucket), ``{prefix}_changes`` (every in-window price
    change for a needed key, one bounded scan per key rather than per bucket),
    ``{prefix}_points`` (their union) and ``{prefix}_buckets`` (locf-gapfilled
    onto the bucket grid, mirroring how position quantities are bucketed
    elsewhere in this file).
    """
    pk_cols = ", ".join(f"pk.{c}" for c in key_columns)
    bare_cols = ", ".join(key_columns)
    order_cols = f"{pk_cols}, otp.timestamp"

    if protocol_id_column:
        seed_join = (
            f"JOIN protocol_oracle po ON po.oracle_id = otp.oracle_id "
            f"AND po.protocol_id = pk.{protocol_id_column}\n            "
        )
        changes_join = f"JOIN protocol_oracle po ON po.protocol_id = pk.{protocol_id_column}\n    "
        oracle_predicate = "otp.oracle_id = po.oracle_id"
    else:
        seed_join = ""
        changes_join = ""
        oracle_predicate = "TRUE"

    return f"""{prefix}_seed AS (
    -- One backward probe per key from {keys_cte} (a handful of rows), not per
    -- bucket: the once-per-key shape #728 uses for the latest price, here
    -- seeding the carry-in before the key's first in-window price change.
    SELECT {pk_cols}, seed.price_usd
    FROM {keys_cte} pk
    LEFT JOIN LATERAL (
        SELECT otp.price_usd
        FROM onchain_token_price otp
        {seed_join}WHERE otp.token_id = pk.{token_id_column}
          AND otp.timestamp < CAST(:from_timestamp AS TIMESTAMPTZ)
          AND EXISTS (
              SELECT 1 FROM {oracle_asset_as_of} oa
              WHERE oa.oracle_id = otp.oracle_id AND oa.token_id = otp.token_id AND oa.enabled
          )
        ORDER BY otp.block_number DESC, otp.block_version DESC,
                 otp.processing_version DESC, otp.oracle_id DESC
        LIMIT 1
    ) seed ON TRUE
),
{prefix}_changes AS (
    -- Every in-window price change for a needed key: one scan bounded by
    -- token + [from, to], not a lateral probe per bucket.
    SELECT DISTINCT ON ({order_cols})
        {pk_cols}, otp.timestamp, otp.price_usd
    FROM {keys_cte} pk
    {changes_join}JOIN onchain_token_price otp
        ON otp.token_id = pk.{token_id_column} AND {oracle_predicate}
    WHERE otp.timestamp >= CAST(:from_timestamp AS TIMESTAMPTZ)
      AND otp.timestamp <= CAST(:to_timestamp AS TIMESTAMPTZ)
      AND EXISTS (
          SELECT 1 FROM {oracle_asset_as_of} oa
          WHERE oa.oracle_id = otp.oracle_id AND oa.token_id = otp.token_id AND oa.enabled
      )
    ORDER BY {order_cols}, otp.block_number DESC, otp.block_version DESC,
             otp.processing_version DESC, otp.oracle_id DESC
),
{prefix}_points AS (
    SELECT {bare_cols}, CAST(:from_timestamp AS TIMESTAMPTZ) AS timestamp, price_usd
    FROM {prefix}_seed
    WHERE price_usd IS NOT NULL
    UNION ALL
    SELECT {bare_cols}, timestamp, price_usd FROM {prefix}_changes
),
{prefix}_buckets AS (
    SELECT
        {bare_cols},
        time_bucket_gapfill(
            make_interval(secs => :bucket_seconds), timestamp,
            CAST(:from_timestamp AS TIMESTAMPTZ), CAST(:to_timestamp AS TIMESTAMPTZ)
        ) AS bucket,
        locf(last(price_usd, timestamp)) AS price_usd
    FROM {prefix}_points
    GROUP BY {bare_cols}, bucket
)"""
