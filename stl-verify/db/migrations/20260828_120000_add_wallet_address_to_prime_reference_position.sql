-- Grove's positions feed legitimately carries the same (network, token_address)
-- under TWO wallet_addresses (two ALM proxies) -- verified live: the Uni V3
-- LP pair holds ~$1.02M under one proxy and ~$29.0M under the other. The
-- client rejected the second row as a duplicate identity
-- ("sky positions for prime "grove" repeat identity ... the row identity
-- assumption no longer holds"), failing every reference-capital-indexer cycle
-- since deploy. wallet_address is real identity, not incidental data, so it
-- must join the row's PK. `prime_reference_position` is empty on staging (its
-- creating migration 20260826_121000 has never had a successful cycle land),
-- so this ALTER carries no backfill.

-- A columnstore-enabled hypertable refuses ADD COLUMN ... NOT NULL with no
-- DEFAULT outright (SQLSTATE 0A000), even on an empty table. The table has no
-- rows to backfill, so the default is dropped immediately after: every future
-- insert supplies wallet_address explicitly, and none ever relies on it.
ALTER TABLE prime_reference_position
    ADD COLUMN wallet_address TEXT NOT NULL DEFAULT '';

ALTER TABLE prime_reference_position
    ALTER COLUMN wallet_address DROP DEFAULT;

ALTER TABLE prime_reference_position
    DROP CONSTRAINT prime_reference_position_pkey;

ALTER TABLE prime_reference_position
    ADD PRIMARY KEY (prime_id, synced_at, network, token_address, wallet_address, processing_version);

-- Build-aware processing-version trigger with advisory lock (ADR-0002 §3),
-- re-declared with wallet_address folded into the natural key: both the
-- advisory-lock key and the two lookup WHERE clauses. plan_cache_mode is
-- re-declared because CREATE OR REPLACE FUNCTION resets a function's settings.
CREATE OR REPLACE FUNCTION assign_processing_version_prime_reference_position()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('prp|%s|%s|%s|%s|%s',
            NEW.prime_id, EXTRACT(epoch FROM NEW.synced_at), NEW.network, NEW.token_address, NEW.wallet_address), 0));

    SELECT processing_version INTO existing_ver
    FROM prime_reference_position
    WHERE prime_id      = NEW.prime_id
      AND synced_at     = NEW.synced_at
      AND network       = NEW.network
      AND token_address = NEW.token_address
      AND wallet_address = NEW.wallet_address
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM prime_reference_position
        WHERE prime_id      = NEW.prime_id
          AND synced_at     = NEW.synced_at
          AND network       = NEW.network
          AND token_address = NEW.token_address
          AND wallet_address = NEW.wallet_address;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SET plan_cache_mode = 'force_custom_plan';

-- ============================================================================
-- Catalogue metadata (COMMENT ON), consistent with 20260609 add_schema_comments.
-- ============================================================================
COMMENT ON TABLE prime_reference_position IS
  '[Hypertable] Per-cycle snapshot of a prime''s balance-sheet positions as reported by Sky''s internal feed, partitioned on synced_at. Reference data, not STL''s own indexing. Position-level counterpart of prime_reference_balance_sheet''s daily aggregates, and a different question from prime_capital_stack_allocation (balance sheet vs risk-capital breakdown). The feed publishes no history, so rows can only be accumulated forward, never backfilled. Identity fields are upstream''s claims verbatim, not registry FKs -- registry resolution happens at read time. Row identity is (prime, cycle, network, token_address, wallet_address): the same token address legitimately recurs under a prime''s different proxy wallets (verified live on grove, whose Uni V3 LP position is split across two proxies at materially different balances), so wallet_address is part of the PK, not incidental data.';

COMMENT ON COLUMN prime_reference_position.wallet_address IS 'Upstream wallet_address: which of the prime''s proxy wallets holds the position, as upstream reports it. Part of PK -- the same token address can appear under multiple proxies for one prime (verified live on grove). allocation_type remains unrecorded, mirroring the serving layer''s decision.';

INSERT INTO migrations (filename)
VALUES ('20260828_120000_add_wallet_address_to_prime_reference_position.sql')
ON CONFLICT (filename) DO NOTHING;
