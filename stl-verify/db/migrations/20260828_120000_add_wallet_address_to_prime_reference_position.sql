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

-- Prove the emptiness this migration assumes rather than trusting it: the feed
-- publishes no history, so a populated table would have no wallet_address to
-- backfill existing rows with, and minting '' identities for them would be
-- silently wrong rather than loudly refused.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM prime_reference_position) THEN
        RAISE EXCEPTION 'prime_reference_position has rows; wallet_address backfill is impossible because the feed publishes no history. Aborting rather than minting empty-string identities for existing rows.';
    END IF;
END $$;

-- Added nullable, with no NOT NULL and no DEFAULT.
--
-- Two separate TimescaleDB restrictions apply to this table, and satisfying the
-- first is what tripped the second. A columnstore-enabled hypertable refuses
-- ADD COLUMN ... NOT NULL with no DEFAULT (SQLSTATE 0A000), so the first cut of
-- this migration supplied `NOT NULL DEFAULT ''`. But the table also carries a
-- tiering policy (added by its creating migration 20260826_121000), and a
-- tiered table refuses ADD COLUMN with a NOT NULL constraint at all -- default
-- or not, empty or not:
--
--   ERROR: Adding column with NOT NULL constraint is blocked for tiered tables
--          (SQLSTATE XX000)
--
-- which failed the staging migrate Job and every deploy behind it.
--
-- No column-level NOT NULL is needed: wallet_address joins the PRIMARY KEY
-- below, and PK membership makes it NOT NULL. That is also the shape every
-- other ADD COLUMN on these hypertables uses (20260819_100000, 20260702_120000
-- -- all nullable). Dropping the placeholder default along with it removes the
-- window in which a row could acquire an empty-string identity.
ALTER TABLE prime_reference_position
    ADD COLUMN wallet_address TEXT;

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
  '[Hypertable] Per-cycle snapshot of a prime''s balance-sheet positions as reported by Sky''s internal feed, partitioned on synced_at. Reference data, not STL''s own indexing. Position-level counterpart of prime_reference_balance_sheet''s daily aggregates, and a different question from prime_capital_stack_allocation (balance sheet vs risk-capital breakdown). The feed publishes no history, so rows can only be accumulated forward, never backfilled. Identity fields are upstream''s claims verbatim, not registry FKs -- registry resolution happens at read time. Row identity is (prime, cycle, network, token_address, wallet_address): a token address legitimately recurs under a prime''s different proxy wallets, so wallet_address is part of the PK, not incidental data.';

COMMENT ON COLUMN prime_reference_position.wallet_address IS 'Upstream wallet_address: which of the prime''s proxy wallets holds the position, as upstream reports it. Part of PK -- the same token address can appear under multiple proxies for one prime. allocation_type remains unrecorded, mirroring the serving layer''s decision.';

INSERT INTO migrations (filename)
VALUES ('20260828_120000_add_wallet_address_to_prime_reference_position.sql')
ON CONFLICT (filename) DO NOTHING;
