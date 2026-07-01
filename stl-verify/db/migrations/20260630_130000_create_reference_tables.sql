-- Reference data sets: 13 controlled-vocabulary tables for the enrichment layer.
-- DEFERRED: risk_profile_ref + revenue_type_ref (policy-driven, pending sign-off).
-- counterparty_type_ref dropped (= entity_type x internal/external); internal/external -> entity_master.is_internal.
-- PK naming: PK = table name minus _ref; _code suffix only for external standard codes (currency_code, country_code, gics_code, caev_code).
-- Hierarchies: asset_class_ref->security_type_ref->security_subtype_ref ; sector_ref->industry_group_ref (GICS L1->L2).
-- credit_rating_ref = S&P/Moody scale; corporate_action_type_ref = ISO 20022 CAEV.

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger
  LANGUAGE plpgsql AS $$ BEGIN NEW.updated_at = now(); RETURN NEW; END $$;

CREATE TABLE IF NOT EXISTS deal_type_ref (
    deal_type text NOT NULL,
    direction text,
    description text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT deal_type_direction_chk CHECK (((direction IS NULL) OR (direction = ANY (ARRAY['LONG'::text, 'SHORT'::text]))))
);
CREATE TABLE IF NOT EXISTS asset_class_ref (
    asset_class text NOT NULL,
    category text NOT NULL,
    description text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT category_chk CHECK ((category = ANY (ARRAY['TRADITIONAL'::text, 'ALTERNATIVE'::text, 'DIGITAL'::text, 'DERIVATIVE'::text, 'UNKNOWN'::text])))
);
COMMENT ON COLUMN asset_class_ref.category IS 'House rollup category (not an external standard): TRADITIONAL/ALTERNATIVE/DIGITAL/DERIVATIVE';
CREATE TABLE IF NOT EXISTS corporate_action_type_ref (
    corporate_action_type text NOT NULL,
    category text NOT NULL,
    election_type text NOT NULL,
    caev_code text,
    description text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT corporate_action_type_category_chk CHECK ((category = ANY (ARRAY['DISTRIBUTION'::text, 'REORGANIZATION'::text, 'REDEMPTION'::text, 'IDENTIFIER_CHANGE'::text, 'CREDIT_EVENT'::text, 'GOVERNANCE'::text, 'TAX'::text, 'STATUS'::text, 'INFORMATIONAL'::text, 'CRYPTO_EVENT'::text, 'OTHER'::text, 'UNKNOWN'::text]))),
    CONSTRAINT corporate_action_type_election_chk CHECK ((election_type = ANY (ARRAY['MANDATORY'::text, 'VOLUNTARY'::text, 'MANDATORY_WITH_CHOICE'::text, 'NOT_APPLICABLE'::text, 'UNKNOWN'::text])))
);
COMMENT ON TABLE corporate_action_type_ref IS 'Corporate-action TYPE vocabulary, anchored to ISO 20022 CAEV (caev_code) + crypto-native. category is a house rollup; election_type is the typical default (actual mandatory/voluntary CAMV is event-level). Events are a future fact table.';
CREATE TABLE IF NOT EXISTS counterparty_role_ref (
    counterparty_role text NOT NULL,
    description text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
CREATE TABLE IF NOT EXISTS country_ref (
    country_code text NOT NULL,
    iso_code text NOT NULL,
    iso_numeric text NOT NULL,
    country_name text NOT NULL,
    region text NOT NULL,
    sub_region text NOT NULL,
    currency_code text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
COMMENT ON COLUMN country_ref.country_code IS 'ISO 3166-1 alpha-2 (PK)';
COMMENT ON COLUMN country_ref.iso_code IS 'ISO 3166-1 alpha-3';
COMMENT ON COLUMN country_ref.iso_numeric IS 'ISO 3166-1 numeric (3-digit, leading zeros)';
COMMENT ON COLUMN country_ref.region IS 'Geographic continent grouping';
COMMENT ON COLUMN country_ref.sub_region IS 'UN M49 sub-region grouping (tidy labels)';
CREATE TABLE IF NOT EXISTS credit_rating_ref (
    rank smallint NOT NULL,
    sp_rating text NOT NULL,
    moodys_rating text,
    tier text NOT NULL,
    grade text NOT NULL,
    description text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT credit_rating_grade_chk CHECK ((grade = ANY (ARRAY['INVESTMENT_GRADE'::text, 'SPECULATIVE'::text, 'DEFAULT'::text, 'NOT_RATED'::text])))
);
COMMENT ON TABLE credit_rating_ref IS 'Standard long-term credit rating scale (S&P/Fitch + Moody''s). rank is the assigned ordinal spine; tier and grade are rollups. A security''s actual rating (code+agency+as_of) is a time-varying per-instrument attribute, not stored here.';
COMMENT ON COLUMN credit_rating_ref.rank IS 'Assigned canonical ordinal of the scale (1 = highest quality, 22 = D, 99 = NR). The spine: tier and grade are rollups of rank. Sort by this, not by notation.';
COMMENT ON COLUMN credit_rating_ref.tier IS 'Rollup of rank into the descriptive letter band (e.g. AA+/AA/AA- = HIGH_GRADE).';
COMMENT ON COLUMN credit_rating_ref.grade IS 'Coarsest rollup of rank: INVESTMENT_GRADE / SPECULATIVE / DEFAULT / NOT_RATED (the IG/HY line).';
CREATE TABLE IF NOT EXISTS currency_ref (
    currency_code text NOT NULL,
    iso_numeric text NOT NULL,
    currency_name text NOT NULL,
    minor_units smallint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
COMMENT ON COLUMN currency_ref.currency_code IS 'ISO 4217 alpha-3 (PK)';
COMMENT ON COLUMN currency_ref.iso_numeric IS 'ISO 4217 numeric';
COMMENT ON COLUMN currency_ref.minor_units IS 'Number of decimal places for the currency';
CREATE TABLE IF NOT EXISTS entity_type_ref (
    entity_type text NOT NULL,
    description text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
CREATE TABLE IF NOT EXISTS industry_group_ref (
    sector text NOT NULL,
    industry_group text NOT NULL,
    gics_code text,
    description text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
COMMENT ON TABLE industry_group_ref IS 'GICS industry group (level 2), child of sector_ref. 25 GICS groups (post-2023) + OTHER/UNKNOWN. Issuer classifies at sector (required) or industry_group (optional). Sub-industry depth (proprietary, frequently revised) not seeded.';
COMMENT ON COLUMN industry_group_ref.gics_code IS 'GICS 4-digit industry-group code; NULL for sentinels.';
CREATE TABLE IF NOT EXISTS origination_type_ref (
    origination_type text NOT NULL,
    description text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
CREATE TABLE IF NOT EXISTS sector_ref (
    sector text NOT NULL,
    gics_code text,
    description text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
COMMENT ON TABLE sector_ref IS 'Issuer business-sector classification. GICS sector level (gics_code) + house extensions (GOVERNMENT, DIGITAL_ASSETS) for issuers outside GICS. Attribute of the issuer entity (entity_master); supports credit sector-concentration. Sector level only - the proprietary GICS sub-industry depth is not seeded.';
COMMENT ON COLUMN sector_ref.gics_code IS 'GICS sector code (10..60); NULL = house extension outside GICS.';
CREATE TABLE IF NOT EXISTS security_subtype_ref (
    asset_class text NOT NULL,
    security_type text NOT NULL,
    security_subtype text NOT NULL,
    description text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT security_subtype_not_type_chk CHECK (((security_subtype <> security_type) OR (security_subtype = 'UNKNOWN'::text)))
);
CREATE TABLE IF NOT EXISTS security_type_ref (
    asset_class text NOT NULL,
    security_type text NOT NULL,
    description text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
INSERT INTO asset_class_ref VALUES ('EQUITY', 'TRADITIONAL', 'Ownership interests in corporations, listed and depositary', '2026-06-22 14:47:52.769993+00', '2026-06-30 08:24:19.72894+00') ON CONFLICT DO NOTHING;
INSERT INTO asset_class_ref VALUES ('FIXED_INCOME', 'TRADITIONAL', 'Interest-bearing debt securities with contractual coupons and defined maturities', '2026-06-22 14:47:52.769993+00', '2026-06-30 08:24:19.72894+00') ON CONFLICT DO NOTHING;
INSERT INTO asset_class_ref VALUES ('INDEX', 'TRADITIONAL', 'Multi-asset or benchmark-linked instruments', '2026-06-22 14:47:52.769993+00', '2026-06-30 08:24:19.72894+00') ON CONFLICT DO NOTHING;
INSERT INTO asset_class_ref VALUES ('COMMODITY', 'ALTERNATIVE', 'Physical or financial exposure to raw materials, energy, carbon', '2026-06-22 14:47:52.769993+00', '2026-06-30 08:24:19.72894+00') ON CONFLICT DO NOTHING;
INSERT INTO asset_class_ref VALUES ('PRIVATE_CREDIT', 'ALTERNATIVE', 'Directly negotiated, illiquid credit instruments not traded on public markets', '2026-06-22 14:47:52.769993+00', '2026-06-30 08:24:19.72894+00') ON CONFLICT DO NOTHING;
INSERT INTO asset_class_ref VALUES ('PRIVATE_EQUITY', 'ALTERNATIVE', 'Directly negotiated equity ownership in private companies and funds, not traded on public markets', '2026-06-30 08:16:17.565266+00', '2026-06-30 08:24:19.72894+00') ON CONFLICT DO NOTHING;
INSERT INTO asset_class_ref VALUES ('REAL_ESTATE', 'ALTERNATIVE', 'Direct property, development projects, REITs', '2026-06-22 14:47:52.769993+00', '2026-06-30 08:24:19.72894+00') ON CONFLICT DO NOTHING;
INSERT INTO asset_class_ref VALUES ('STRUCTURED_CREDIT', 'ALTERNATIVE', 'Securities backed by pools of underlying assets with tranched risk', '2026-06-22 14:47:52.769993+00', '2026-06-30 08:24:19.72894+00') ON CONFLICT DO NOTHING;
INSERT INTO asset_class_ref VALUES ('DIGITAL_ASSET', 'DIGITAL', 'Native on-chain instruments: L1 tokens, stablecoins, staked assets, LP positions, DeFi yield tokens', '2026-06-22 14:47:52.769993+00', '2026-06-30 08:24:19.72894+00') ON CONFLICT DO NOTHING;
INSERT INTO asset_class_ref VALUES ('DERIVATIVE', 'DERIVATIVE', 'Contracts deriving value from an underlying: options, futures, forwards, swaps', '2026-06-30 08:16:17.565266+00', '2026-06-30 08:24:19.72894+00') ON CONFLICT DO NOTHING;
INSERT INTO asset_class_ref VALUES ('UNKNOWN', 'UNKNOWN', 'Unclassified or pending classification', '2026-06-26 11:53:54.897517+00', '2026-06-30 08:24:19.72894+00') ON CONFLICT DO NOTHING;
INSERT INTO asset_class_ref VALUES ('CASH', 'TRADITIONAL', 'Cash balances and demand deposits at banks or custodians; immediately available, not term instruments', '2026-06-30 08:16:17.565266+00', '2026-06-30 08:27:07.367015+00') ON CONFLICT DO NOTHING;
INSERT INTO asset_class_ref VALUES ('MONEY_MARKET', 'TRADITIONAL', 'Short-term, high-quality liquid instruments: T-bills, commercial paper, repo, tokenised MMFs', '2026-06-22 14:47:52.769993+00', '2026-06-30 08:27:07.367015+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('CASH_DIVIDEND', 'DISTRIBUTION', 'MANDATORY', 'DVCA', 'Cash dividend', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('STOCK_DIVIDEND', 'DISTRIBUTION', 'MANDATORY', 'DVSE', 'Dividend paid in shares', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('DIVIDEND_OPTION', 'DISTRIBUTION', 'MANDATORY_WITH_CHOICE', 'DVOP', 'Dividend with cash/stock election', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('SCRIP_DIVIDEND', 'DISTRIBUTION', 'MANDATORY_WITH_CHOICE', 'DVSC', 'Scrip dividend', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('DIVIDEND_REINVESTMENT', 'DISTRIBUTION', 'VOLUNTARY', 'DRIP', 'Dividend reinvestment plan', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('INTEREST_PAYMENT', 'DISTRIBUTION', 'MANDATORY', 'INTR', 'Interest / coupon payment', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('PAY_IN_KIND', 'DISTRIBUTION', 'MANDATORY', 'PINK', 'Payment in kind', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('CAPITAL_GAINS', 'DISTRIBUTION', 'MANDATORY', 'CAPG', 'Capital gains distribution', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('BONUS_ISSUE', 'DISTRIBUTION', 'MANDATORY', 'BONU', 'Bonus / capitalisation issue', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('RIGHTS_ISSUE', 'DISTRIBUTION', 'VOLUNTARY', 'RHTS', 'Rights issue / subscription', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('INTERMED_SEC_DISTRIBUTION', 'DISTRIBUTION', 'MANDATORY', 'RHDI', 'Intermediate securities distribution', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('STOCK_SPLIT', 'REORGANIZATION', 'MANDATORY', 'SPLF', 'Forward split / subdivision', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('REVERSE_SPLIT', 'REORGANIZATION', 'MANDATORY', 'SPLR', 'Reverse split / consolidation', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('MERGER', 'REORGANIZATION', 'MANDATORY', 'MRGR', 'Merger / consolidation', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('SPINOFF', 'REORGANIZATION', 'MANDATORY', 'SOFF', 'Spin-off', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('CONVERSION', 'REORGANIZATION', 'MANDATORY_WITH_CHOICE', 'CONV', 'Conversion', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('EXCHANGE_OFFER', 'REORGANIZATION', 'VOLUNTARY', 'EXOF', 'Exchange offer', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('TENDER_OFFER', 'REORGANIZATION', 'VOLUNTARY', 'TEND', 'Tender / acquisition offer', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('DUTCH_AUCTION', 'REORGANIZATION', 'VOLUNTARY', 'DTCH', 'Dutch auction', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('REPURCHASE_OFFER', 'REORGANIZATION', 'VOLUNTARY', 'BIDS', 'Issuer repurchase / buyback', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('WARRANT_EXERCISE', 'REORGANIZATION', 'VOLUNTARY', 'EXWA', 'Warrant exercise', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('RIGHTS_EXERCISE', 'REORGANIZATION', 'VOLUNTARY', 'EXRI', 'Exercise of intermediate securities', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('REDEMPTION', 'REDEMPTION', 'MANDATORY', 'REDM', 'Final maturity redemption', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('FULL_CALL', 'REDEMPTION', 'MANDATORY', 'MCAL', 'Full call / early redemption', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('PARTIAL_CALL', 'REDEMPTION', 'MANDATORY', 'PCAL', 'Partial redemption without nominal reduction', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('PARTIAL_REDEMPTION', 'REDEMPTION', 'MANDATORY', 'PRED', 'Partial redemption with nominal (pool factor) reduction', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('PUT_REDEMPTION', 'REDEMPTION', 'VOLUNTARY', 'BPUT', 'Put redemption (holder elects)', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('DRAWING', 'REDEMPTION', 'MANDATORY', 'DRAW', 'Drawing / lottery redemption', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('MATURITY', 'REDEMPTION', 'MANDATORY', NULL, 'Instrument reaches contractual maturity', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('NAME_CHANGE', 'IDENTIFIER_CHANGE', 'MANDATORY', 'CHAN', 'Name / identifier change', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('REDENOMINATION', 'IDENTIFIER_CHANGE', 'MANDATORY', 'REDO', 'Currency redenomination', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('PARI_PASSU', 'IDENTIFIER_CHANGE', 'MANDATORY', 'PARI', 'Assimilation / pari-passu', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('TICKER_CHANGE', 'IDENTIFIER_CHANGE', 'MANDATORY', NULL, 'Ticker / symbol change', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('DEFAULT', 'CREDIT_EVENT', 'MANDATORY', 'DFLT', 'Payment default', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('BANKRUPTCY', 'CREDIT_EVENT', 'MANDATORY', 'BRUP', 'Bankruptcy / insolvency', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('CREDIT_EVENT', 'CREDIT_EVENT', 'MANDATORY', 'CREV', 'Credit event (CDS-style)', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('WORTHLESS', 'CREDIT_EVENT', 'MANDATORY', 'WRTH', 'Security declared worthless', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('LIQUIDATION', 'CREDIT_EVENT', 'MANDATORY', 'LIQU', 'Liquidation distribution', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('RESTRUCTURING', 'CREDIT_EVENT', 'MANDATORY', NULL, 'Debt restructuring', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('AGM', 'GOVERNANCE', 'VOLUNTARY', NULL, 'Annual general meeting (no CAEV code - meetings use the ISO 20022 seev.001+ MeetingNotification family)', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('EGM', 'GOVERNANCE', 'VOLUNTARY', NULL, 'Extraordinary general meeting (no CAEV code - meetings use the ISO 20022 seev.001+ MeetingNotification family)', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('CONSENT', 'GOVERNANCE', 'VOLUNTARY', 'CONS', 'Consent solicitation', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('CLASS_ACTION', 'GOVERNANCE', 'VOLUNTARY', 'CLSA', 'Class action / settlement', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('TAX_RECLAIM', 'TAX', 'VOLUNTARY', 'TREC', 'Tax reclaim', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('WITHHOLDING_RELIEF', 'TAX', 'VOLUNTARY', 'WTRC', 'Withholding tax relief certification', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('TRADING_ACTIVE', 'STATUS', 'NOT_APPLICABLE', 'ACTV', 'Trading status: active', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('TRADING_SUSPENDED', 'STATUS', 'NOT_APPLICABLE', 'SUSP', 'Trading status: suspended', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('DELISTING', 'STATUS', 'NOT_APPLICABLE', 'DLST', 'Delisting', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('INFORMATION', 'INFORMATIONAL', 'NOT_APPLICABLE', 'INFO', 'Information notice', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('DISCLOSURE', 'INFORMATIONAL', 'NOT_APPLICABLE', 'DSCL', 'Beneficial-owner disclosure request', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('TOKEN_MIGRATION', 'CRYPTO_EVENT', 'MANDATORY_WITH_CHOICE', NULL, 'Migration to a new token contract', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('REBASE', 'CRYPTO_EVENT', 'MANDATORY', NULL, 'Automatic supply rebase', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('AIRDROP', 'CRYPTO_EVENT', 'VOLUNTARY', NULL, 'Token airdrop (often claim-based)', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('FORK', 'CRYPTO_EVENT', 'MANDATORY', NULL, 'Chain fork', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('CONTRACT_UPGRADE', 'CRYPTO_EVENT', 'MANDATORY', NULL, 'Protocol / contract upgrade', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('SLASHING', 'CRYPTO_EVENT', 'MANDATORY', NULL, 'Validator stake slashing', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('DEPEG', 'CRYPTO_EVENT', 'MANDATORY', NULL, 'Pegged asset breaks its peg', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('STAKING_REWARD', 'CRYPTO_EVENT', 'MANDATORY', NULL, 'On-chain staking reward distribution', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('GOVERNANCE_VOTE', 'CRYPTO_EVENT', 'VOLUNTARY', NULL, 'On-chain governance vote', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('OTHER', 'OTHER', 'UNKNOWN', 'OTHR', 'Known corporate action not among the listed types', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO corporate_action_type_ref VALUES ('UNKNOWN', 'UNKNOWN', 'UNKNOWN', NULL, 'Unclassified or pending classification', '2026-06-30 11:57:34.488347+00', '2026-06-30 11:57:34.488347+00') ON CONFLICT DO NOTHING;
INSERT INTO counterparty_role_ref VALUES ('ISSUER', 'Issues or sponsors the instrument held', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO counterparty_role_ref VALUES ('CUSTODIAN', 'Holds assets in custody for Sky/Prime', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO counterparty_role_ref VALUES ('TRADING_COUNTERPARTY', 'The other side of a trade or lending transaction', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO counterparty_role_ref VALUES ('PROTOCOL_OPERATOR', 'Operates the on-chain contract a position is held through', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO counterparty_role_ref VALUES ('UNKNOWN', 'Unclassified or pending classification', '2026-06-26 11:53:54.897517+00', '2026-06-26 11:53:54.897517+00') ON CONFLICT DO NOTHING;
INSERT INTO counterparty_role_ref VALUES ('BORROWER', 'Entity that borrows in a lending/credit position', '2026-06-30 10:35:46.242462+00', '2026-06-30 10:35:46.242462+00') ON CONFLICT DO NOTHING;
INSERT INTO counterparty_role_ref VALUES ('LENDER', 'Entity that provides the loan/credit', '2026-06-30 10:35:46.242462+00', '2026-06-30 10:35:46.242462+00') ON CONFLICT DO NOTHING;
INSERT INTO counterparty_role_ref VALUES ('GUARANTOR', 'Entity providing a guarantee or credit support', '2026-06-30 10:35:46.242462+00', '2026-06-30 10:35:46.242462+00') ON CONFLICT DO NOTHING;
INSERT INTO counterparty_role_ref VALUES ('SERVICER', 'Entity that administers/services a loan or securitization', '2026-06-30 10:35:46.242462+00', '2026-06-30 10:35:46.242462+00') ON CONFLICT DO NOTHING;
INSERT INTO counterparty_role_ref VALUES ('OTHER', 'Known counterparty role not among the listed roles', '2026-06-30 10:35:46.242462+00', '2026-06-30 10:35:46.242462+00') ON CONFLICT DO NOTHING;
INSERT INTO counterparty_role_ref VALUES ('SPONSOR', 'Entity that sponsors / backs / arranges the vehicle or deal', '2026-06-30 10:57:38.555343+00', '2026-06-30 10:57:38.555343+00') ON CONFLICT DO NOTHING;
INSERT INTO counterparty_role_ref VALUES ('ORIGINATOR', 'Entity that originated the underlying assets', '2026-06-30 10:57:38.555343+00', '2026-06-30 10:57:38.555343+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('AE', 'ARE', '784', 'United Arab Emirates', 'ASIA', 'WESTERN_ASIA', 'AED', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('AR', 'ARG', '032', 'Argentina', 'SOUTH_AMERICA', 'SOUTH_AMERICA', 'ARS', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('AT', 'AUT', '040', 'Austria', 'EUROPE', 'WESTERN_EUROPE', 'EUR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('AU', 'AUS', '036', 'Australia', 'OCEANIA', 'AUSTRALASIA', 'AUD', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:54:07.965845+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('BE', 'BEL', '056', 'Belgium', 'EUROPE', 'WESTERN_EUROPE', 'EUR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('BM', 'BMU', '060', 'Bermuda', 'NORTH_AMERICA', 'NORTHERN_AMERICA', 'BMD', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('BR', 'BRA', '076', 'Brazil', 'SOUTH_AMERICA', 'SOUTH_AMERICA', 'BRL', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('BS', 'BHS', '044', 'Bahamas', 'NORTH_AMERICA', 'CARIBBEAN', 'BSD', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('BY', 'BLR', '112', 'Belarus', 'EUROPE', 'EASTERN_EUROPE', 'BYN', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('CA', 'CAN', '124', 'Canada', 'NORTH_AMERICA', 'NORTHERN_AMERICA', 'CAD', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('CH', 'CHE', '756', 'Switzerland', 'EUROPE', 'WESTERN_EUROPE', 'CHF', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('CL', 'CHL', '152', 'Chile', 'SOUTH_AMERICA', 'SOUTH_AMERICA', 'CLP', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('CN', 'CHN', '156', 'China', 'ASIA', 'EASTERN_ASIA', 'CNY', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('CO', 'COL', '170', 'Colombia', 'SOUTH_AMERICA', 'SOUTH_AMERICA', 'COP', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('CU', 'CUB', '192', 'Cuba', 'NORTH_AMERICA', 'CARIBBEAN', 'CUP', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('CZ', 'CZE', '203', 'Czech Republic', 'EUROPE', 'EASTERN_EUROPE', 'CZK', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('DE', 'DEU', '276', 'Germany', 'EUROPE', 'WESTERN_EUROPE', 'EUR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('DK', 'DNK', '208', 'Denmark', 'EUROPE', 'NORTHERN_EUROPE', 'DKK', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('EE', 'EST', '233', 'Estonia', 'EUROPE', 'NORTHERN_EUROPE', 'EUR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('EG', 'EGY', '818', 'Egypt', 'AFRICA', 'NORTHERN_AFRICA', 'EGP', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('ES', 'ESP', '724', 'Spain', 'EUROPE', 'SOUTHERN_EUROPE', 'EUR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('FI', 'FIN', '246', 'Finland', 'EUROPE', 'NORTHERN_EUROPE', 'EUR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('FR', 'FRA', '250', 'France', 'EUROPE', 'WESTERN_EUROPE', 'EUR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('GB', 'GBR', '826', 'United Kingdom', 'EUROPE', 'NORTHERN_EUROPE', 'GBP', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('GG', 'GGY', '831', 'Guernsey', 'EUROPE', 'NORTHERN_EUROPE', 'GBP', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('GI', 'GIB', '292', 'Gibraltar', 'EUROPE', 'SOUTHERN_EUROPE', 'GBP', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('HK', 'HKG', '344', 'Hong Kong', 'ASIA', 'EASTERN_ASIA', 'HKD', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('HT', 'HTI', '332', 'Haiti', 'NORTH_AMERICA', 'CARIBBEAN', 'HTG', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('ID', 'IDN', '360', 'Indonesia', 'ASIA', 'SOUTH_EASTERN_ASIA', 'IDR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('IE', 'IRL', '372', 'Ireland', 'EUROPE', 'NORTHERN_EUROPE', 'EUR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('IL', 'ISR', '376', 'Israel', 'ASIA', 'WESTERN_ASIA', 'ILS', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('IN', 'IND', '356', 'India', 'ASIA', 'SOUTHERN_ASIA', 'INR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('IR', 'IRN', '364', 'Iran', 'ASIA', 'SOUTHERN_ASIA', 'IRR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('IT', 'ITA', '380', 'Italy', 'EUROPE', 'SOUTHERN_EUROPE', 'EUR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('JE', 'JEY', '832', 'Jersey', 'EUROPE', 'NORTHERN_EUROPE', 'GBP', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('JP', 'JPN', '392', 'Japan', 'ASIA', 'EASTERN_ASIA', 'JPY', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('KE', 'KEN', '404', 'Kenya', 'AFRICA', 'EASTERN_AFRICA', 'KES', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('KP', 'PRK', '408', 'North Korea', 'ASIA', 'EASTERN_ASIA', 'KPW', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('KR', 'KOR', '410', 'South Korea', 'ASIA', 'EASTERN_ASIA', 'KRW', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('KY', 'CYM', '136', 'Cayman Islands', 'NORTH_AMERICA', 'CARIBBEAN', 'KYD', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('LI', 'LIE', '438', 'Liechtenstein', 'EUROPE', 'WESTERN_EUROPE', 'CHF', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('LT', 'LTU', '440', 'Lithuania', 'EUROPE', 'NORTHERN_EUROPE', 'EUR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('LU', 'LUX', '442', 'Luxembourg', 'EUROPE', 'WESTERN_EUROPE', 'EUR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('LY', 'LBY', '434', 'Libya', 'AFRICA', 'NORTHERN_AFRICA', 'LYD', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('MM', 'MMR', '104', 'Myanmar', 'ASIA', 'SOUTH_EASTERN_ASIA', 'MMK', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('MT', 'MLT', '470', 'Malta', 'EUROPE', 'SOUTHERN_EUROPE', 'EUR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('MU', 'MUS', '480', 'Mauritius', 'AFRICA', 'EASTERN_AFRICA', 'MUR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('MX', 'MEX', '484', 'Mexico', 'NORTH_AMERICA', 'CENTRAL_AMERICA', 'MXN', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('MY', 'MYS', '458', 'Malaysia', 'ASIA', 'SOUTH_EASTERN_ASIA', 'MYR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('NG', 'NGA', '566', 'Nigeria', 'AFRICA', 'WESTERN_AFRICA', 'NGN', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('NL', 'NLD', '528', 'Netherlands', 'EUROPE', 'WESTERN_EUROPE', 'EUR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('NO', 'NOR', '578', 'Norway', 'EUROPE', 'NORTHERN_EUROPE', 'NOK', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('NZ', 'NZL', '554', 'New Zealand', 'OCEANIA', 'AUSTRALASIA', 'NZD', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:54:07.965845+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('PA', 'PAN', '591', 'Panama', 'NORTH_AMERICA', 'CENTRAL_AMERICA', 'USD', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('PH', 'PHL', '608', 'Philippines', 'ASIA', 'SOUTH_EASTERN_ASIA', 'PHP', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('PL', 'POL', '616', 'Poland', 'EUROPE', 'EASTERN_EUROPE', 'PLN', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('PT', 'PRT', '620', 'Portugal', 'EUROPE', 'SOUTHERN_EUROPE', 'EUR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('RU', 'RUS', '643', 'Russia', 'EUROPE', 'EASTERN_EUROPE', 'RUB', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('SA', 'SAU', '682', 'Saudi Arabia', 'ASIA', 'WESTERN_ASIA', 'SAR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('SC', 'SYC', '690', 'Seychelles', 'AFRICA', 'EASTERN_AFRICA', 'SCR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('SD', 'SDN', '729', 'Sudan', 'AFRICA', 'NORTHERN_AFRICA', 'SDG', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('SE', 'SWE', '752', 'Sweden', 'EUROPE', 'NORTHERN_EUROPE', 'SEK', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('SG', 'SGP', '702', 'Singapore', 'ASIA', 'SOUTH_EASTERN_ASIA', 'SGD', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('SO', 'SOM', '706', 'Somalia', 'AFRICA', 'EASTERN_AFRICA', 'SOS', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('SV', 'SLV', '222', 'El Salvador', 'NORTH_AMERICA', 'CENTRAL_AMERICA', 'USD', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('SY', 'SYR', '760', 'Syria', 'ASIA', 'WESTERN_ASIA', 'SYP', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('TH', 'THA', '764', 'Thailand', 'ASIA', 'SOUTH_EASTERN_ASIA', 'THB', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('TR', 'TUR', '792', 'Turkey', 'ASIA', 'WESTERN_ASIA', 'TRY', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('TW', 'TWN', '158', 'Taiwan', 'ASIA', 'EASTERN_ASIA', 'TWD', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('UA', 'UKR', '804', 'Ukraine', 'EUROPE', 'EASTERN_EUROPE', 'UAH', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('US', 'USA', '840', 'United States', 'NORTH_AMERICA', 'NORTHERN_AMERICA', 'USD', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('VE', 'VEN', '862', 'Venezuela', 'SOUTH_AMERICA', 'SOUTH_AMERICA', 'VES', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('VG', 'VGB', '092', 'British Virgin Islands', 'NORTH_AMERICA', 'CARIBBEAN', 'USD', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('XX', 'XXX', '000', 'Unknown', 'UNKNOWN', 'UNKNOWN', 'XXX', '2026-06-26 11:53:54.897517+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('ZA', 'ZAF', '710', 'South Africa', 'AFRICA', 'SOUTHERN_AFRICA', 'ZAR', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO country_ref VALUES ('ZW', 'ZWE', '716', 'Zimbabwe', 'AFRICA', 'EASTERN_AFRICA', 'ZWL', '2026-06-22 14:47:52.769993+00', '2026-06-30 07:44:59.022269+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (1, 'AAA', 'Aaa', 'PRIME', 'INVESTMENT_GRADE', 'Highest quality, minimal credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (2, 'AA+', 'Aa1', 'HIGH_GRADE', 'INVESTMENT_GRADE', 'Very high quality, very low credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (3, 'AA', 'Aa2', 'HIGH_GRADE', 'INVESTMENT_GRADE', 'Very high quality, very low credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (4, 'AA-', 'Aa3', 'HIGH_GRADE', 'INVESTMENT_GRADE', 'Very high quality, very low credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (5, 'A+', 'A1', 'UPPER_MEDIUM_GRADE', 'INVESTMENT_GRADE', 'High quality, low credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (6, 'A', 'A2', 'UPPER_MEDIUM_GRADE', 'INVESTMENT_GRADE', 'High quality, low credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (7, 'A-', 'A3', 'UPPER_MEDIUM_GRADE', 'INVESTMENT_GRADE', 'High quality, low credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (8, 'BBB+', 'Baa1', 'LOWER_MEDIUM_GRADE', 'INVESTMENT_GRADE', 'Good quality, moderate credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (9, 'BBB', 'Baa2', 'LOWER_MEDIUM_GRADE', 'INVESTMENT_GRADE', 'Good quality, moderate credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (10, 'BBB-', 'Baa3', 'LOWER_MEDIUM_GRADE', 'INVESTMENT_GRADE', 'Lowest investment grade, moderate credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (11, 'BB+', 'Ba1', 'NON_INVESTMENT_GRADE', 'SPECULATIVE', 'Speculative, substantial credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (12, 'BB', 'Ba2', 'NON_INVESTMENT_GRADE', 'SPECULATIVE', 'Speculative, substantial credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (13, 'BB-', 'Ba3', 'NON_INVESTMENT_GRADE', 'SPECULATIVE', 'Speculative, substantial credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (14, 'B+', 'B1', 'HIGHLY_SPECULATIVE', 'SPECULATIVE', 'Highly speculative, high credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (15, 'B', 'B2', 'HIGHLY_SPECULATIVE', 'SPECULATIVE', 'Highly speculative, high credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (16, 'B-', 'B3', 'HIGHLY_SPECULATIVE', 'SPECULATIVE', 'Highly speculative, high credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (17, 'CCC+', 'Caa1', 'SUBSTANTIAL_RISK', 'SPECULATIVE', 'Substantial credit risk, vulnerable', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (18, 'CCC', 'Caa2', 'SUBSTANTIAL_RISK', 'SPECULATIVE', 'Substantial credit risk, vulnerable', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (19, 'CCC-', 'Caa3', 'SUBSTANTIAL_RISK', 'SPECULATIVE', 'Substantial credit risk, vulnerable', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (20, 'CC', 'Ca', 'EXTREMELY_SPECULATIVE', 'SPECULATIVE', 'Very high credit risk', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (21, 'C', 'C', 'DEFAULT_IMMINENT', 'SPECULATIVE', 'Default imminent, little prospect of recovery', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (22, 'D', NULL, 'IN_DEFAULT', 'DEFAULT', 'In default', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO credit_rating_ref VALUES (99, 'NR', 'NR', 'NOT_RATED', 'NOT_RATED', 'Not rated', '2026-06-30 10:09:13.075084+00', '2026-06-30 10:09:13.075084+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('AED', '784', 'UAE Dirham', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('ARS', '032', 'Argentine Peso', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('AUD', '036', 'Australian Dollar', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('BMD', '060', 'Bermudian Dollar', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('BRL', '986', 'Brazilian Real', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('BSD', '044', 'Bahamian Dollar', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('BYN', '933', 'Belarusian Ruble', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('CAD', '124', 'Canadian Dollar', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('CHF', '756', 'Swiss Franc', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('CLP', '152', 'Chilean Peso', 0, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('CNY', '156', 'Yuan Renminbi', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('COP', '170', 'Colombian Peso', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('CUP', '192', 'Cuban Peso', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('CZK', '203', 'Czech Koruna', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('DKK', '208', 'Danish Krone', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('EGP', '818', 'Egyptian Pound', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('EUR', '978', 'Euro', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('GBP', '826', 'Pound Sterling', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('HKD', '344', 'Hong Kong Dollar', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('HTG', '332', 'Gourde', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('IDR', '360', 'Rupiah', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('ILS', '376', 'New Israeli Sheqel', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('INR', '356', 'Indian Rupee', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('IRR', '364', 'Iranian Rial', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('JPY', '392', 'Yen', 0, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('KES', '404', 'Kenyan Shilling', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('KPW', '408', 'North Korean Won', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('KRW', '410', 'Won', 0, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('KYD', '136', 'Cayman Islands Dollar', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('LYD', '434', 'Libyan Dinar', 3, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('MMK', '104', 'Kyat', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('MUR', '480', 'Mauritius Rupee', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('MXN', '484', 'Mexican Peso', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('MYR', '458', 'Malaysian Ringgit', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('NGN', '566', 'Naira', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('NOK', '578', 'Norwegian Krone', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('NZD', '554', 'New Zealand Dollar', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('PHP', '608', 'Philippine Peso', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('PLN', '985', 'Zloty', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('RUB', '643', 'Russian Ruble', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('SAR', '682', 'Saudi Riyal', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('SCR', '690', 'Seychelles Rupee', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('SDG', '938', 'Sudanese Pound', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('SEK', '752', 'Swedish Krona', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('SGD', '702', 'Singapore Dollar', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('SOS', '706', 'Somali Shilling', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('SYP', '760', 'Syrian Pound', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('THB', '764', 'Baht', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('TRY', '949', 'Turkish Lira', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('TWD', '901', 'New Taiwan Dollar', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('UAH', '980', 'Hryvnia', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('USD', '840', 'US Dollar', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('VES', '928', 'Bolivar Soberano', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('ZAR', '710', 'Rand', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('ZWL', '932', 'Zimbabwe Dollar', 2, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO currency_ref VALUES ('XXX', '999', 'Unknown / No currency', 0, '2026-06-30 08:05:49.040608+00', '2026-06-30 08:05:49.040608+00') ON CONFLICT DO NOTHING;
INSERT INTO deal_type_ref VALUES ('LOAN', 'LONG', 'Asset lent to a protocol/counterparty; earns yield', '2026-06-22 15:12:10.464516+00', '2026-06-22 15:12:10.464516+00') ON CONFLICT DO NOTHING;
INSERT INTO deal_type_ref VALUES ('BORROW', 'SHORT', 'Asset borrowed against collateral; incurs interest cost', '2026-06-22 15:12:10.464516+00', '2026-06-22 15:12:10.464516+00') ON CONFLICT DO NOTHING;
INSERT INTO deal_type_ref VALUES ('COLLATERAL', 'LONG', 'Asset pledged to secure a borrow; not earning yield', '2026-06-22 15:12:10.464516+00', '2026-06-22 15:12:10.464516+00') ON CONFLICT DO NOTHING;
INSERT INTO deal_type_ref VALUES ('CUSTODY', 'LONG', 'Asset held at custodian, unencumbered', '2026-06-22 15:12:10.464516+00', '2026-06-22 15:12:10.464516+00') ON CONFLICT DO NOTHING;
INSERT INTO deal_type_ref VALUES ('CUSTODY_COLLATERAL', 'LONG', 'Asset held at custodian and pledged as collateral', '2026-06-22 15:12:10.464516+00', '2026-06-22 15:12:10.464516+00') ON CONFLICT DO NOTHING;
INSERT INTO deal_type_ref VALUES ('ALLOCATION', 'LONG', 'TradFi instrument held via proxy/allocation vehicle', '2026-06-22 15:12:10.464516+00', '2026-06-22 15:12:10.464516+00') ON CONFLICT DO NOTHING;
INSERT INTO deal_type_ref VALUES ('UNKNOWN', NULL, 'Unclassified; do not hash into position_id until resolved', '2026-06-26 11:53:54.897517+00', '2026-06-26 11:53:54.897517+00') ON CONFLICT DO NOTHING;
INSERT INTO deal_type_ref VALUES ('LP_POSITION', 'LONG', 'Liquidity provided to an AMM pool; earns trading fees, bears impermanent loss', '2026-06-22 15:12:10.464516+00', '2026-06-30 08:43:53.055677+00') ON CONFLICT DO NOTHING;
INSERT INTO deal_type_ref VALUES ('STAKED', 'LONG', 'Native asset locked directly in a validator staking contract; earns staking rewards, bears slashing risk. Liquid-staking tokens (wstETH etc.) are securities, not this deal type.', '2026-06-22 15:12:10.464516+00', '2026-06-30 08:50:44.209621+00') ON CONFLICT DO NOTHING;
INSERT INTO deal_type_ref VALUES ('OTHER', NULL, 'Known deal type not among the listed types', '2026-06-30 10:51:00.263281+00', '2026-06-30 10:51:00.263281+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('CORPORATION', 'Registered company with share capital', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('LLC', 'Limited liability company', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('TRUST', 'Legal trust managed by a trustee', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('FUND', 'Investment fund or collective vehicle', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('SPV', 'Special purpose vehicle for a defined transaction', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('GOVERNMENT', 'Government department, ministry, or agency', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('SOVEREIGN', 'Sovereign state entity; central bank / SWF', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('FOUNDATION', 'Non-profit foundation under a charter', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('BANK', 'Licensed deposit-taking institution', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('ASSOCIATION', 'Membership-based association or cooperative', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('HOLDING_COMPANY', 'Group holding entity; no direct trading', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('PARTNERSHIP', 'Limited or general partnership', '2026-06-22 14:47:52.769993+00', '2026-06-22 14:47:52.769993+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('UNKNOWN', 'Unclassified or pending classification', '2026-06-26 11:53:54.897517+00', '2026-06-26 11:53:54.897517+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('DAO', 'Decentralized autonomous organization governed on-chain by token holders', '2026-06-30 08:54:58.740889+00', '2026-06-30 08:54:58.740889+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('SUPRANATIONAL', 'Supranational / multilateral institution (e.g. World Bank, EIB, IMF)', '2026-06-30 10:21:41.655168+00', '2026-06-30 10:21:41.655168+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('INDIVIDUAL', 'Natural person (e.g. high-net-worth individual borrower or investor)', '2026-06-30 10:24:28.791558+00', '2026-06-30 10:24:28.791558+00') ON CONFLICT DO NOTHING;
INSERT INTO entity_type_ref VALUES ('OTHER', 'Known entity whose legal/organizational form is not one of the listed types', '2026-06-30 10:25:35.434629+00', '2026-06-30 10:25:35.434629+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('ENERGY', 'ENERGY', '1010', 'Energy', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('MATERIALS', 'MATERIALS', '1510', 'Materials', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('INDUSTRIALS', 'CAPITAL_GOODS', '2010', 'Capital Goods', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('INDUSTRIALS', 'COMMERCIAL_PROF_SERVICES', '2020', 'Commercial & Professional Services', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('INDUSTRIALS', 'TRANSPORTATION', '2030', 'Transportation', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('CONSUMER_DISCRETIONARY', 'AUTOMOBILES_COMPONENTS', '2510', 'Automobiles & Components', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('CONSUMER_DISCRETIONARY', 'CONSUMER_DURABLES_APPAREL', '2520', 'Consumer Durables & Apparel', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('CONSUMER_DISCRETIONARY', 'CONSUMER_SERVICES', '2530', 'Consumer Services', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('CONSUMER_DISCRETIONARY', 'CONSUMER_DISCR_DISTRIB_RETAIL', '2550', 'Consumer Discretionary Distribution & Retail', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('CONSUMER_STAPLES', 'CONSUMER_STAPLES_DISTRIB_RETAIL', '3010', 'Consumer Staples Distribution & Retail', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('CONSUMER_STAPLES', 'FOOD_BEVERAGE_TOBACCO', '3020', 'Food, Beverage & Tobacco', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('CONSUMER_STAPLES', 'HOUSEHOLD_PERSONAL_PRODUCTS', '3030', 'Household & Personal Products', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('HEALTH_CARE', 'HEALTHCARE_EQUIP_SERVICES', '3510', 'Health Care Equipment & Services', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('HEALTH_CARE', 'PHARMA_BIOTECH_LIFE', '3520', 'Pharmaceuticals, Biotechnology & Life Sciences', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('FINANCIALS', 'BANKS', '4010', 'Banks', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('FINANCIALS', 'FINANCIAL_SERVICES', '4020', 'Financial Services', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('FINANCIALS', 'INSURANCE', '4030', 'Insurance', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('INFORMATION_TECHNOLOGY', 'SOFTWARE_SERVICES', '4510', 'Software & Services', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('INFORMATION_TECHNOLOGY', 'TECH_HARDWARE_EQUIP', '4520', 'Technology Hardware & Equipment', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('INFORMATION_TECHNOLOGY', 'SEMICONDUCTORS', '4530', 'Semiconductors & Semiconductor Equipment', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('COMMUNICATION_SERVICES', 'TELECOM_SERVICES', '5010', 'Telecommunication Services', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('COMMUNICATION_SERVICES', 'MEDIA_ENTERTAINMENT', '5020', 'Media & Entertainment', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('UTILITIES', 'UTILITIES', '5510', 'Utilities', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('REAL_ESTATE', 'EQUITY_REITS', '6010', 'Equity Real Estate Investment Trusts (REITs)', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('REAL_ESTATE', 'REAL_ESTATE_MGMT_DEV', '6020', 'Real Estate Management & Development', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('OTHER', 'OTHER', NULL, 'Known industry group not among the listed groups', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO industry_group_ref VALUES ('UNKNOWN', 'UNKNOWN', NULL, 'Unclassified or pending classification', '2026-06-30 12:18:44.992869+00', '2026-06-30 12:18:44.992869+00') ON CONFLICT DO NOTHING;
INSERT INTO origination_type_ref VALUES ('PROTOCOL', 'Credit extended and terms set by on-chain governance / immutable contract; no human discretion', '2026-06-26 11:53:54.897517+00', '2026-06-26 11:53:54.897517+00') ON CONFLICT DO NOTHING;
INSERT INTO origination_type_ref VALUES ('INSTITUTIONAL', 'Regulated institution underwrites and structures; human credit approval', '2026-06-26 11:53:54.897517+00', '2026-06-26 11:53:54.897517+00') ON CONFLICT DO NOTHING;
INSERT INTO origination_type_ref VALUES ('BILATERAL', 'Two counterparties negotiate all terms directly', '2026-06-26 11:53:54.897517+00', '2026-06-26 11:53:54.897517+00') ON CONFLICT DO NOTHING;
INSERT INTO origination_type_ref VALUES ('RETAIL', 'Retail lending facility; not currently in use', '2026-06-26 11:53:54.897517+00', '2026-06-26 11:53:54.897517+00') ON CONFLICT DO NOTHING;
INSERT INTO origination_type_ref VALUES ('UNKNOWN', 'Unclassified or pending classification', '2026-06-26 11:53:54.897517+00', '2026-06-26 11:53:54.897517+00') ON CONFLICT DO NOTHING;
INSERT INTO origination_type_ref VALUES ('OTHER', 'Known origination channel not among the listed types', '2026-06-30 10:40:34.430287+00', '2026-06-30 10:40:34.430287+00') ON CONFLICT DO NOTHING;
INSERT INTO sector_ref VALUES ('ENERGY', '10', 'Energy', '2026-06-30 12:06:38.236126+00', '2026-06-30 12:06:38.236126+00') ON CONFLICT DO NOTHING;
INSERT INTO sector_ref VALUES ('MATERIALS', '15', 'Materials', '2026-06-30 12:06:38.236126+00', '2026-06-30 12:06:38.236126+00') ON CONFLICT DO NOTHING;
INSERT INTO sector_ref VALUES ('INDUSTRIALS', '20', 'Industrials', '2026-06-30 12:06:38.236126+00', '2026-06-30 12:06:38.236126+00') ON CONFLICT DO NOTHING;
INSERT INTO sector_ref VALUES ('CONSUMER_DISCRETIONARY', '25', 'Consumer discretionary', '2026-06-30 12:06:38.236126+00', '2026-06-30 12:06:38.236126+00') ON CONFLICT DO NOTHING;
INSERT INTO sector_ref VALUES ('CONSUMER_STAPLES', '30', 'Consumer staples', '2026-06-30 12:06:38.236126+00', '2026-06-30 12:06:38.236126+00') ON CONFLICT DO NOTHING;
INSERT INTO sector_ref VALUES ('HEALTH_CARE', '35', 'Health care', '2026-06-30 12:06:38.236126+00', '2026-06-30 12:06:38.236126+00') ON CONFLICT DO NOTHING;
INSERT INTO sector_ref VALUES ('FINANCIALS', '40', 'Financials', '2026-06-30 12:06:38.236126+00', '2026-06-30 12:06:38.236126+00') ON CONFLICT DO NOTHING;
INSERT INTO sector_ref VALUES ('INFORMATION_TECHNOLOGY', '45', 'Information technology', '2026-06-30 12:06:38.236126+00', '2026-06-30 12:06:38.236126+00') ON CONFLICT DO NOTHING;
INSERT INTO sector_ref VALUES ('COMMUNICATION_SERVICES', '50', 'Communication services', '2026-06-30 12:06:38.236126+00', '2026-06-30 12:06:38.236126+00') ON CONFLICT DO NOTHING;
INSERT INTO sector_ref VALUES ('UTILITIES', '55', 'Utilities', '2026-06-30 12:06:38.236126+00', '2026-06-30 12:06:38.236126+00') ON CONFLICT DO NOTHING;
INSERT INTO sector_ref VALUES ('REAL_ESTATE', '60', 'Real estate', '2026-06-30 12:06:38.236126+00', '2026-06-30 12:06:38.236126+00') ON CONFLICT DO NOTHING;
INSERT INTO sector_ref VALUES ('GOVERNMENT', NULL, 'Government / sovereign / supranational issuer (outside GICS - house extension)', '2026-06-30 12:06:38.236126+00', '2026-06-30 12:06:38.236126+00') ON CONFLICT DO NOTHING;
INSERT INTO sector_ref VALUES ('DIGITAL_ASSETS', NULL, 'Crypto protocol / token / stablecoin issuer (outside GICS - house extension)', '2026-06-30 12:06:38.236126+00', '2026-06-30 12:06:38.236126+00') ON CONFLICT DO NOTHING;
INSERT INTO sector_ref VALUES ('OTHER', NULL, 'Known issuer sector not among the listed sectors', '2026-06-30 12:06:38.236126+00', '2026-06-30 12:06:38.236126+00') ON CONFLICT DO NOTHING;
INSERT INTO sector_ref VALUES ('UNKNOWN', NULL, 'Unclassified or pending classification', '2026-06-30 12:06:38.236126+00', '2026-06-30 12:06:38.236126+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('MONEY_MARKET', 'COMMERCIAL_PAPER', 'ABCP', 'Asset-backed commercial paper', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('MONEY_MARKET', 'COMMERCIAL_PAPER', 'UNSECURED_CP', 'Unsecured commercial paper', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('MONEY_MARKET', 'REPO', 'TERM_REPO', 'Repurchase agreement with a fixed term', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('MONEY_MARKET', 'REPO', 'OPEN_REPO', 'Repurchase agreement with no fixed maturity', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('MONEY_MARKET', 'REPO', 'TRI_PARTY_REPO', 'Repurchase agreement settled via a tri-party agent', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('FIXED_INCOME', 'GOVERNMENT_BOND', 'INFLATION_LINKED', 'Government bond indexed to inflation', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('FIXED_INCOME', 'GOVERNMENT_BOND', 'ZERO_COUPON', 'Discount bond paying no coupon', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('FIXED_INCOME', 'GOVERNMENT_BOND', 'FLOATING_RATE_NOTE', 'Bond with a floating coupon', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('FIXED_INCOME', 'CORPORATE_BOND', 'CONVERTIBLE', 'Corporate bond convertible into equity', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('STRUCTURED_CREDIT', 'CMBS', 'CONDUIT_CMBS', 'Multi-borrower conduit CMBS', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('STRUCTURED_CREDIT', 'CMBS', 'SINGLE_ASSET_CMBS', 'CMBS backed by a single asset or borrower', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('STRUCTURED_CREDIT', 'CMO', 'PAC', 'Planned amortization class CMO tranche', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('STRUCTURED_CREDIT', 'CMO', 'TAC', 'Targeted amortization class CMO tranche', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('STRUCTURED_CREDIT', 'CMO', 'Z_BOND', 'Accrual (Z) CMO tranche', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('EQUITY', 'DEPOSITARY_RECEIPT', 'ADR', 'American depositary receipt', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('EQUITY', 'DEPOSITARY_RECEIPT', 'GDR', 'Global depositary receipt', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('COMMODITY', 'PRECIOUS_METAL', 'GOLD', 'Gold', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('COMMODITY', 'PRECIOUS_METAL', 'SILVER', 'Silver', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('COMMODITY', 'PRECIOUS_METAL', 'PLATINUM', 'Platinum', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('COMMODITY', 'ENERGY', 'CRUDE_OIL', 'Crude oil', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('COMMODITY', 'ENERGY', 'NATURAL_GAS', 'Natural gas', '2026-06-22 15:42:47.980596+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'Unclassified or pending classification', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('STRUCTURED_CREDIT', 'ABS', 'AUTO', 'Auto-loan-backed ABS', '2026-06-30 10:11:24.215999+00', '2026-06-30 10:11:24.215999+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('STRUCTURED_CREDIT', 'ABS', 'CONSUMER', 'Consumer-loan-backed ABS', '2026-06-30 10:11:24.215999+00', '2026-06-30 10:11:24.215999+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('STRUCTURED_CREDIT', 'ABS', 'CREDIT_CARD', 'Credit-card-receivable ABS', '2026-06-30 10:11:24.215999+00', '2026-06-30 10:11:24.215999+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('STRUCTURED_CREDIT', 'ABS', 'STUDENT_LOAN', 'Student-loan-backed ABS', '2026-06-30 10:11:24.215999+00', '2026-06-30 10:11:24.215999+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('STRUCTURED_CREDIT', 'RMBS', 'AGENCY_RMBS', 'Agency-guaranteed RMBS', '2026-06-30 10:11:24.215999+00', '2026-06-30 10:11:24.215999+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('STRUCTURED_CREDIT', 'RMBS', 'NON_AGENCY_RMBS', 'Non-agency RMBS', '2026-06-30 10:11:24.215999+00', '2026-06-30 10:11:24.215999+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('STRUCTURED_CREDIT', 'CDO', 'CASH_CDO', 'Cash CDO (funded with actual assets)', '2026-06-30 10:11:24.215999+00', '2026-06-30 10:11:24.215999+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('STRUCTURED_CREDIT', 'CDO', 'SYNTHETIC_CDO', 'Synthetic CDO (credit exposure via derivatives)', '2026-06-30 10:11:24.215999+00', '2026-06-30 10:11:24.215999+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('DIGITAL_ASSET', 'STABLECOIN', 'FIAT_BACKED', 'Fiat-reserve-backed stablecoin', '2026-06-30 10:11:24.215999+00', '2026-06-30 10:11:24.215999+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('DIGITAL_ASSET', 'STABLECOIN', 'CRYPTO_BACKED', 'Crypto-collateral-backed stablecoin', '2026-06-30 10:11:24.215999+00', '2026-06-30 10:11:24.215999+00') ON CONFLICT DO NOTHING;
INSERT INTO security_subtype_ref VALUES ('DIGITAL_ASSET', 'STABLECOIN', 'ALGORITHMIC', 'Algorithmic / undercollateralised stablecoin', '2026-06-30 10:11:24.215999+00', '2026-06-30 10:11:24.215999+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DIGITAL_ASSET', 'STABLECOIN', 'Price-stable token pegged to a reference asset', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DIGITAL_ASSET', 'LAYER1_TOKEN', 'Native token of a layer-1 blockchain', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DIGITAL_ASSET', 'LIQUID_STAKED_TOKEN', 'Liquid staking receipt token', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DIGITAL_ASSET', 'NATIVE_STAKED_TOKEN', 'Natively staked layer-1 asset', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DIGITAL_ASSET', 'WRAPPED_TOKEN', 'Wrapped representation of another asset', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DIGITAL_ASSET', 'LP_TOKEN', 'AMM liquidity-pool position token', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DIGITAL_ASSET', 'YIELD_BEARING_TOKEN', 'Token accruing protocol yield', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DIGITAL_ASSET', 'GOVERNANCE_TOKEN', 'Protocol governance token', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DIGITAL_ASSET', 'PERPETUAL_TOKEN', 'Perpetual or derivative position token', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DIGITAL_ASSET', 'CRYPTO_ETF', 'Exchange-traded crypto fund', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DIGITAL_ASSET', 'CRYPTO_INDEX_TOKEN', 'Tokenised crypto index', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('MONEY_MARKET', 'T_BILL', 'Short-term government treasury bill', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('MONEY_MARKET', 'COMMERCIAL_PAPER', 'Short-term corporate debt', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('MONEY_MARKET', 'REPO', 'Repurchase agreement', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('MONEY_MARKET', 'TOKENISED_FUND', 'Tokenised money-market fund', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('FIXED_INCOME', 'GOVERNMENT_BOND', 'Sovereign government bond', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('FIXED_INCOME', 'AGENCY_BOND', 'Government-agency bond', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('FIXED_INCOME', 'SUPRANATIONAL_BOND', 'Supranational issuer bond', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('FIXED_INCOME', 'MUNICIPAL_BOND', 'Municipal or local-government bond', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('FIXED_INCOME', 'COVERED_BOND', 'Bond secured by a ring-fenced cover pool', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('FIXED_INCOME', 'CORPORATE_BOND', 'Corporate debt security', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('FIXED_INCOME', 'FIXED_INCOME_ETF', 'Exchange-traded fixed-income fund', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('PRIVATE_CREDIT', 'DIRECT_LENDING', 'Directly originated private loan', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('PRIVATE_CREDIT', 'MEZZANINE_DEBT', 'Subordinated mezzanine debt', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('PRIVATE_CREDIT', 'VENTURE_DEBT', 'Debt to venture-backed companies', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('PRIVATE_CREDIT', 'REAL_ESTATE_DEBT', 'Private real-estate debt', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('PRIVATE_CREDIT', 'INFRASTRUCTURE_DEBT', 'Private infrastructure debt', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('PRIVATE_CREDIT', 'REVOLVING_CREDIT', 'Revolving credit facility', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('STRUCTURED_CREDIT', 'ABS', 'Asset-backed security', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('STRUCTURED_CREDIT', 'RMBS', 'Residential mortgage-backed security', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('STRUCTURED_CREDIT', 'CMBS', 'Commercial mortgage-backed security', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('STRUCTURED_CREDIT', 'CLO', 'Collateralised loan obligation', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('STRUCTURED_CREDIT', 'CDO', 'Collateralised debt obligation', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('STRUCTURED_CREDIT', 'CMO', 'Collateralised mortgage obligation', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('EQUITY', 'COMMON_STOCK', 'Common equity shares', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('EQUITY', 'PREFERRED_STOCK', 'Preferred equity shares', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('EQUITY', 'DEPOSITARY_RECEIPT', 'Depositary receipt for foreign equity', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('EQUITY', 'EQUITY_ETF', 'Exchange-traded equity fund', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('EQUITY', 'EQUITY_INDEX_FUND', 'Equity index fund', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('REAL_ESTATE', 'INCOME_PRODUCING', 'Income-producing property', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('REAL_ESTATE', 'DEVELOPMENT', 'Property development project', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('REAL_ESTATE', 'REIT', 'Listed real-estate investment trust', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('REAL_ESTATE', 'PRIVATE_REIT', 'Private real-estate investment trust', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('COMMODITY', 'PRECIOUS_METAL', 'Precious-metal exposure', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('COMMODITY', 'BASE_METAL', 'Base-metal exposure', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('COMMODITY', 'ENERGY', 'Energy commodity exposure', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('COMMODITY', 'CARBON_CREDIT', 'Carbon credit', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('COMMODITY', 'AGRICULTURAL', 'Agricultural commodity', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('COMMODITY', 'COMMODITY_ETF', 'Exchange-traded commodity fund', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('INDEX', 'MULTI_ASSET_ETF', 'Multi-asset exchange-traded fund', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('INDEX', 'BALANCED_FUND', 'Balanced multi-asset fund', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('INDEX', 'INDEX_LINKED_NOTE', 'Note linked to an index', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('UNKNOWN', 'UNKNOWN', 'Unclassified or pending classification', '2026-06-26 11:53:54.897517+00', '2026-06-26 12:49:53.014606+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DERIVATIVE', 'FUTURE', 'Exchange-traded futures contract', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DERIVATIVE', 'FORWARD', 'Bilateral forward contract', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DERIVATIVE', 'OPTION', 'Option contract (call or put)', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DERIVATIVE', 'SWAP', 'Swap contract (rate, FX, or total return)', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DERIVATIVE', 'PERPETUAL_SWAP', 'Perpetual swap / perp contract', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('PRIVATE_EQUITY', 'DIRECT_EQUITY', 'Direct equity stake in a private company', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('PRIVATE_EQUITY', 'FUND_INTEREST', 'Limited-partner interest in a PE/VC fund', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('PRIVATE_EQUITY', 'CO_INVESTMENT', 'Co-investment alongside a fund', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('CASH', 'DEMAND_DEPOSIT', 'Instant-access bank deposit', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('CASH', 'TERM_DEPOSIT', 'Fixed-term deposit / certificate of deposit', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('CASH', 'CASH_BALANCE', 'Uninvested cash balance at a custodian or broker', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('EQUITY', 'OTHER', 'Residual: belongs to this asset class but matches no specific listed type', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('FIXED_INCOME', 'OTHER', 'Residual: belongs to this asset class but matches no specific listed type', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('INDEX', 'OTHER', 'Residual: belongs to this asset class but matches no specific listed type', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('COMMODITY', 'OTHER', 'Residual: belongs to this asset class but matches no specific listed type', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('PRIVATE_CREDIT', 'OTHER', 'Residual: belongs to this asset class but matches no specific listed type', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('PRIVATE_EQUITY', 'OTHER', 'Residual: belongs to this asset class but matches no specific listed type', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('REAL_ESTATE', 'OTHER', 'Residual: belongs to this asset class but matches no specific listed type', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('STRUCTURED_CREDIT', 'OTHER', 'Residual: belongs to this asset class but matches no specific listed type', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DIGITAL_ASSET', 'OTHER', 'Residual: belongs to this asset class but matches no specific listed type', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('DERIVATIVE', 'OTHER', 'Residual: belongs to this asset class but matches no specific listed type', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('CASH', 'OTHER', 'Residual: belongs to this asset class but matches no specific listed type', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
INSERT INTO security_type_ref VALUES ('MONEY_MARKET', 'OTHER', 'Residual: belongs to this asset class but matches no specific listed type', '2026-06-30 09:44:32.121648+00', '2026-06-30 09:44:32.121648+00') ON CONFLICT DO NOTHING;
ALTER TABLE ONLY asset_class_ref
    ADD CONSTRAINT asset_class_ref_pkey PRIMARY KEY (asset_class);
ALTER TABLE ONLY corporate_action_type_ref
    ADD CONSTRAINT corporate_action_type_ref_pkey PRIMARY KEY (corporate_action_type);
ALTER TABLE ONLY counterparty_role_ref
    ADD CONSTRAINT counterparty_role_ref_pkey PRIMARY KEY (counterparty_role);
ALTER TABLE ONLY country_ref
    ADD CONSTRAINT country_ref_pkey PRIMARY KEY (country_code);
ALTER TABLE ONLY credit_rating_ref
    ADD CONSTRAINT credit_rating_ref_moodys_rating_key UNIQUE (moodys_rating);
ALTER TABLE ONLY credit_rating_ref
    ADD CONSTRAINT credit_rating_ref_pkey PRIMARY KEY (rank);
ALTER TABLE ONLY credit_rating_ref
    ADD CONSTRAINT credit_rating_ref_sp_rating_key UNIQUE (sp_rating);
ALTER TABLE ONLY currency_ref
    ADD CONSTRAINT currency_ref_pkey PRIMARY KEY (currency_code);
ALTER TABLE ONLY deal_type_ref
    ADD CONSTRAINT deal_type_ref_pkey PRIMARY KEY (deal_type);
ALTER TABLE ONLY entity_type_ref
    ADD CONSTRAINT entity_type_ref_pkey PRIMARY KEY (entity_type);
ALTER TABLE ONLY industry_group_ref
    ADD CONSTRAINT industry_group_ref_pkey PRIMARY KEY (industry_group);
ALTER TABLE ONLY origination_type_ref
    ADD CONSTRAINT origination_type_ref_pkey PRIMARY KEY (origination_type);
ALTER TABLE ONLY sector_ref
    ADD CONSTRAINT sector_ref_pkey PRIMARY KEY (sector);
ALTER TABLE ONLY security_subtype_ref
    ADD CONSTRAINT security_subtype_ref_pkey PRIMARY KEY (asset_class, security_type, security_subtype);
ALTER TABLE ONLY security_type_ref
    ADD CONSTRAINT security_type_ref_pkey PRIMARY KEY (asset_class, security_type);
CREATE TRIGGER trg_updated_at BEFORE UPDATE ON asset_class_ref FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at BEFORE UPDATE ON corporate_action_type_ref FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at BEFORE UPDATE ON counterparty_role_ref FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at BEFORE UPDATE ON country_ref FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at BEFORE UPDATE ON credit_rating_ref FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at BEFORE UPDATE ON currency_ref FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at BEFORE UPDATE ON deal_type_ref FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at BEFORE UPDATE ON entity_type_ref FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at BEFORE UPDATE ON industry_group_ref FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at BEFORE UPDATE ON origination_type_ref FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at BEFORE UPDATE ON sector_ref FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at BEFORE UPDATE ON security_subtype_ref FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_updated_at BEFORE UPDATE ON security_type_ref FOR EACH ROW EXECUTE FUNCTION set_updated_at();
ALTER TABLE ONLY country_ref
    ADD CONSTRAINT country_ref_currency_code_fkey FOREIGN KEY (currency_code) REFERENCES currency_ref(currency_code);
ALTER TABLE ONLY industry_group_ref
    ADD CONSTRAINT industry_group_ref_sector_fkey FOREIGN KEY (sector) REFERENCES sector_ref(sector);
ALTER TABLE ONLY security_subtype_ref
    ADD CONSTRAINT security_subtype_ref_asset_class_fkey FOREIGN KEY (asset_class) REFERENCES asset_class_ref(asset_class);
ALTER TABLE ONLY security_subtype_ref
    ADD CONSTRAINT security_subtype_ref_asset_class_security_type_fkey FOREIGN KEY (asset_class, security_type) REFERENCES security_type_ref(asset_class, security_type);
ALTER TABLE ONLY security_type_ref
    ADD CONSTRAINT security_type_ref_asset_class_fkey FOREIGN KEY (asset_class) REFERENCES asset_class_ref(asset_class);

-- Immutability guard: these are controlled vocabularies, changed only by a deliberate
-- migration. REVOKE UPDATE/DELETE so any accidental mutation errors out - both for the
-- indexer role (stl_readwrite, which stl_read_write inherits) AND for the table owner
-- (stl_migrator), since a non-superuser owner's own writes are denied once revoked, so a
-- stray fix-migration fails loudly instead of silently changing reference data.
-- Guarded by role existence: stl_readwrite is created by 20260122_140100; stl_migrator is
-- infra-provisioned (bootstrap-db.yaml / Terraform) and absent in unit-test databases.
-- INSERT is left intact so the seed above and deliberate future additions still work.
DO $$
DECLARE
    tbl  text;
    role text;
BEGIN
    FOREACH tbl IN ARRAY ARRAY[
        'asset_class_ref','security_type_ref','security_subtype_ref','credit_rating_ref',
        'sector_ref','industry_group_ref','currency_ref','country_ref','deal_type_ref',
        'entity_type_ref','counterparty_role_ref','origination_type_ref','corporate_action_type_ref']
    LOOP
        FOREACH role IN ARRAY ARRAY['stl_readwrite','stl_migrator']
        LOOP
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role) THEN
                EXECUTE format('REVOKE UPDATE, DELETE ON %I FROM %I', tbl, role);
            END IF;
        END LOOP;
    END LOOP;
END
$$;

INSERT INTO migrations (filename) VALUES ('20260630_130000_create_reference_tables.sql') ON CONFLICT (filename) DO NOTHING;
