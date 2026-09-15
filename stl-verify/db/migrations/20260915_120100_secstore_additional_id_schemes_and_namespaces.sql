-- VEC-616: the identifier schemes and key namespaces the asset-identity model needs beyond
-- ADR-0007's ratified set. Both vocabularies are seed-once reference tables, so this is what
-- "a new namespace is one row, not a schema change" looks like in practice — the path every
-- later source takes, including a non-EVM asset with no contract.

-- COINGECKO and MAPLE_SYMBOL are the asset-identity model's Decision 7 schemes.
--
-- CEX_SYMBOL is ONE scheme carrying the venue inside the value, not one scheme per exchange:
-- a scheme per venue grows a governed reference table by a row for every exchange we ever
-- touch, and the bounded form is what TICKER already does for the same many-to-many-over-time
-- shape (unique_current = false). alias_register has no payload column, so the venue lives in
-- id_value or nowhere. OPEN: the model's own Decision 8 asks for venue-scoped scheme names;
-- this ships the bounded form and the choice wants confirming before the CEX load lands.
INSERT INTO id_scheme_vocabulary (id_scheme, applies_to, value_form, unique_current, description) VALUES
 ('COINGECKO','{SECURITY}','CoinGecko asset id, lowercase slug', true,'CoinGecko asset id, assigned by CoinGecko'),
 ('MAPLE_SYMBOL','{SECURITY}','Maple pool symbol', true,'Maple pool symbol, assigned by Maple'),
 ('CEX_SYMBOL','{SECURITY}','venue '':'' symbol, e.g. coinbase:BTC-USD; the venue is IN the value because the scheme is not venue-scoped', false,'Exchange-listed symbol at a named venue. Not unique_current: one security is listed at many venues, and a venue reassigns a symbol')
ON CONFLICT (id_scheme) DO NOTHING;

-- The two namespaces the position projections already emit keys for and the ratified four do
-- not cover. Both are the projection's own native key, unchanged: composing or rewriting one
-- here would change position_id's pre-image for positions that already exist.
--
-- loan_address is deliberately not folded into token_address. A Maple loan contract is not a
-- token: it has no balance, no decimals and no holder, so a register row under token_address
-- would make every reader that trusts the namespace wrong about what it is looking at.
INSERT INTO key_namespace_vocabulary (key_namespace, description) VALUES
 ('alm_proxy_token','Prime ALM: alm_proxy_address '':'' token_address, each lowercase hex, no 0x (stl#931)'),
 ('loan_address','the loan contract''s own on-chain address, lowercase hex, no 0x (Maple OTL, stl#933). A loan contract is not a token, so this is not token_address')
ON CONFLICT (key_namespace) DO NOTHING;

INSERT INTO migrations (filename) VALUES ('20260915_120100_secstore_additional_id_schemes_and_namespaces.sql') ON CONFLICT (filename) DO NOTHING;
