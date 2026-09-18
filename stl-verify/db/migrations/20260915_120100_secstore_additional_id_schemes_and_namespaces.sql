-- VEC-616: the identifier schemes and key namespaces the asset-identity model needs beyond
-- ADR-0007's ratified set. Both vocabularies are seed-once reference tables, so this is what
-- "a new namespace is one row, not a schema change" looks like in practice — the path every
-- later source takes, including a non-EVM asset with no contract.

-- CEX symbols are venue-scoped because a venue's pair string means nothing outside it: the same
-- six markets are BTC-USD / BTC-USDT / XBT-USD across our three venues (see the scheme rows).
-- Kraken's pattern is NULL deliberately: its wsname is taken from the venue API rather than
-- built by convention, so any regex here would be a convention we invented.
INSERT INTO id_scheme_vocabulary (id_scheme, applies_to, value_form, value_pattern, unique_current, description) VALUES
 ('COINGECKO','{SECURITY}','CoinGecko asset id, lowercase slug','^[a-z0-9]+(-[a-z0-9]+)*$', true,'CoinGecko asset id, assigned by CoinGecko'),
 ('MAPLE_SYMBOL','{SECURITY}','Maple pool symbol', NULL, true,'Maple pool symbol, assigned by Maple'),
 ('CEX_SYMBOL_COINBASE','{SECURITY}','Coinbase product id: BASE-QUOTE, uppercase, hyphen — e.g. BTC-USD','^[A-Z0-9]+-[A-Z0-9]+$', true,'Symbol as listed on Coinbase (cex_orderbook_snapshots.exchange = ''coinbase'')'),
 ('CEX_SYMBOL_OKX','{SECURITY}','OKX SPOT instrument id: BASE-QUOTE, uppercase, hyphen, USDT-quoted across the indexed set — e.g. BTC-USDT','^[A-Z0-9]+-[A-Z0-9]+$', true,'Symbol as listed on OKX (exchange = ''okx''). A USDT quote is not a USD quote; the basis is VEC-458''s'),
 ('CEX_SYMBOL_KRAKEN','{SECURITY}','Kraken AssetPairs wsname verbatim: slash-separated, venue asset names — e.g. XBT/USD, not BTC/USD', NULL, true,'Symbol as listed on Kraken (exchange = ''kraken''). Taken from the venue API rather than built by convention, because Kraken''s asset names are its own')
ON CONFLICT (id_scheme) DO NOTHING;

-- Each takes the projection's own key unchanged: composing or rewriting one here would change
-- position_id's pre-image for positions that already exist.
INSERT INTO key_namespace_vocabulary (key_namespace, description) VALUES
 ('alm_proxy_token','Prime ALM: alm_proxy_address '':'' token_address, each lowercase hex, no 0x (stl#931)'),
 ('loan_address','the loan contract''s own on-chain address, lowercase hex, no 0x (Maple OTL, stl#933). A loan contract is not a token, so this is not token_address')
ON CONFLICT (key_namespace) DO NOTHING;

INSERT INTO migrations (filename) VALUES ('20260915_120100_secstore_additional_id_schemes_and_namespaces.sql') ON CONFLICT (filename) DO NOTHING;
