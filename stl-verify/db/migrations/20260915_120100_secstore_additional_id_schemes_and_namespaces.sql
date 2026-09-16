-- VEC-616: the identifier schemes and key namespaces the asset-identity model needs beyond
-- ADR-0007's ratified set. Both vocabularies are seed-once reference tables, so this is what
-- "a new namespace is one row, not a schema change" looks like in practice — the path every
-- later source takes, including a non-EVM asset with no contract.

-- COINGECKO and MAPLE_SYMBOL are the asset-identity model's Decision 7 schemes.
--
-- CEX symbols are VENUE-SCOPED, one scheme per exchange (the model's Decision 8), because a
-- venue's pair string is meaningless outside that venue. cex_orderbook_snapshots.symbol already
-- records why and says to join on (exchange, symbol) and never on symbol alone; the fleet's own
-- config is the evidence — the same six markets are BTC-USD / BTC-USDT / XBT-USD-shaped across
-- coinbase, okx and kraken, differing in separator, asset spelling (XBT, not BTC) and even quote
-- asset. Nothing external fixes this: CCXT's unified BASE/QUOTE is a client-side convention over
-- per-exchange market ids, and ISO 24165 standardises the TOKEN, not the venue's pair.
--
-- Scoping buys two things one shared scheme cannot. value_form states each venue's ACTUAL form
-- instead of "varies by venue", which is the column admitting it cannot do its job. And each
-- scheme is unique_current: within one venue a symbol names one market, so the duplicate-target
-- DQ rule applies to CEX symbols — TICKER is false precisely BECAUSE it is not venue-scoped,
-- where the same string means different securities on different venues at the same instant.
--
-- Only the venues we index are seeded; inventing rows for venues we do not read would be
-- inventing data. A new venue is one row here alongside its deployment and configmap.
INSERT INTO id_scheme_vocabulary (id_scheme, applies_to, value_form, unique_current, description) VALUES
 ('COINGECKO','{SECURITY}','CoinGecko asset id, lowercase slug', true,'CoinGecko asset id, assigned by CoinGecko'),
 ('MAPLE_SYMBOL','{SECURITY}','Maple pool symbol', true,'Maple pool symbol, assigned by Maple'),
 ('CEX_SYMBOL_COINBASE','{SECURITY}','Coinbase product id: BASE-QUOTE, uppercase, hyphen — e.g. BTC-USD', true,'Symbol as listed on Coinbase (cex_orderbook_snapshots.exchange = ''coinbase'')'),
 ('CEX_SYMBOL_OKX','{SECURITY}','OKX SPOT instrument id: BASE-QUOTE, uppercase, hyphen, USDT-quoted across the indexed set — e.g. BTC-USDT', true,'Symbol as listed on OKX (exchange = ''okx''). A USDT quote is not a USD quote; the basis is VEC-458''s'),
 ('CEX_SYMBOL_KRAKEN','{SECURITY}','Kraken AssetPairs wsname verbatim: slash-separated, venue asset names — e.g. XBT/USD, not BTC/USD', true,'Symbol as listed on Kraken (exchange = ''kraken''). Taken from the venue API rather than built by convention, because Kraken''s asset names are its own')
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
