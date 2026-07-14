-- Canonical position_id helper (VEC-400): the single hash + readable-key contract every
-- per-protocol position materializer uses, so all protocols produce identical ids from the
-- same inputs. Dual key: position_id (sha256 -> bytea(32), the joinable PK/FK) and
-- position_key (the readable canonical string, the hash pre-image, for humans/debugging).

-- Identity is holder + instrument. No classifications live in the id: deal_type and the old
-- leg discriminator are mutable classification schemes, so they are carried as looked-up
-- attributes on position_classification (VEC-401), not hashed. The leg split (one Morpho row
-- into supply/borrow/collateral, an Aave aToken vs debtToken) is preserved by the instrument
-- itself, which resolves to distinct instruments (VEC-412 bridge / VEC-413 debt_token). The
-- holder is a single id resolved via entity_master, so an on-chain wallet and a prime collapse
-- to one holder; prime-vs-wallet is an attribute, not part of the id.

-- position_key: the canonical identity string
--   chain_id;protocol_id;instrument_key;holder_id
-- Nullable structural fields (chain_id/protocol_id) render as empty between the ';' delimiters
-- (so field positions are stable); instrument_key and holder_id are required. instrument_key is
-- globally unique (VEC-412 bridge), so it no longer carries a kind namespace prefix.
-- IMMUTABLE so it can back a generated column or index. Fail hard on bad inputs rather than
-- emit a silently-wrong identity.
CREATE OR REPLACE FUNCTION public.position_key(
  _chain_id integer, _protocol_id bigint, _instrument_key text, _holder_id text
) RETURNS text
  LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  IF _instrument_key IS NULL THEN
    RAISE EXCEPTION 'position_key: instrument_key is required';
  END IF;
  IF _holder_id IS NULL THEN
    RAISE EXCEPTION 'position_key: holder_id is required (a position always has a holder; resolve it via entity_master before hashing)';
  END IF;
  RETURN concat_ws(';',
    coalesce(_chain_id::text, ''),
    coalesce(_protocol_id::text, ''),
    _instrument_key,
    _holder_id);
END $$;
COMMENT ON FUNCTION public.position_key(integer, bigint, text, text) IS
  '[Operational] Canonical position identity string (VEC-400): chain_id;protocol_id;instrument_key;holder_id. Human-readable descriptor and the pre-image of position_id(). Identity is holder + instrument only; classifications (deal_type, leg) are looked-up attributes, not part of the key. Nullable structural fields render empty; instrument_key and holder_id required. Holder is the unified entity_master holder id.';

-- position_id: sha256 of the canonical string -> bytea(32), the joinable PK stamped onto every
-- position and its downstream tables. sha256() is a built-in (PG 11+); no pgcrypto extension.
-- Delegates to position_key so the id and the readable key can never diverge.
CREATE OR REPLACE FUNCTION public.position_id(
  _chain_id integer, _protocol_id bigint, _instrument_key text, _holder_id text
) RETURNS bytea
  LANGUAGE sql IMMUTABLE AS $$
  SELECT sha256(convert_to(
    public.position_key(_chain_id, _protocol_id, _instrument_key, _holder_id),
    'UTF8'));
$$;
COMMENT ON FUNCTION public.position_id(integer, bigint, text, text) IS
  '[Operational] Canonical position_id (VEC-400): sha256 of position_key() -> bytea(32). The identity every per-protocol materializer stamps; excludes the time/version axis, so one position_id maps to many observations. Pair with position_key() for the readable form.';

INSERT INTO migrations (filename) VALUES ('20260713_140000_create_position_id_helper.sql') ON CONFLICT (filename) DO NOTHING;
