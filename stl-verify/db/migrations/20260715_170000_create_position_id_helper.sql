-- Canonical position_id helper (VEC-400): the single hash + readable-key contract every
-- per-protocol position materializer uses, so all protocols produce identical ids from the
-- same inputs. Dual key: position_id (sha256 -> bytea, 32 bytes, the joinable PK/FK) and
-- position_key (the readable canonical string, the hash pre-image, for humans/debugging).

-- Identity is holder + instrument. No classifications live in the id: deal_type and the old
-- leg discriminator are mutable classification schemes, so they are carried as looked-up
-- attributes on position_classification (VEC-401), not hashed. The leg split (one Morpho row
-- into supply/borrow/collateral, an Aave aToken vs debtToken) is preserved by the instrument
-- itself, which resolves to distinct instruments (VEC-412 bridge / VEC-413 debt_token). The
-- holder is the native on-chain holder id (a wallet address, or a prime's vault address), NOT a
-- resolved entity: the wallet/prime -> single entity_master entity mapping happens downstream
-- (entity_ref_codes / entity_master), so the id stays computable at materialization time without
-- a master lookup, and never shifts when that mapping is re-curated.

-- position_key: the canonical identity string
--   chain_id;protocol_id;instrument_key;holder_id
-- Nullable structural fields (chain_id/protocol_id) render as empty between the ';' delimiters
-- (so field positions are stable); instrument_key and holder_id are required and non-empty, and
-- must not contain the ';' delimiter (unescaped, it would collide distinct identities). chain_id and
-- protocol_id stay nullable, but each protocol's materializer must use a FIXED, documented NULL-ness
-- convention for them: two call sites that disagree (one NULL, one a value) for the same position
-- would fork it into two ids with no error. chain_id is load-bearing, not redundant: when set it
-- disambiguates the same native instrument_key reused across chains (VEC-412).
-- Same fork risk applies to the TEXT of instrument_key/holder_id: raw on-chain addresses feed the
-- hash, and 0xAbC (EIP-55), 0xabc, and unprefixed hex are distinct pre-images, so materializers must
-- pass a single canonical form (addresses as lowercase hex, no 0x prefix; the VEC-412 native-key
-- form). The helper cannot normalize (it is IMMUTABLE and the fields are opaque), so the convention
-- is the materializer's contract, like the NULL-ness convention above.
-- IMMUTABLE so it can back a generated column or index. Fail hard on bad inputs rather than
-- emit a silently-wrong identity.
CREATE OR REPLACE FUNCTION public.position_key(
  _chain_id integer, _protocol_id bigint, _instrument_key text, _holder_id text
) RETURNS text
  LANGUAGE plpgsql IMMUTABLE
  SET search_path = pg_catalog, public AS $$
BEGIN
  IF _instrument_key IS NULL OR _instrument_key = '' THEN
    RAISE EXCEPTION 'position_key: instrument_key is required and must be non-empty';
  END IF;
  IF _holder_id IS NULL OR _holder_id = '' THEN
    RAISE EXCEPTION 'position_key: holder_id is required and must be non-empty (use the native holder id; the entity is resolved downstream, not here)';
  END IF;
  -- The ';' delimiter is not escaped, so an input containing it could collide two distinct
  -- identities onto one id: instrument_key='a;b',holder='c' and instrument_key='a',holder='b;c'
  -- both render '...;a;b;c'. Native ids (hex address, bytes32, registry:ilk, provider:package)
  -- never contain ';'; reject it rather than emit a silently-ambiguous identity.
  IF strpos(_instrument_key, ';') > 0 OR strpos(_holder_id, ';') > 0 THEN
    RAISE EXCEPTION 'position_key: instrument_key/holder_id must not contain the '';'' delimiter';
  END IF;
  RETURN concat_ws(';',
    coalesce(_chain_id::text, ''),
    coalesce(_protocol_id::text, ''),
    _instrument_key,
    _holder_id);
END $$;
COMMENT ON FUNCTION public.position_key(integer, bigint, text, text) IS
  '[Operational] Canonical position identity string (VEC-400): chain_id;protocol_id;instrument_key;holder_id. Human-readable descriptor and the pre-image of position_id(). Identity is holder + instrument only; classifications (deal_type, leg) are looked-up attributes, not part of the key. Nullable structural fields render empty; instrument_key and holder_id are required, non-empty, and must not contain '';''. chain_id/protocol_id stay nullable but each protocol materializer must use a fixed, documented NULL-ness convention so the same position never forks into two ids. Holder is the native on-chain holder id (wallet or prime vault address); the entity is resolved downstream, not in the key.';

-- position_id: sha256 of the canonical string -> bytea, 32 bytes, the joinable PK stamped onto every
-- position and its downstream tables. sha256() is a built-in (PG 11+); no pgcrypto extension.
-- Delegates to position_key so the id and the readable key can never diverge.
CREATE OR REPLACE FUNCTION public.position_id(
  _chain_id integer, _protocol_id bigint, _instrument_key text, _holder_id text
) RETURNS bytea
  LANGUAGE sql IMMUTABLE
  SET search_path = pg_catalog, public AS $$
  SELECT sha256(convert_to(
    public.position_key(_chain_id, _protocol_id, _instrument_key, _holder_id),
    'UTF8'));
$$;
COMMENT ON FUNCTION public.position_id(integer, bigint, text, text) IS
  '[Operational] Canonical position_id (VEC-400): sha256 of position_key() -> bytea (32 bytes). The identity every per-protocol materializer stamps; excludes the time/version axis, so one position_id maps to many observations. Pair with position_key() for the readable form. Downstream tables enforce the 32-byte width via CHECK (octet_length(position_id) = 32).';

INSERT INTO migrations (filename) VALUES ('20260715_170000_create_position_id_helper.sql') ON CONFLICT (filename) DO NOTHING;
