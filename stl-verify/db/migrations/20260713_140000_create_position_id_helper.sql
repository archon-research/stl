-- Canonical position_id helper (VEC-400): the single hash + readable-key contract every
-- per-protocol position materializer uses, so all protocols produce identical ids from the
-- same inputs. Dual key: position_id (sha256 -> bytea(32), the joinable PK/FK) and
-- position_key (the readable canonical string, the hash pre-image, for humans/debugging).

-- position_key: the canonical identity string
--   chain_id;protocol_id;<kind>:<instrument_key>;user_id;prime_id;deal_type_code
-- Nullable identity fields (chain_id/protocol_id/user_id/prime_id) render as empty between the
-- ';' delimiters (so field positions are stable); kind/instrument_key/deal_type_code are
-- required; exactly one of user_id / prime_id must be set (a position always has a holder).
-- IMMUTABLE so it can back a generated
-- column or index. Fail hard on bad inputs rather than emit a silently-wrong identity.
CREATE OR REPLACE FUNCTION public.position_key(
  _chain_id integer, _protocol_id bigint, _kind text, _instrument_key text,
  _user_id bigint, _prime_id bigint, _deal_type_code text
) RETURNS text
  LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  IF _kind IS NULL OR _instrument_key IS NULL THEN
    RAISE EXCEPTION 'position_key: kind and instrument_key are required (kind=%, instrument_key=%)', _kind, _instrument_key;
  END IF;
  IF _deal_type_code IS NULL THEN
    RAISE EXCEPTION 'position_key: deal_type_code is required (assign it in position_classification before hashing)';
  END IF;
  IF (_user_id IS NULL) = (_prime_id IS NULL) THEN
    RAISE EXCEPTION 'position_key: exactly one of user_id / prime_id must be set (user_id=%, prime_id=%)', _user_id, _prime_id;
  END IF;
  RETURN concat_ws(';',
    coalesce(_chain_id::text, ''),
    coalesce(_protocol_id::text, ''),
    _kind || ':' || _instrument_key,
    coalesce(_user_id::text, ''),
    coalesce(_prime_id::text, ''),
    _deal_type_code);
END $$;
COMMENT ON FUNCTION public.position_key(integer, bigint, text, text, bigint, bigint, text) IS
  '[Operational] Canonical position identity string (VEC-400): chain_id;protocol_id;<kind>:<instrument_key>;user_id;prime_id;deal_type_code. Human-readable descriptor and the pre-image of position_id(). Nullable identity fields render empty; kind/instrument_key/deal_type_code required; user_id XOR prime_id.';

-- position_id: sha256 of the canonical string -> bytea(32), the joinable PK stamped onto every
-- position and its downstream tables. sha256() is a built-in (PG 11+); no pgcrypto extension.
-- Delegates to position_key so the id and the readable key can never diverge.
CREATE OR REPLACE FUNCTION public.position_id(
  _chain_id integer, _protocol_id bigint, _kind text, _instrument_key text,
  _user_id bigint, _prime_id bigint, _deal_type_code text
) RETURNS bytea
  LANGUAGE sql IMMUTABLE AS $$
  SELECT sha256(convert_to(
    public.position_key(_chain_id, _protocol_id, _kind, _instrument_key, _user_id, _prime_id, _deal_type_code),
    'UTF8'));
$$;
COMMENT ON FUNCTION public.position_id(integer, bigint, text, text, bigint, bigint, text) IS
  '[Operational] Canonical position_id (VEC-400): sha256 of position_key() -> bytea(32). The identity every per-protocol materializer stamps; excludes the time/version axis, so one position_id maps to many observations. Pair with position_key() for the readable form.';

INSERT INTO migrations (filename) VALUES ('20260713_140000_create_position_id_helper.sql') ON CONFLICT (filename) DO NOTHING;
