-- Mint prime_key: the opaque, permanent handle every prime-scoped API response carries
-- (ADR-0005 decision 1). A real prime's key is a literal, so staging, prod and a fresh local
-- database agree on it. The DEFAULT exists only so test fixtures can seed a prime without
-- one; it is self-describing rather than plausible, and TestPrimeNamesNeverRemap
-- (db/migrator) rejects any prime whose key still carries the `prm_unminted_` prefix.

ALTER TABLE prime ADD COLUMN prime_key TEXT;

UPDATE prime p
SET prime_key = v.prime_key
FROM (VALUES
    ('spark', 'prm_2d3ceee8415e59f3'),
    ('grove', 'prm_9612b110a1975ba4'),
    ('obex',  'prm_11db7b73f1333a63')
) AS v(name, prime_key)
WHERE p.name = v.name;

DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM prime WHERE prime_key IS NULL) THEN
            RAISE EXCEPTION
                'Migration aborted: prime rows carry no key: %. Mint one in this migration — a key is assigned once and never changes.',
                (SELECT string_agg(name, ', ') FROM prime WHERE prime_key IS NULL);
        END IF;
    END $$;

ALTER TABLE prime ALTER COLUMN prime_key SET DEFAULT 'prm_unminted_' || substr(replace(gen_random_uuid()::text, '-', ''), 1, 12);
ALTER TABLE prime ALTER COLUMN prime_key SET NOT NULL;
ALTER TABLE prime ADD CONSTRAINT prime_prime_key_key UNIQUE (prime_key);

-- The name is the preferred external identifier, so a name the API cannot address must not
-- exist. Both constraints mirror `PRIME_NAME_PATTERN` in app/api/_validators.py: the slug
-- alphabet, and the 0x prefix the resolver uses to tell a name from an address.
ALTER TABLE prime ADD CONSTRAINT prime_name_is_a_slug CHECK (name ~ '^[a-z0-9][a-z0-9_-]{0,62}$');
ALTER TABLE prime ADD CONSTRAINT prime_name_is_not_an_address CHECK (name !~* '^0x');

CREATE OR REPLACE FUNCTION prime_key_immutable() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.prime_key IS DISTINCT FROM OLD.prime_key THEN
        RAISE EXCEPTION
            'prime_key is minted once and never changes: prime % holds %, refusing to re-key it to % (a rebrand adds a name; it does not re-key the prime)',
            OLD.name, OLD.prime_key, NEW.prime_key;
    END IF;
    RETURN NEW;
END $$;
COMMENT ON FUNCTION prime_key_immutable() IS 'Rejects any UPDATE that changes prime.prime_key. Column-scoped rather than the reference tables'' whole-row immutability, because prime.name and prime.vault_address stay mutable by design (ADR-0005 decision 1).';

CREATE TRIGGER prime_key_immutable
    BEFORE UPDATE OF prime_key ON prime
    FOR EACH ROW EXECUTE FUNCTION prime_key_immutable();

COMMENT ON COLUMN prime.prime_key IS 'The prime''s opaque public handle, minted once and never changed (enforced by the prime_key_immutable trigger). Roles: Natural key (public). Unlike name and vault_address — both time-varying attributes of this row — it is what an API client keys on. Never a URL segment; it rides the response envelope. Mint it as a literal in the migration that adds the prime, so every environment agrees on it.';

INSERT INTO migrations (filename)
VALUES ('20260908_130000_add_prime_key.sql')
ON CONFLICT (filename) DO NOTHING;
