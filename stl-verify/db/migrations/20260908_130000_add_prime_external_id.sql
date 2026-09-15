-- Mint external_id: the opaque, permanent identifier every prime-scoped API response carries
-- (ADR-0005 decision 1). Every external_id is a literal and the column has no DEFAULT, so an insert
-- that omits one fails rather than minting a value that differs between environments —
-- which for a permanent public identifier is the failure with no recovery.

ALTER TABLE prime ADD COLUMN external_id UUID;

UPDATE prime p
SET external_id = v.external_id
FROM (VALUES
    ('spark', '4bd9ee3c-58df-4587-9c04-63b928f1a169'::uuid),
    ('grove', '9f5e309d-17a7-4fd4-b49b-009b5071afcd'::uuid),
    ('obex',  'd0906a47-9b0e-481a-b427-29514e0c2153'::uuid)
) AS v(name, external_id)
WHERE p.name = v.name;

DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM prime WHERE external_id IS NULL) THEN
            RAISE EXCEPTION
                'Migration aborted: prime rows carry no external_id: %. Mint one in this migration — an external_id is assigned once and never changes.',
                (SELECT string_agg(name, ', ') FROM prime WHERE external_id IS NULL);
        END IF;
    END $$;

ALTER TABLE prime ALTER COLUMN external_id SET NOT NULL;
ALTER TABLE prime ADD CONSTRAINT prime_external_id_key UNIQUE (external_id);

-- The name is the preferred external identifier, so a name the API cannot address must not
-- exist. Both constraints mirror `PRIME_NAME_PATTERN` in app/api/_validators.py: the slug
-- alphabet, and the 0x prefix the resolver uses to tell a name from an address.
ALTER TABLE prime ADD CONSTRAINT prime_name_is_a_slug CHECK (name ~ '^[a-z0-9][a-z0-9_-]{0,62}$');
ALTER TABLE prime ADD CONSTRAINT prime_name_is_not_an_address CHECK (name !~* '^0x');

CREATE OR REPLACE FUNCTION prime_external_id_immutable() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.external_id IS DISTINCT FROM OLD.external_id THEN
        RAISE EXCEPTION
            'external_id is minted once and never changes: prime % holds %, refusing to change it to % (a rebrand renames the prime; its external_id stays)',
            OLD.name, OLD.external_id, NEW.external_id;
    END IF;
    RETURN NEW;
END $$;
COMMENT ON FUNCTION prime_external_id_immutable() IS 'Rejects any UPDATE that changes prime.external_id. Column-scoped rather than the reference tables'' whole-row immutability, because prime.name and prime.vault_address stay mutable by design (ADR-0005 decision 1).';

CREATE TRIGGER prime_external_id_immutable
    BEFORE UPDATE OF external_id ON prime
    FOR EACH ROW EXECUTE FUNCTION prime_external_id_immutable();

COMMENT ON COLUMN prime.external_id IS 'The prime''s opaque public handle, minted once and never changed (enforced by the prime_external_id_immutable trigger). Roles: Natural key (public). Unlike name and vault_address — both time-varying attributes of this row — it is what an API client identifies the prime by. Never a URL segment; it rides the response envelope. There is deliberately no DEFAULT: mint it as a literal in the migration that adds the prime, so every environment agrees on it.';

INSERT INTO migrations (filename)
VALUES ('20260908_130000_add_prime_external_id.sql')
ON CONFLICT (filename) DO NOTHING;
