-- migrate: no-transaction
--
-- VEC-353: the registry "first known block" columns (token.created_at_block,
-- "user".first_seen_block) overload 0 to mean two different things: the genesis
-- block, and "no data" (the Go int64 zero value of an off-block-context caller).
-- The LEAST() merge in the get-or-create upserts reads that 0 as a real,
-- very-early block and silently clobbers the stored value down to 0.
--
-- Convert the already-clobbered zeros to NULL ("unknown") so a later real
-- on-chain observation self-heals the row via LEAST() (which ignores NULL),
-- then forbid future zeros at the column level. With the columns nullable,
-- off-block-context callers now insert NULL and LEAST() preserves the stored
-- block instead of clobbering it.
--
-- Runs outside a transaction (each statement commits on its own) so the CHECK
-- constraint can be added NOT VALID — a brief ACCESS EXCLUSIVE lock with no table
-- scan — then validated in a separate step under the weaker SHARE UPDATE EXCLUSIVE
-- lock, which does not block concurrent writes. A single-transaction ADD CONSTRAINT
-- holds ACCESS EXCLUSIVE for the entire validation scan instead ("user" is ~316k
-- rows on staging and larger on prod), stalling watcher/indexer writes for that
-- whole scan.
--
-- No-transaction migrations are not atomic, so each statement is idempotent and
-- safe to re-run after a partial failure: the UPDATEs find no zeros on a second
-- pass, and each constraint is dropped-if-exists before being re-added.

UPDATE token SET created_at_block = NULL WHERE created_at_block = 0;
UPDATE "user" SET first_seen_block = NULL WHERE first_seen_block = 0;

ALTER TABLE token DROP CONSTRAINT IF EXISTS token_created_at_block_positive;
ALTER TABLE token
    ADD CONSTRAINT token_created_at_block_positive
    CHECK (created_at_block IS NULL OR created_at_block > 0) NOT VALID;
ALTER TABLE token VALIDATE CONSTRAINT token_created_at_block_positive;

ALTER TABLE "user" DROP CONSTRAINT IF EXISTS user_first_seen_block_positive;
ALTER TABLE "user"
    ADD CONSTRAINT user_first_seen_block_positive
    CHECK (first_seen_block IS NULL OR first_seen_block > 0) NOT VALID;
ALTER TABLE "user" VALIDATE CONSTRAINT user_first_seen_block_positive;

INSERT INTO migrations (filename)
VALUES ('20260616_120000_registry_block_nullify_zero_and_constrain.sql')
ON CONFLICT (filename) DO NOTHING;
