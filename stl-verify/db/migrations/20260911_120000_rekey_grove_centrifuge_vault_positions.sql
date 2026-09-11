-- VEC-535. From axis-synome 0.2.0 (#622, 2026-07-23/24) until #904 the allocation tracker
-- keyed Grove's Centrifuge positions (JAAA, JTRSY; mainnet and Avalanche) on the ERC-7540
-- vault address, while balanceOf was read on the vault's share token. #904 keys new rows on
-- the share. This file moves the vault-keyed history onto the share key, deletes the four
-- rows the vault keys left in allocation_position_current, and deletes the four vault rows
-- from token: a vault is not a token, and nothing else references them (every FK column
-- into token(id) and every informal token_id column counted 0 on staging, 2026-09-11).
--
-- Re-keyed, not deleted: these rows are the only record of the positions between the two
-- switches (seven weeks, 114,734 rows on staging), and on the share key they are exactly
-- what the tracker would have written. No vault row shares its natural key with an existing
-- share row (the two key ranges abut: share rows end at block 25602782, vault rows run
-- 25602930..25953063, share rows resume at 25953193), and token_id is in the primary key,
-- so an overlap would fail the UPDATE instead of duplicating a row.
--
-- Precondition: the allocation trackers on the target environment already run #904
-- (97b2be06 or later). The migrate Job is a PreSync hook, so a tracker still writing the
-- vault key re-creates the cache rows on its next sweep after this file has been recorded
-- as applied.
--
-- Ids are resolved by (chain_id, address) because they differ per environment; a database
-- the tracker never ran on resolves no pair and applies nothing. A vault whose share token
-- row is missing is left alone: the share row is created by the new tracker, so its
-- absence means the precondition does not hold on that database, and a failing PreSync
-- hook would pin the environment on the very image that writes the vault key.

SET LOCAL lock_timeout = '10s';

-- The UPDATE decompresses the touched columnstore batches; the default cap of 100,000
-- tuples per transaction is below the row count on staging.
SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 500000;

DO $$
DECLARE
    pair        RECORD;
    moved       BIGINT;
    total_moved BIGINT := 0;
    pairs       INT    := 0;
BEGIN
    FOR pair IN
        SELECT m.label, m.chain_id, m.proxy, v.id AS vault_id, s.id AS share_id
        FROM (VALUES
            ('mainnet JAAA',    1,     '\x491edfb0b8b608044e227225c715981a30f3a44e'::bytea,
                                       '\x4880799ee5200fc58da299e965df644fbf46780b'::bytea,
                                       '\x5a0f93d040de44e78f251b03c43be9cf317dcf64'::bytea),
            ('mainnet JTRSY',   1,     '\x491edfb0b8b608044e227225c715981a30f3a44e'::bytea,
                                       '\xfe6920eb6c421f1179ca8c8d4170530cdbdfd77a'::bytea,
                                       '\x8c213ee79581ff4984583c6a801e5263418c4b86'::bytea),
            ('avalanche JAAA',  43114, '\x7107dd8f56642327945294a18a4280c78e153644'::bytea,
                                       '\x1121f4e21ed8b9bc1bb9a2952cdd8639ac897784'::bytea,
                                       '\x58f93d6b1ef2f44ec379cb975657c132cbed3b6b'::bytea),
            ('avalanche JTRSY', 43114, '\x7107dd8f56642327945294a18a4280c78e153644'::bytea,
                                       '\xfe6920eb6c421f1179ca8c8d4170530cdbdfd77a'::bytea,
                                       '\xa5d465251fbcc907f5dd6bb2145488dfc6a2627b'::bytea)
        ) AS m(label, chain_id, proxy, vault_addr, share_addr)
        JOIN token v ON v.chain_id = m.chain_id AND v.address = m.vault_addr
        JOIN token s ON s.chain_id = m.chain_id AND s.address = m.share_addr
        ORDER BY m.chain_id, m.label
    LOOP
        -- Literals, not bind parameters: the columnstore prunes batches only on constant
        -- predicates over the segmentby columns (chain_id, token_id, proxy_address).
        EXECUTE format(
            'UPDATE allocation_position SET token_id = %s '
            'WHERE chain_id = %s AND token_id = %s AND proxy_address = %L',
            pair.share_id, pair.chain_id, pair.vault_id, pair.proxy);
        GET DIAGNOSTICS moved = ROW_COUNT;

        DELETE FROM allocation_position_current
        WHERE chain_id = pair.chain_id AND token_id = pair.vault_id;

        DELETE FROM token WHERE id = pair.vault_id;

        -- The tracker keeps created_at_block at the earliest observation (LEAST on
        -- conflict); the share's history now starts earlier than its row said.
        UPDATE token
        SET created_at_block = LEAST(created_at_block,
            (SELECT min(block_number) FROM allocation_position WHERE token_id = pair.share_id))
        WHERE id = pair.share_id;

        pairs       := pairs + 1;
        total_moved := total_moved + moved;
        RAISE NOTICE 'VEC-535 %: % allocation_position rows re-keyed % -> %',
            pair.label, moved, pair.vault_id, pair.share_id;
    END LOOP;

    RAISE NOTICE 'VEC-535: % vault/share pairs processed, % allocation_position rows re-keyed',
        pairs, total_moved;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260911_120000_rekey_grove_centrifuge_vault_positions.sql')
ON CONFLICT (filename) DO NOTHING;
