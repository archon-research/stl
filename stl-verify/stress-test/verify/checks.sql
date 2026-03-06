-- Stress test verification queries.
-- Run with: psql $DATABASE_URL -v chain_id=1 -f stress-test/verify/checks.sql
--
-- All queries should return 0 for a healthy canonical chain.

-- 1. No gaps in canonical block numbers
-- Returns: number of missing block numbers in the canonical chain.
SELECT count(*) AS gaps
FROM generate_series(
  (SELECT min(number) FROM block_states WHERE chain_id = :chain_id AND NOT is_orphaned),
  (SELECT max(number) FROM block_states WHERE chain_id = :chain_id AND NOT is_orphaned)
) AS s(n)
LEFT JOIN block_states b ON b.number = s.n AND b.chain_id = :chain_id AND NOT b.is_orphaned
WHERE b.number IS NULL;

-- 2. No unexpected orphaned blocks in the recent window (last 64 blocks)
-- Orphaned blocks within the finality window should only exist for reorged forks.
SELECT count(*) AS unexpected_orphans
FROM block_states
WHERE chain_id = :chain_id
  AND is_orphaned
  AND number > (SELECT max(number) - 64 FROM block_states WHERE chain_id = :chain_id);

-- 3. ParentHash continuity on the canonical chain
-- Returns: number of canonical blocks whose parent_hash does not match the previous block's hash.
SELECT count(*) AS broken_parent_links
FROM block_states b
JOIN block_states p
  ON p.number = b.number - 1
  AND p.chain_id = b.chain_id
  AND NOT p.is_orphaned
WHERE NOT b.is_orphaned
  AND b.chain_id = :chain_id
  AND b.parent_hash != p.hash;
