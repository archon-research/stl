-- VEC-152: Delete deprecated rsETH feed rows.
--
-- Both feeds have been retired provider-side on Ethereum mainnet:
--   * Chainlink proxy 0x03c68933f7a3F76875C0bc670a58e69294cDFD01:
--       phaseAggregators(3) == 0x0 (current phase has no aggregator).
--       Feed was decommissioned on/around 2026-03-27.
--   * Redstone proxy 0xA736eAe8805dDeFFba40cAB8c99bCB309dEaBd9B:
--       implementation 0x1b7406b06ce2ff145c274f39941211c2a146194a is
--       literally named "TerminatedContract" with the comment:
--       "This contract was terminated by the RedStone team and can no
--       longer be used."
--
-- Neither has ever successfully produced an onchain_token_price row.
-- rsETH remains priced by sparklend with no gap.

DELETE FROM oracle_asset
WHERE token_id IN (SELECT id FROM token WHERE chain_id = 1 AND symbol = 'rsETH')
  AND oracle_id IN (SELECT id FROM oracle WHERE name IN ('chainlink','redstone'))
  AND feed_address IN (
    '\x03c68933f7a3F76875C0bc670a58e69294cDFD01'::BYTEA,
    '\xA736eAe8805dDeFFba40cAB8c99bCB309dEaBd9B'::BYTEA
  );

INSERT INTO migrations (filename)
VALUES ('20260423_071108_delete_deprecated_rseth_feeds.sql')
ON CONFLICT (filename) DO NOTHING;
