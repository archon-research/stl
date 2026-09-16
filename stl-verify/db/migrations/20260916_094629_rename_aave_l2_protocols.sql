-- The four L2 Aave V3 deployments baked the chain into protocol.name while the
-- mainnet and Morpho rows did not, so names diverged per deployment. The chain
-- lives in chain_id and the UI renders it as its own chip next to the protocol.

UPDATE protocol SET name = 'Aave V3', updated_at = NOW()
WHERE (chain_id, address) IN (
    (43114, '\x794a61358D6845594F94dc1DB02A252b5b4814aD'::bytea),
    (42161, '\x794a61358D6845594F94dc1DB02A252b5b4814aD'::bytea),
    (10,    '\x794a61358D6845594F94dc1DB02A252b5b4814aD'::bytea),
    (8453,  '\xA238Dd80C259a72e81d7e4664a9801593F98d1c5'::bytea)
);

INSERT INTO migrations (filename)
VALUES ('20260916_094629_rename_aave_l2_protocols.sql')
ON CONFLICT (filename) DO NOTHING;
