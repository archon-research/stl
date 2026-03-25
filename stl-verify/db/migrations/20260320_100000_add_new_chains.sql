-- Add new blockchain chains for multi-chain watcher support.
INSERT INTO chain (chain_id, name) VALUES (10, 'Optimism') ON CONFLICT DO NOTHING;
INSERT INTO chain (chain_id, name) VALUES (130, 'Unichain') ON CONFLICT DO NOTHING;
INSERT INTO chain (chain_id, name) VALUES (8453, 'Base') ON CONFLICT DO NOTHING;
INSERT INTO chain (chain_id, name) VALUES (42161, 'Arbitrum One') ON CONFLICT DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260320_100000_add_new_chains.sql')
ON CONFLICT (filename) DO NOTHING;
