-- Add Robinhood Chain for Grove ALM indexing.
INSERT INTO chain (chain_id, name) VALUES (4663, 'Robinhood Chain') ON CONFLICT DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260827_100000_add_robinhood_chain.sql')
ON CONFLICT (filename) DO NOTHING;
