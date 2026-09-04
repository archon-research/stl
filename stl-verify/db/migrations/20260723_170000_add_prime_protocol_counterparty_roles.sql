-- Add PRIME and PROTOCOL to the ref_counterparty_role controlled vocabulary.
--   PRIME    : internal Sky/Prime SPV that holds the position — gives primes a first-class role
--              instead of leaving it null.
--   PROTOCOL : the on-chain protocol itself, distinct from PROTOCOL_OPERATOR (the operating company).
--              e.g. the maple protocol (PROTOCOL) is operated by Maple Finance (PROTOCOL_OPERATOR).
-- Append-only controlled vocab: new rows only, never modify existing. Idempotent.
INSERT INTO ref_counterparty_role VALUES ('PRIME', 'Internal Sky/Prime SPV that holds the position', DEFAULT) ON CONFLICT DO NOTHING;
INSERT INTO ref_counterparty_role VALUES ('PROTOCOL', 'The on-chain protocol itself (distinct from PROTOCOL_OPERATOR, the operating company)', DEFAULT) ON CONFLICT DO NOTHING;

INSERT INTO migrations (filename) VALUES ('20260723_170000_add_prime_protocol_counterparty_roles.sql') ON CONFLICT (filename) DO NOTHING;
