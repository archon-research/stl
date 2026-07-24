-- Add PRIME and PROTOCOL to the counterparty_role_ref controlled vocabulary.
--   PRIME    : internal Sky/Prime SPV that holds the position (Peter, 2026-07-23 —
--              gives primes a first-class role instead of leaving it null / debating "what is a prime").
--   PROTOCOL : the on-chain protocol itself, distinct from PROTOCOL_OPERATOR (the operating company).
--              e.g. the maple protocol (PROTOCOL) is operated by Maple Finance (PROTOCOL_OPERATOR).
-- Append-only controlled vocab: new rows only, never modify existing. Idempotent.
INSERT INTO counterparty_role_ref VALUES ('PRIME', 'Internal Sky/Prime SPV that holds the position', DEFAULT) ON CONFLICT DO NOTHING;
INSERT INTO counterparty_role_ref VALUES ('PROTOCOL', 'The on-chain protocol itself (distinct from PROTOCOL_OPERATOR, the operating company)', DEFAULT) ON CONFLICT DO NOTHING;
