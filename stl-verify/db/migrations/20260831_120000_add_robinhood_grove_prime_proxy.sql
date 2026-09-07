-- Declare Grove's Robinhood Chain ALM proxy; pairs with the axis-synome contract
-- entry added in the same PR (contract + prime_proxy row land together).
INSERT INTO prime_proxy (chain_id, proxy_address, prime_id)
SELECT 4663, decode('29626c2d8ca49a51e4deceec5499e52983c42bd5', 'hex'), p.id
FROM prime p
WHERE p.name = 'grove'
ON CONFLICT DO NOTHING;

DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM prime_proxy WHERE chain_id = 4663) THEN
            RAISE EXCEPTION
                'Migration aborted: no prime named grove, robinhood proxy row not inserted.';
        END IF;
    END $$;

INSERT INTO migrations (filename)
VALUES ('20260831_120000_add_robinhood_grove_prime_proxy.sql')
ON CONFLICT (filename) DO NOTHING;
