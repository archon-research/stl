-- Seed Maple Finance protocol (MapleGlobals contract on mainnet)
-- https://github.com/maple-labs/address-registry/blob/main/MapleAddressRegistryETH.md
INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at)
VALUES (1, '\x804a6F5F667170F545Bf14e5DDB48C70B788390C'::bytea, 'Maple Finance', 'rwa', 11964925, NOW())
ON CONFLICT (chain_id, address) DO NOTHING;
