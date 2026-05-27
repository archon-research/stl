-- The Curve worker v1 ships without a per-Transfer balanceOf(user) multicall,
-- so it can write only the signed delta (not the post-event absolute balance).
-- Loosen curve_user_lp_position.lp_balance so writers may pass NULL until a
-- follow-up PR wires the balanceOf read path. Consumers reconstruct an
-- absolute balance via running SUM over delta, windowed by (user, pool).

ALTER TABLE curve_user_lp_position
    ALTER COLUMN lp_balance DROP NOT NULL;

INSERT INTO migrations (filename)
VALUES ('20260521_140000_curve_user_lp_position_nullable_balance.sql')
ON CONFLICT (filename) DO NOTHING;
