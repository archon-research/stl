//go:build integration

package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// seedReferencePrime inserts (or reuses) a prime row keyed by name and returns
// its id. The vault address is derived from name by hashing rather than
// truncating, so two names sharing a long common prefix still resolve to
// distinct vault_address values and never collide on the unique constraint.
//
// ON CONFLICT DO NOTHING plus a follow-up SELECT, never DO UPDATE: the
// no-op DO UPDATE arm still requires UPDATE privilege (db/migrations/AGENTS.md),
// and this file's helpers are meant to double as the append-only pattern other
// reference-table seeders copy.
func seedReferencePrime(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name string) int64 {
	t.Helper()

	hash := sha256.Sum256([]byte(name))
	vaultHex := hex.EncodeToString(hash[:20])

	if _, err := pool.Exec(ctx, `
		INSERT INTO prime (name, vault_address) VALUES ($1, decode($2, 'hex'))
		ON CONFLICT (name) DO NOTHING`,
		name, vaultHex); err != nil {
		t.Fatalf("seeding prime %q: %v", name, err)
	}

	var primeID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM prime WHERE name = $1`, name).Scan(&primeID); err != nil {
		t.Fatalf("reading seeded prime %q: %v", name, err)
	}
	return primeID
}

// newReferenceRepoTxm builds the TxManager a reference-data repository needs.
func newReferenceRepoTxm(t *testing.T, pool *pgxpool.Pool) *TxManager {
	t.Helper()

	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	return txm
}
