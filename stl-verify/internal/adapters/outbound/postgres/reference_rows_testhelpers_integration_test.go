//go:build integration

package postgres

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// seedReferencePrime inserts (or reuses) a prime row keyed by name and returns
// its id. The vault address is derived from name — mirroring the Python tests'
// _vault_address_for — so distinctly-named primes never collide on the
// vault_address unique constraint.
func seedReferencePrime(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name string) int64 {
	t.Helper()

	vaultHex := hex.EncodeToString([]byte(name))
	vaultHex = (vaultHex + strings.Repeat("0", 40))[:40]

	var primeID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO prime (name, vault_address) VALUES ($1, decode($2, 'hex'))
		ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id`,
		name, vaultHex).Scan(&primeID); err != nil {
		t.Fatalf("seeding prime %q: %v", name, err)
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
