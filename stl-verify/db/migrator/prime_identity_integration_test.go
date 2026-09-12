//go:build integration

package migrator_test

import (
	"context"
	"sort"
	"strings"
	"testing"
)

// primeIdentityLedger is the audit record the schema does not keep: prime.name is mutable in
// place with no history, so a rename plus a reuse would silently repoint a published URL at
// another entity's figures. A prime is added here once, with the id and key its migration
// mints, and the row never changes afterwards (ADR-0005 decision 1).
var primeIdentityLedger = map[string]struct {
	id  int64
	key string
}{
	"spark": {id: 1, key: "prm_2d3ceee8415e59f3"},
	"grove": {id: 2, key: "prm_9612b110a1975ba4"},
	"obex":  {id: 3, key: "prm_11db7b73f1333a63"},
}

// TestPrimeNamesNeverRemap fails when a prime name maps to a different prime.id or prime_key
// than it did before, and when a prime reaches the database without a ledger entry. It runs
// against a freshly migrated database, so the ids it pins are the fresh-DB assignment order —
// it certifies the migration set, not a long-lived environment.
func TestPrimeNamesNeverRemap(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	rows, err := pool.Query(ctx, `SELECT name, id, prime_key FROM prime ORDER BY id`)
	if err != nil {
		t.Fatalf("read prime registry: %v", err)
	}
	defer rows.Close()

	seen := make(map[string]bool, len(primeIdentityLedger))
	for rows.Next() {
		var name, key string
		var id int64
		if err := rows.Scan(&name, &id, &key); err != nil {
			t.Fatalf("scan prime row: %v", err)
		}
		seen[name] = true

		want, ok := primeIdentityLedger[name]
		if !ok {
			t.Errorf("prime %q is in the database but not in the ledger; add it with the id and key its migration mints", name)
			continue
		}
		if id != want.id || key != want.key {
			t.Errorf("prime %q maps to (id %d, key %q), ledger says (id %d, key %q); a name never remaps",
				name, id, key, want.id, want.key)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate prime registry: %v", err)
	}

	var missing []string
	for name := range primeIdentityLedger {
		if !seen[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	for _, name := range missing {
		t.Errorf("prime %q is in the ledger but not in the database; a name is never retired, only renamed", name)
	}
}

// TestPrimeKeyCannotBeRekeyed pins the database half of "minted once, never changed": the CI
// ledger above cannot see a live environment, the trigger can.
func TestPrimeKeyCannotBeRekeyed(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	_, err := pool.Exec(ctx, `UPDATE prime SET prime_key = 'prm_0000000000000000' WHERE name = 'spark'`)
	if err == nil {
		t.Fatal("re-keying a prime should raise via the prime_key_immutable trigger")
	}
	if !strings.Contains(err.Error(), "minted once and never changes") {
		t.Errorf("UPDATE error = %v, want the prime_key immutability message", err)
	}

	// The trigger is column-scoped: a rename is still allowed, and re-stating the same key
	// alongside it is not a re-key.
	if _, err := pool.Exec(ctx,
		`UPDATE prime SET name = 'spark2', prime_key = prime_key WHERE name = 'spark'`); err != nil {
		t.Errorf("renaming a prime should stay allowed: %v", err)
	}
}

// TestPrimeNameMustBeAnAddressableSlug pins that a name the API cannot put in a path segment
// cannot reach the registry — the resolver tells a name from an address by the 0x prefix alone.
func TestPrimeNameMustBeAnAddressableSlug(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	for _, tc := range []struct{ name, primeName, want string }{
		{"uppercase", "Spark2", "prime_name_is_a_slug"},
		{"dot", "spark.v2", "prime_name_is_a_slug"},
		{"address shaped", "0x691a6c29e9e96dd897718305427ad5d534db16ba", "prime_name_is_not_an_address"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pool.Exec(ctx,
				`INSERT INTO prime (prime_key, name, vault_address)
				  VALUES ('prm_t_rejected', $1, decode('11223344556677889900aabbccddeeff00112233', 'hex'))`,
				tc.primeName)
			if err == nil {
				t.Fatalf("prime name %q should be rejected", tc.primeName)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want constraint %s", err, tc.want)
			}
		})
	}
}
