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
// another entity's figures. A prime is added here once, with the id and external_id its migration
// mints, and the row never changes afterwards (ADR-0005 decision 1).
var primeIdentityLedger = map[string]struct {
	id         int64
	externalID string
}{
	"spark": {id: 1, externalID: "4bd9ee3c-58df-4587-9c04-63b928f1a169"},
	"grove": {id: 2, externalID: "9f5e309d-17a7-4fd4-b49b-009b5071afcd"},
	"obex":  {id: 3, externalID: "d0906a47-9b0e-481a-b427-29514e0c2153"},
}

// TestPrimeNamesNeverRemap fails when a prime name maps to a different prime.id or external_id
// than it did before, and when a prime reaches the database without a ledger entry. It runs
// against a freshly migrated database, so the ids it pins are the fresh-DB assignment order —
// it certifies the migration set, not a long-lived environment.
func TestPrimeNamesNeverRemap(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	rows, err := pool.Query(ctx, `SELECT name, id, external_id FROM prime ORDER BY id`)
	if err != nil {
		t.Fatalf("read prime registry: %v", err)
	}
	defer rows.Close()

	seen := make(map[string]bool, len(primeIdentityLedger))
	for rows.Next() {
		var name, externalID string
		var id int64
		if err := rows.Scan(&name, &id, &externalID); err != nil {
			t.Fatalf("scan prime row: %v", err)
		}
		seen[name] = true

		want, ok := primeIdentityLedger[name]
		if !ok {
			t.Errorf("prime %q is in the database but not in the ledger; add it with the id and external_id its migration mints", name)
			continue
		}
		if id != want.id || externalID != want.externalID {
			t.Errorf("prime %q maps to (id %d, external_id %q), ledger says (id %d, external_id %q); a name never remaps",
				name, id, externalID, want.id, want.externalID)
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

	_, err := pool.Exec(ctx, `UPDATE prime SET external_id = '00000000-0000-0000-0000-000000000000' WHERE name = 'spark'`)
	if err == nil {
		t.Fatal("changing a prime's external_id should raise via the prime_external_id_immutable trigger")
	}
	if !strings.Contains(err.Error(), "minted once and never changes") {
		t.Errorf("UPDATE error = %v, want the external_id immutability message", err)
	}

	// The trigger is column-scoped: a rename is still allowed, and re-stating the same external_id
	// alongside it is not a change.
	if _, err := pool.Exec(ctx,
		`UPDATE prime SET name = 'spark2', external_id = external_id WHERE name = 'spark'`); err != nil {
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
				`INSERT INTO prime (external_id, name, vault_address, chain_id)
				  VALUES (gen_random_uuid(), $1, decode('11223344556677889900aabbccddeeff00112233', 'hex'), 1)`,
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
