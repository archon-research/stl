//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// Native instrument keys (the VEC-412 bridge form): the instrument's own globally-unique id, never a
// house-classifier prefix. Morpho = the protocol-emitted market id (bytes32, lowercase hex); Sky =
// ilk-registry address ':' ilk. Addresses/hex are lowercase, no 0x prefix.
const (
	holder       = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	morphoMarket = "b0c9e5b4a3f2e1d0c9b8a7968574635241302010ffeeddccbbaa998877665544"
	skyIlk       = "3c9be1f8b7c2d4e5a6b7c8d9e0f1a2b3c4d5e6f7:ALLOCATOR-SPARK-A"
)

// TestPositionIdHelper is the VEC-400 contract test: after migrations, public.position_key
// produces the canonical identity string, public.position_id is its 32-byte sha256, and the
// required-field guards fail hard. Identity is holder + instrument only (no classifications).
// Every per-protocol materializer depends on this helper, so the contract is pinned here.
func TestPositionIdHelper(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// Chain + protocol present -> the canonical string per the spec.
	var key string
	if err := pool.QueryRow(ctx,
		`SELECT position_key(1, 3, $1, $2)`, morphoMarket, holder).Scan(&key); err != nil {
		t.Fatalf("position_key: %v", err)
	}
	if want := "1;3;" + morphoMarket + ";" + holder; key != want {
		t.Errorf("position_key = %q, want %q", key, want)
	}

	// position_id is the 32-byte sha256 of exactly that string.
	var idIsSha256 bool
	var idLen int
	if err := pool.QueryRow(ctx, `
		SELECT position_id(1,3,$1,$2) = sha256(convert_to('1;3;' || $1 || ';' || $2,'UTF8')),
		       length(position_id(1,3,$1,$2))`, morphoMarket, holder).Scan(&idIsSha256, &idLen); err != nil {
		t.Fatalf("position_id: %v", err)
	}
	if !idIsSha256 {
		t.Error("position_id is not the sha256 of position_key")
	}
	if idLen != 32 {
		t.Errorf("position_id length = %d bytes, want 32", idLen)
	}

	// Nullable structural fields render empty between the delimiters, positions stay stable.
	for _, c := range []struct{ name, sql, want string }{
		{"null protocol", `SELECT position_key(1, NULL, $1, $2)`, "1;;" + skyIlk + ";" + holder},
		{"null chain", `SELECT position_key(NULL, 3, $1, $2)`, ";3;" + skyIlk + ";" + holder},
		{"both null", `SELECT position_key(NULL, NULL, $1, $2)`, ";;" + skyIlk + ";" + holder},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			var got string
			if err := pool.QueryRow(ctx, c.sql, skyIlk, holder).Scan(&got); err != nil {
				t.Fatalf("position_key: %v", err)
			}
			if got != c.want {
				t.Errorf("position_key = %q, want %q", got, c.want)
			}
		})
	}

	// Guards must fail hard rather than emit a silently-wrong identity: NULL, empty, whitespace-only,
	// and a ';' delimiter in either identity field (the last would collide two distinct positions).
	// Assert the specific message so a mislabeled guard or an unrelated SQL error can't pass silently.
	for _, g := range []struct{ name, sql, wantErr string }{
		{"instrument_key null", `SELECT position_key(1,3,NULL,'` + holder + `')`, "instrument_key is required"},
		{"instrument_key empty", `SELECT position_key(1,3,'','` + holder + `')`, "instrument_key is required"},
		{"instrument_key blank", `SELECT position_key(1,3,'   ','` + holder + `')`, "instrument_key is required"},
		{"instrument_key has delimiter", `SELECT position_key(1,3,'a;b','` + holder + `')`, "must not contain"},
		{"holder_id null", `SELECT position_key(1,3,'` + morphoMarket + `',NULL)`, "holder_id is required"},
		{"holder_id empty", `SELECT position_key(1,3,'` + morphoMarket + `','')`, "holder_id is required"},
		{"holder_id blank", `SELECT position_key(1,3,'` + morphoMarket + `','   ')`, "holder_id is required"},
		{"holder_id has delimiter", `SELECT position_key(1,3,'` + morphoMarket + `','a;b')`, "must not contain"},
	} {
		g := g
		t.Run(g.name, func(t *testing.T) {
			var s string
			err := pool.QueryRow(ctx, g.sql).Scan(&s)
			if err == nil {
				t.Fatalf("expected the helper to raise, got none (%q)", s)
			}
			if !strings.Contains(err.Error(), g.wantErr) {
				t.Errorf("error = %v, want it to contain %q", err, g.wantErr)
			}
		})
	}

	// The functions are IMMUTABLE specifically so downstream tables can build a STORED generated
	// column and an expression index over position_id. A plain SELECT passes under any volatility
	// class, so exercise the real DDL contract: if a future edit drops IMMUTABLE (or the body picks up
	// a non-immutable construct) this fails at DDL time, where the plain-SELECT assertions would not.
	t.Run("immutable backs generated column and expression index", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `
			CREATE TABLE pin_immutable (
				chain_id int, protocol_id bigint, instrument_key text, holder_id text,
				pid bytea GENERATED ALWAYS AS (position_id(chain_id, protocol_id, instrument_key, holder_id)) STORED
			)`); err != nil {
			t.Fatalf("STORED generated column over position_id rejected (IMMUTABLE contract broken?): %v", err)
		}
		if _, err := pool.Exec(ctx,
			`CREATE UNIQUE INDEX ON pin_immutable ((position_id(chain_id, protocol_id, instrument_key, holder_id)))`); err != nil {
			t.Fatalf("expression index over position_id rejected: %v", err)
		}
		// A valid row stores a 32-byte generated pid.
		var pidLen int
		if err := pool.QueryRow(ctx, `
			INSERT INTO pin_immutable (chain_id, protocol_id, instrument_key, holder_id)
			VALUES (1, 3, $1, $2) RETURNING length(pid)`, morphoMarket, holder).Scan(&pidLen); err != nil {
			t.Fatalf("insert into generated-column table: %v", err)
		}
		if pidLen != 32 {
			t.Errorf("generated pid length = %d, want 32", pidLen)
		}
		// A NULL identity field fires the guard inside the generated expression and fails the whole
		// INSERT rather than storing a silent NULL pid.
		if _, err := pool.Exec(ctx,
			`INSERT INTO pin_immutable (chain_id, protocol_id, instrument_key, holder_id) VALUES (1, 3, NULL, $1)`, holder); err == nil {
			t.Error("NULL instrument_key was accepted into the generated-column table; expected the INSERT to fail")
		}
	})
}
