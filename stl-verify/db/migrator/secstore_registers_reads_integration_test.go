//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Mutation coverage for the registers' resolution order, which review found unpinned: 18 of 20
// mutations survived because no fixture ever created a version contest. Every test below puts two
// rows in ONE group — same (identity, valid_from) — so the ordering keys are what decide the
// answer, and a dropped or flipped key changes it.
//
//	ORDER BY <identity>, valid_from, processing_version DESC, ingest_xid DESC, record_id DESC
//
// One per key, plus the valid-time bound, run across every read that carries the order. The
// pattern for decoupling ingest_xid from record_id is Cyril's from #984.
//
// Note throughout: supersedes_record_id is required by the supersession guard when a second row
// lands on an occupied window, but the READS never consult it. Which row wins is decided purely
// by the keys above — that is itself worth pinning, and is why a fixture can look semantically
// odd (a superseded row winning) while being exactly correct.

// One fixture detail is load-bearing: the two rows of each pair differ in valid_to and nothing
// else. valid_to is the only primary-key component the reads do NOT group on, so it is what lets
// both rows exist and still land in one resolution group. Both values must contain the as-of date
// ('infinity' and '2099-01-01'), or the loser drops out on the window rather than on the ordering
// key under test, and the mutation survives again.

// openInstrument inserts an opening row and returns its record_id.
func openInstrument(ctx context.Context, t *testing.T, q queryRower, key, security, validFrom, validToSQL string) int64 {
	t.Helper()
	var id int64
	if err := q.QueryRow(ctx, `
		INSERT INTO instrument_register
			(instrument_key, key_namespace, security_id, chain_id, valid_from, valid_to, `+registerSpine+`)
		VALUES ($1, 'token_address', $2, 1, $3::date, `+validToSQL+`,
		        'test', 'SEED_LOAD', 'reads fixture', 'test')
		RETURNING record_id`, key, security, validFrom).Scan(&id); err != nil {
		t.Fatalf("open %s -> %s: %v", key, security, err)
	}
	return id
}

// queryRower is the intersection of *pgxpool.Pool and pgx.Tx that these fixtures need.
type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgxRow
}

type pgxRow interface{ Scan(dest ...any) error }

// poolRower adapts a pool to queryRower.
type poolRower struct{ p *pgxpool.Pool }

func (r poolRower) QueryRow(ctx context.Context, sql string, args ...any) pgxRow {
	return r.p.QueryRow(ctx, sql, args...)
}

// TestInstrumentRegisterProcessingVersionOutranksArrivalOrder pins processing_version as the FIRST
// ordering key. The correction is inserted BEFORE the live append on purpose: with
// processing_version DESC the correction wins, and without it the order falls to ingest_xid DESC,
// which would pick the later-arriving live row instead. Insert them the other way round and both
// orderings agree, which is why the register's own fixtures never caught a dropped key.
//
// It also pins the consequence wave 1 documents: once a window is corrected at N, an ordinary
// append at 0 on that window never wins again, silently.
func TestInstrumentRegisterProcessingVersionOutranksArrivalOrder(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const key = "aa01aa01aa01aa01aa01aa01aa01aa01aa01aa01"

	var corrected int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO instrument_register
			(instrument_key, key_namespace, security_id, chain_id, valid_from, valid_to,
			 processing_version, actor, change_reason_code, change_reason, approved_by, source_system)
		VALUES ($1, 'token_address', 'sec-corrected', 1, '2026-01-01', 'infinity',
		        1, 'curator', 'RESTATEMENT', 'the corrected mapping', 'reviewer', 'test')
		RETURNING record_id`, key).Scan(&corrected); err != nil {
		t.Fatalf("insert the correction: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO instrument_register
			(instrument_key, key_namespace, security_id, chain_id, valid_from, valid_to,
			 supersedes_record_id, `+registerSpine+`)
		VALUES ($1, 'token_address', 'sec-live', 1, '2026-01-01', '2099-01-01', $2,
		        'test', 'SEED_LOAD', 'an ordinary append after the correction', 'test')`,
		key, corrected); err != nil {
		t.Fatalf("insert the later live append: %v", err)
	}

	for _, read := range []struct {
		name string
		sql  string
	}{
		{"current", `SELECT security_id FROM instrument_register_current WHERE instrument_key = $1`},
		{"as_of", `SELECT security_id FROM instrument_register_as_of('2026-06-01'::date) WHERE instrument_key = $1`},
	} {
		t.Run(read.name, func(t *testing.T) {
			var got string
			if err := pool.QueryRow(ctx, read.sql, key).Scan(&got); err != nil {
				t.Fatalf("%s: %v", read.name, err)
			}
			if got != "sec-corrected" {
				t.Errorf("%s resolves %s, want sec-corrected — processing_version must outrank arrival order, and a curator's correction is not undone by the next pipeline run", read.name, got)
			}
		})
	}
}

// TestInstrumentRegisterIngestXidOutranksRecordId pins ingest_xid ahead of record_id. The two must
// disagree or the test proves nothing, so the transactions force their xids in the opposite order
// to their inserts: xids are assigned lazily at first write, and pg_current_xact_id() pulls that
// forward.
//
//	txEarly  xid assigned FIRST  (lower xid)  inserts SECOND (higher record_id)
//	txLate   xid assigned SECOND (higher xid) inserts FIRST  (lower record_id)
//
// ingest_xid DESC picks txLate's row; record_id DESC would pick txEarly's. txLate commits before
// txEarly inserts because the supersession guard takes an advisory lock per identity — without
// that ordering the second insert blocks until the first transaction ends.
func TestInstrumentRegisterIngestXidOutranksRecordId(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const key = "bb02bb02bb02bb02bb02bb02bb02bb02bb02bb02"

	txEarly, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer txEarly.Rollback(ctx)
	if _, err := txEarly.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
		t.Fatalf("force txEarly xid: %v", err)
	}

	txLate, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer txLate.Rollback(ctx)
	if _, err := txLate.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
		t.Fatalf("force txLate xid: %v", err)
	}

	var lateRow int64
	if err := txLate.QueryRow(ctx, `
		INSERT INTO instrument_register
			(instrument_key, key_namespace, security_id, chain_id, valid_from, valid_to, `+registerSpine+`)
		VALUES ($1, 'token_address', 'sec-higher-xid', 1, '2026-01-01', 'infinity',
		        'test', 'SEED_LOAD', 'higher xid, lower record_id', 'test')
		RETURNING record_id`, key).Scan(&lateRow); err != nil {
		t.Fatalf("txLate insert: %v", err)
	}
	if err := txLate.Commit(ctx); err != nil {
		t.Fatalf("txLate commit: %v", err)
	}

	if _, err := txEarly.Exec(ctx, `
		INSERT INTO instrument_register
			(instrument_key, key_namespace, security_id, chain_id, valid_from, valid_to,
			 supersedes_record_id, `+registerSpine+`)
		VALUES ($1, 'token_address', 'sec-lower-xid', 1, '2026-01-01', '2099-01-01', $2,
		        'test', 'SEED_LOAD', 'lower xid, higher record_id', 'test')`,
		key, lateRow); err != nil {
		t.Fatalf("txEarly insert: %v", err)
	}
	if err := txEarly.Commit(ctx); err != nil {
		t.Fatalf("txEarly commit: %v", err)
	}

	var got string
	if err := pool.QueryRow(ctx,
		`SELECT security_id FROM instrument_register_current WHERE instrument_key = $1`, key,
	).Scan(&got); err != nil {
		t.Fatalf("current: %v", err)
	}
	if got != "sec-higher-xid" {
		t.Errorf("current resolves %s, want sec-higher-xid — ingest_xid must outrank record_id, and the row that arrived second holds the LOWER xid here", got)
	}
}

// TestInstrumentRegisterRecordIdBreaksASameTransactionTie pins the last key. Two rows written in
// one transaction share identity, processing_version AND ingest_xid, so record_id is the only
// thing left to separate them; drop it and the winner is whatever the planner returns.
func TestInstrumentRegisterRecordIdBreaksASameTransactionTie(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const key = "cc03cc03cc03cc03cc03cc03cc03cc03cc03cc03"

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	var first int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO instrument_register
			(instrument_key, key_namespace, security_id, chain_id, valid_from, valid_to, `+registerSpine+`)
		VALUES ($1, 'token_address', 'sec-written-first', 1, '2026-01-01', 'infinity',
		        'test', 'SEED_LOAD', 'first in the transaction', 'test')
		RETURNING record_id`, key).Scan(&first); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO instrument_register
			(instrument_key, key_namespace, security_id, chain_id, valid_from, valid_to,
			 supersedes_record_id, `+registerSpine+`)
		VALUES ($1, 'token_address', 'sec-written-second', 1, '2026-01-01', '2099-01-01', $2,
		        'test', 'SEED_LOAD', 'second in the transaction', 'test')`, key, first); err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	var got string
	var sameXid bool
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT security_id FROM instrument_register_current WHERE instrument_key = $1),
		       (SELECT count(DISTINCT ingest_xid) = 1 FROM instrument_register WHERE instrument_key = $1)`,
		key,
	).Scan(&got, &sameXid); err != nil {
		t.Fatalf("current: %v", err)
	}
	if !sameXid {
		t.Fatal("the two rows do not share an ingest_xid, so record_id is not what decides — fixture is wrong")
	}
	if got != "sec-written-second" {
		t.Errorf("current resolves %s, want sec-written-second — record_id DESC is the last tiebreak", got)
	}
}

// TestRegisterReadsIncludeTheirLowerValidFromBound pins `valid_from <= effective_at`. Every read
// carries the bound, and tightening it to `<` silently drops a mapping on the exact day it starts
// — which is also the day a load most often reads it back.
func TestRegisterReadsIncludeTheirLowerValidFromBound(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const key = "dd04dd04dd04dd04dd04dd04dd04dd04dd04dd04"
	openInstrument(ctx, t, poolRower{pool}, key, "sec-boundary", "2026-05-01", "'infinity'")

	const value = "dd04dd04dd04dd04dd04dd04dd04dd04dd04dd05"
	if _, err := pool.Exec(ctx, `
		INSERT INTO alias_register (id_scheme, id_value, node_id, valid_from, valid_to, `+registerSpine+`)
		VALUES ('BLOCKCHAIN_ADDRESS', $1, 'em-boundary', '2026-05-01', 'infinity',
		        'test', 'SEED_LOAD', 'boundary fixture', 'test')`, value); err != nil {
		t.Fatalf("insert alias: %v", err)
	}

	for _, tc := range []struct {
		name string
		sql  string
		arg  string
	}{
		{"instrument as_of", `SELECT count(*) FROM instrument_register_as_of('2026-05-01'::date) WHERE instrument_key = $1`, key},
		{"alias as_of", `SELECT count(*) FROM alias_register_as_of('2026-05-01'::date) WHERE id_value = $1`, value},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var n int
			if err := pool.QueryRow(ctx, tc.sql, tc.arg).Scan(&n); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if n != 1 {
				t.Errorf("%s at exactly valid_from returned %d rows, want 1 — the window is half-open [valid_from, valid_to)", tc.name, n)
			}
		})
	}
}

// TestAliasRegisterOrderingKeysDecideTheSameWay is the alias half: the two registers share the
// resolution shape, so a mutation to one order is a mutation to both. Covers processing_version
// and record_id there; the ingest_xid mechanism is pinned once, above, since the ORDER BY clauses
// are identical in form.
func TestAliasRegisterOrderingKeysDecideTheSameWay(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	t.Run("processing_version outranks arrival order", func(t *testing.T) {
		const value = "ee05ee05ee05ee05ee05ee05ee05ee05ee05ee05"
		var corrected int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO alias_register (id_scheme, id_value, node_id, valid_from, valid_to,
			                            processing_version, actor, change_reason_code, change_reason,
			                            approved_by, source_system)
			VALUES ('BLOCKCHAIN_ADDRESS', $1, 'em-corrected', '2026-01-01', 'infinity',
			        1, 'curator', 'RESTATEMENT', 'corrected holder', 'reviewer', 'test')
			RETURNING record_id`, value).Scan(&corrected); err != nil {
			t.Fatalf("insert correction: %v", err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO alias_register (id_scheme, id_value, node_id, valid_from, valid_to,
			                            supersedes_record_id, `+registerSpine+`)
			VALUES ('BLOCKCHAIN_ADDRESS', $1, 'em-live', '2026-01-01', '2099-01-01', $2,
			        'test', 'SEED_LOAD', 'ordinary append after', 'test')`, value, corrected); err != nil {
			t.Fatalf("insert live append: %v", err)
		}

		var got string
		if err := pool.QueryRow(ctx,
			`SELECT node_id FROM alias_register_current WHERE id_value = $1`, value,
		).Scan(&got); err != nil {
			t.Fatalf("current: %v", err)
		}
		if got != "em-corrected" {
			t.Errorf("current resolves %s, want em-corrected", got)
		}
	})

	t.Run("record_id breaks a same-transaction tie", func(t *testing.T) {
		const value = "ff06ff06ff06ff06ff06ff06ff06ff06ff06ff06"
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)

		var first int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO alias_register (id_scheme, id_value, node_id, valid_from, valid_to, `+registerSpine+`)
			VALUES ('BLOCKCHAIN_ADDRESS', $1, 'em-written-first', '2026-01-01', 'infinity',
			        'test', 'SEED_LOAD', 'first', 'test')
			RETURNING record_id`, value).Scan(&first); err != nil {
			t.Fatalf("first insert: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO alias_register (id_scheme, id_value, node_id, valid_from, valid_to,
			                            supersedes_record_id, `+registerSpine+`)
			VALUES ('BLOCKCHAIN_ADDRESS', $1, 'em-written-second', '2026-01-01', '2099-01-01', $2,
			        'test', 'SEED_LOAD', 'second', 'test')`, value, first); err != nil {
			t.Fatalf("second insert: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var got string
		if err := pool.QueryRow(ctx,
			`SELECT node_id FROM alias_register_current WHERE id_value = $1`, value,
		).Scan(&got); err != nil {
			t.Fatalf("current: %v", err)
		}
		if got != "em-written-second" {
			t.Errorf("current resolves %s, want em-written-second", got)
		}
	})
}
