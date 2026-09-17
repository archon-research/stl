//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// correctionVersion returns the next processing_version for a table. Today: max(pv)+1.
// When processing_version_log lands (VEC-632), this becomes an allocator call.
func correctionVersion(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var pv int
	if err := pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT coalesce(max(processing_version), -1) + 1 FROM %s", table)).Scan(&pv); err != nil {
		t.Fatalf("correctionVersion(%s): %v", table, err)
	}
	return pv
}

func TestSecStoreResolvedReadsHonourSupersessionWindowsAndTiebreaks(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	// -----------------------------------------------------------------------
	// Shared fixture: builds a complete scenario for one store.
	//
	// Timeline for id "em-t-reads" (node) / edge with equivalent structure:
	//   pv 0: Jan-open   (2026-01-01..infinity)   <- the first append
	//   pv 0: Jan-closed (2026-01-01..2026-06-01) <- close: same (id, valid_from, pv), diff valid_to
	//   pv 0: Jun-open   (2026-06-01..infinity)   <- open the next window
	//   pv N: Jun-restatement (2026-06-01..infinity) <- correction; wins over Jun-open
	//   pv 0: tombstone  (2026-03-15..2026-03-15) <- retract a third record
	//
	// The third record (tombstoned) is a separate id "em-t-reads-tomb".
	// -----------------------------------------------------------------------

	const nodeID = "em-t-reads"
	const tombID = "em-t-reads-tomb"

	insertNode(ctx, t, pool, nodeID, "ACTIVE", "2026-01-01", "'infinity'", "open the first window")
	insertNode(ctx, t, pool, nodeID, "ACTIVE", "2026-01-01", "'2026-06-01'", "close it")
	insertNode(ctx, t, pool, nodeID, "INACTIVE", "2026-06-01", "'infinity'", "open the next window")

	pvN := correctionVersion(ctx, t, pool, "sec_node")
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, processing_version, `+secstoreSpine+`)
		VALUES ('em-t-reads', 'ENTITY', 'INACTIVE', '2026-06-01', 'infinity', %d, 'test', 'RESTATEMENT', 'pv-N restatement of Jun window', 'test')`, pvN))
	if err != nil {
		t.Fatalf("insert pv-N restatement: %v", err)
	}

	// Tombstoned record
	insertNode(ctx, t, pool, tombID, "ACTIVE", "2026-03-15", "'infinity'", "a record to retract")
	_, err = pool.Exec(ctx, `
		INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
		VALUES ('em-t-reads-tomb', 'ENTITY', 'ACTIVE', '2026-03-15', '2026-03-15', 'test', 'RETRACTION', 'tombstone', 'test')`)
	if err != nil {
		t.Fatalf("insert tombstone: %v", err)
	}

	// Edge fixture
	_, err = pool.Exec(ctx, `
		INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
		VALUES ('sec-t-reads-src', 'SECURITY', 'em-t-reads-edst', 'ENTITY', 'ISSUED_BY', '2026-01-01', 'test', 'SEED_LOAD', 'edge first window', 'test')`)
	if err != nil {
		t.Fatalf("insert edge first window: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
		VALUES ('sec-t-reads-src', 'SECURITY', 'em-t-reads-edst', 'ENTITY', 'ISSUED_BY', '2026-01-01', '2026-06-01', 'test', 'VALID_TIME_AMEND', 'edge close', 'test')`)
	if err != nil {
		t.Fatalf("insert edge close: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
		VALUES ('sec-t-reads-src', 'SECURITY', 'em-t-reads-edst', 'ENTITY', 'ISSUED_BY', '2026-06-01', 'test', 'SEED_LOAD', 'edge second window', 'test')`)
	if err != nil {
		t.Fatalf("insert edge second window: %v", err)
	}

	edgePvN := correctionVersion(ctx, t, pool, "sec_edge")
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, processing_version, `+secstoreSpine+`)
		VALUES ('sec-t-reads-src', 'SECURITY', 'em-t-reads-edst', 'ENTITY', 'ISSUED_BY', '2026-06-01', %d, 'test', 'RESTATEMENT', 'edge pv-N restatement', 'test')`, edgePvN))
	if err != nil {
		t.Fatalf("insert edge pv-N restatement: %v", err)
	}

	// Overlapping-window node: two open windows valid at as_of('2026-04-01')
	const overlapNodeID = "em-t-reads-overlap"
	insertNode(ctx, t, pool, overlapNodeID, "ACTIVE", "2026-01-01", "'infinity'", "overlap window A")
	insertNode(ctx, t, pool, overlapNodeID, "ACTIVE", "2026-03-01", "'infinity'", "overlap window B")

	// Overlapping-window edge: same pattern
	_, err = pool.Exec(ctx, `
		INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
		VALUES ('sec-t-reads-overlap-src', 'SECURITY', 'em-t-reads-overlap-dst', 'ENTITY', 'ISSUED_BY', '2026-01-01', 'test', 'SEED_LOAD', 'overlap edge A', 'test')`)
	if err != nil {
		t.Fatalf("insert overlap edge A: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
		VALUES ('sec-t-reads-overlap-src', 'SECURITY', 'em-t-reads-overlap-dst', 'ENTITY', 'ISSUED_BY', '2026-03-01', 'test', 'SEED_LOAD', 'overlap edge B', 'test')`)
	if err != nil {
		t.Fatalf("insert overlap edge B: %v", err)
	}

	// -----------------------------------------------------------------------
	// WI-2: reads with supersession and window coverage
	// -----------------------------------------------------------------------

	t.Run("node_as_of_inside_closed_window_returns_closed_row", func(t *testing.T) {
		var validTo string
		if err := pool.QueryRow(ctx, `
			SELECT valid_to::text FROM sec_node_as_of('2026-03-01'::date) WHERE id = $1`, nodeID).Scan(&validTo); err != nil {
			t.Fatalf("sec_node_as_of(2026-03-01) for %s: %v", nodeID, err)
		}
		if validTo != "2026-06-01" {
			t.Fatalf("got valid_to=%s, want 2026-06-01 (the closed window, not the superseded open one)", validTo)
		}
	})

	t.Run("node_as_of_window_first_disagrees", func(t *testing.T) {
		// Filtering windows first, then resolving, would return the open row for Mar
		// because both the open and closed rows match [Jan..infinity) and [Jan..Jun).
		// The correct two-step returns only the closed row.
		var count int
		if err := pool.QueryRow(ctx, `
			WITH window_first AS (
				SELECT * FROM sec_node
				WHERE valid_from <= '2026-03-01' AND '2026-03-01' < valid_to
			)
			SELECT count(DISTINCT valid_to) FROM window_first WHERE id = $1`, nodeID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count <= 1 {
			t.Fatal("window-first and two-step agree on this fixture — both rows must be valid-time candidates for the distinction to matter")
		}
	})

	t.Run("node_current_returns_restatement_not_original", func(t *testing.T) {
		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_node_current WHERE id = $1`, nodeID).Scan(&pv); err != nil {
			t.Fatalf("sec_node_current for %s: %v", nodeID, err)
		}
		if pv != pvN {
			t.Fatalf("got processing_version=%d, want %d (the restatement wins)", pv, pvN)
		}
	})

	t.Run("node_as_of_returns_restatement", func(t *testing.T) {
		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_node_as_of('2026-07-01'::date) WHERE id = $1`, nodeID).Scan(&pv); err != nil {
			t.Fatalf("sec_node_as_of(2026-07-01) for %s: %v", nodeID, err)
		}
		if pv != pvN {
			t.Fatalf("got processing_version=%d, want %d", pv, pvN)
		}
	})

	t.Run("node_as_of_kind_returns_restatement", func(t *testing.T) {
		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_node_as_of_kind('2026-07-01'::date, 'ENTITY') WHERE id = $1`, nodeID).Scan(&pv); err != nil {
			t.Fatalf("sec_node_as_of_kind(2026-07-01, ENTITY) for %s: %v", nodeID, err)
		}
		if pv != pvN {
			t.Fatalf("got processing_version=%d, want %d", pv, pvN)
		}
	})

	t.Run("edge_current_returns_restatement", func(t *testing.T) {
		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_edge_current
			WHERE src_id = 'sec-t-reads-src' AND rel_type = 'ISSUED_BY'`).Scan(&pv); err != nil {
			t.Fatalf("sec_edge_current: %v", err)
		}
		if pv != edgePvN {
			t.Fatalf("got processing_version=%d, want %d", pv, edgePvN)
		}
	})

	t.Run("edge_as_of_returns_restatement", func(t *testing.T) {
		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_edge_as_of('2026-07-01'::date)
			WHERE src_id = 'sec-t-reads-src' AND rel_type = 'ISSUED_BY'`).Scan(&pv); err != nil {
			t.Fatalf("sec_edge_as_of(2026-07-01): %v", err)
		}
		if pv != edgePvN {
			t.Fatalf("got processing_version=%d, want %d", pv, edgePvN)
		}
	})

	t.Run("tombstoned_window_absent_from_all_node_reads", func(t *testing.T) {
		for _, q := range []struct {
			name, sql string
		}{
			{"current", fmt.Sprintf("SELECT count(*) FROM sec_node_current WHERE id = '%s'", tombID)},
			{"as_of_inside", fmt.Sprintf("SELECT count(*) FROM sec_node_as_of('2026-03-15'::date) WHERE id = '%s'", tombID)},
			{"as_of_outside", fmt.Sprintf("SELECT count(*) FROM sec_node_as_of('2026-04-01'::date) WHERE id = '%s'", tombID)},
		} {
			t.Run(q.name, func(t *testing.T) {
				var count int
				if err := pool.QueryRow(ctx, q.sql).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 0 {
					t.Fatalf("tombstoned record visible in %s: got %d rows", q.name, count)
				}
			})
		}
	})

	t.Run("one_row_per_logical_record_per_read", func(t *testing.T) {
		for _, q := range []struct {
			name, sql string
		}{
			{"current", fmt.Sprintf("SELECT count(*) FROM sec_node_current WHERE id = '%s'", nodeID)},
			{"as_of_mar", fmt.Sprintf("SELECT count(*) FROM sec_node_as_of('2026-03-01'::date) WHERE id = '%s'", nodeID)},
			{"as_of_jul", fmt.Sprintf("SELECT count(*) FROM sec_node_as_of('2026-07-01'::date) WHERE id = '%s'", nodeID)},
		} {
			t.Run(q.name, func(t *testing.T) {
				var count int
				if err := pool.QueryRow(ctx, q.sql).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("expected 1 row for %s, got %d", q.name, count)
				}
			})
		}
	})

	t.Run("edge_as_of_inside_closed_window_returns_closed_row", func(t *testing.T) {
		var validTo string
		if err := pool.QueryRow(ctx, `
			SELECT valid_to::text FROM sec_edge_as_of('2026-03-01'::date)
			WHERE src_id = 'sec-t-reads-src' AND rel_type = 'ISSUED_BY'`).Scan(&validTo); err != nil {
			t.Fatalf("sec_edge_as_of(2026-03-01): %v", err)
		}
		if validTo != "2026-06-01" {
			t.Fatalf("got valid_to=%s, want 2026-06-01", validTo)
		}
	})

	t.Run("edge_as_of_window_first_disagrees", func(t *testing.T) {
		// Mirror of node_as_of_window_first_disagrees for sec_edge_as_of.
		// Both the open and closed edge rows are valid-time candidates for Mar;
		// window-first returns both, two-step picks only the closed one.
		var count int
		if err := pool.QueryRow(ctx, `
			WITH window_first AS (
				SELECT * FROM sec_edge
				WHERE valid_from <= '2026-03-01' AND '2026-03-01' < valid_to
			)
			SELECT count(DISTINCT valid_to) FROM window_first
			WHERE src_id = 'sec-t-reads-src' AND rel_type = 'ISSUED_BY'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count <= 1 {
			t.Fatal("edge window-first and two-step agree on this fixture — both rows must be valid-time candidates for the distinction to matter")
		}
	})

	t.Run("edge_current_returns_second_window", func(t *testing.T) {
		var validFrom string
		if err := pool.QueryRow(ctx, `
			SELECT valid_from::text FROM sec_edge_current
			WHERE src_id = 'sec-t-reads-src' AND rel_type = 'ISSUED_BY'`).Scan(&validFrom); err != nil {
			t.Fatalf("sec_edge_current: %v", err)
		}
		if validFrom != "2026-06-01" {
			t.Fatalf("got valid_from=%s, want 2026-06-01", validFrom)
		}
	})

	// -----------------------------------------------------------------------
	// WI-3: _current never reads processing_version > 0
	// -----------------------------------------------------------------------

	// Late pv-0 appends: these inserts live in the parent body so every pv-0
	// subtest below has its fixture regardless of -run filtering.
	_, err = pool.Exec(ctx, `
		INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, processing_version, `+secstoreSpine+`)
		VALUES ('em-t-reads', 'ENTITY', 'ACTIVE', '2026-06-01', '9999-12-31', 0, 'test', 'SEED_LOAD', 'late pv-0 append', 'test')`)
	if err != nil {
		t.Fatalf("late pv-0 append: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, processing_version, `+secstoreSpine+`)
		VALUES ('sec-t-reads-src', 'SECURITY', 'em-t-reads-edst', 'ENTITY', 'ISSUED_BY', '2026-06-01', '9999-12-31', 0, 'test', 'SEED_LOAD', 'late pv-0 edge', 'test')`)
	if err != nil {
		t.Fatalf("late pv-0 edge append: %v", err)
	}

	t.Run("pv0_append_after_correction_does_not_win", func(t *testing.T) {
		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_node_current WHERE id = $1`, nodeID).Scan(&pv); err != nil {
			t.Fatalf("sec_node_current after late pv-0: %v", err)
		}
		if pv != pvN {
			t.Fatalf("got pv=%d, want %d — a pv-0 append must not beat a correction", pv, pvN)
		}
	})

	t.Run("node_as_of_pv0_does_not_beat_correction", func(t *testing.T) {
		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_node_as_of('2026-07-01'::date) WHERE id = $1`, nodeID).Scan(&pv); err != nil {
			t.Fatalf("sec_node_as_of after late pv-0: %v", err)
		}
		if pv != pvN {
			t.Fatalf("got pv=%d, want %d", pv, pvN)
		}
	})

	t.Run("node_as_of_kind_pv0_does_not_beat_correction", func(t *testing.T) {
		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_node_as_of_kind('2026-07-01'::date, 'ENTITY') WHERE id = $1`, nodeID).Scan(&pv); err != nil {
			t.Fatalf("sec_node_as_of_kind after late pv-0: %v", err)
		}
		if pv != pvN {
			t.Fatalf("got pv=%d, want %d", pv, pvN)
		}
	})

	t.Run("edge_current_pv0_does_not_beat_correction", func(t *testing.T) {
		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_edge_current
			WHERE src_id = 'sec-t-reads-src' AND rel_type = 'ISSUED_BY'`).Scan(&pv); err != nil {
			t.Fatalf("sec_edge_current after late pv-0: %v", err)
		}
		if pv != edgePvN {
			t.Fatalf("got pv=%d, want %d — a pv-0 append must not beat a correction", pv, edgePvN)
		}
	})

	t.Run("edge_as_of_pv0_does_not_beat_correction", func(t *testing.T) {
		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_edge_as_of('2026-07-01'::date)
			WHERE src_id = 'sec-t-reads-src' AND rel_type = 'ISSUED_BY'`).Scan(&pv); err != nil {
			t.Fatalf("sec_edge_as_of after late pv-0: %v", err)
		}
		if pv != edgePvN {
			t.Fatalf("got pv=%d, want %d — a pv-0 append must not beat a correction", pv, edgePvN)
		}
	})

	// -----------------------------------------------------------------------
	// WI-6: tiebreak and boundaries
	// -----------------------------------------------------------------------

	t.Run("close_and_open_in_one_transaction", func(t *testing.T) {
		const txnID = "em-t-reads-txn"
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'ACTIVE', '2026-01-01', 'infinity', 'test', 'SEED_LOAD', 'open', 'test')`, txnID)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'ACTIVE', '2026-01-01', '2026-06-01', 'test', 'VALID_TIME_AMEND', 'close', 'test')`, txnID)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'INACTIVE', '2026-06-01', 'test', 'SEED_LOAD', 'reopen', 'test')`, txnID)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		// All three share (id, valid_from, pv=0) and ingest_xid — only record_id separates them.
		var validTo string
		if err := pool.QueryRow(ctx, `
			SELECT valid_to::text FROM sec_node_as_of('2026-03-01'::date) WHERE id = $1`, txnID).Scan(&validTo); err != nil {
			t.Fatalf("as_of(Mar) for txn: %v", err)
		}
		if validTo != "2026-06-01" {
			t.Fatalf("got valid_to=%s, want 2026-06-01 — the closing row must win by record_id", validTo)
		}
	})

	t.Run("ingest_xid_tiebreak_beats_record_id", func(t *testing.T) {
		// Two rows with the same (id, valid_from, pv=0) in separate transactions.
		// xids are assigned lazily at first write, not at BEGIN, so we force
		// assignment with pg_current_xact_id() to decouple xid order from
		// record_id (sequence) order.
		// txEarly: forced xid first (lower xid), inserts second (higher record_id).
		// txLate:  forced xid second (higher xid), inserts first (lower record_id).
		// Resolution uses ingest_xid DESC before record_id DESC, so txLate's row must win.
		const xidID = "em-t-reads-xid"

		txEarly, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txEarly.Rollback(ctx)

		// Force xid assignment on txEarly first — it gets the lower xid.
		if _, err := txEarly.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
			t.Fatal(err)
		}

		txLate, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txLate.Rollback(ctx)

		// Force xid assignment on txLate second — it gets the higher xid.
		if _, err := txLate.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
			t.Fatal(err)
		}

		// txLate inserts first — gets a lower record_id but higher ingest_xid.
		_, err = txLate.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'INACTIVE', '2026-01-01', 'infinity', 'test', 'SEED_LOAD', 'late-xid row', 'test')`, xidID)
		if err != nil {
			t.Fatal(err)
		}

		// txEarly inserts second — gets a higher record_id but lower ingest_xid.
		_, err = txEarly.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'ACTIVE', '2026-01-01', '9999-12-31', 'test', 'SEED_LOAD', 'early-xid row', 'test')`, xidID)
		if err != nil {
			t.Fatal(err)
		}

		if err := txLate.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := txEarly.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var status string
		if err := pool.QueryRow(ctx, `
			SELECT status FROM sec_node_current WHERE id = $1`, xidID).Scan(&status); err != nil {
			t.Fatalf("sec_node_current for %s: %v", xidID, err)
		}
		if status != "INACTIVE" {
			t.Fatalf("got status=%s, want INACTIVE — the higher ingest_xid (txLate) must win over higher record_id (txEarly)", status)
		}
	})

	t.Run("as_of_exactly_at_valid_from_returns_the_row", func(t *testing.T) {
		var count int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM sec_node_as_of('2026-01-01'::date) WHERE id = $1`, nodeID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("as_of exactly at valid_from: got %d, want 1", count)
		}
	})

	t.Run("as_of_exactly_at_valid_to_does_not_return_the_row", func(t *testing.T) {
		// The window is [Jan..Jun), half-open: effective_at < valid_to, so Jun is excluded.
		const boundID = "em-t-reads-bound"
		insertNode(ctx, t, pool, boundID, "ACTIVE", "2026-01-01", "'2026-06-01'", "single closed window")

		var count int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM sec_node_as_of('2026-06-01'::date) WHERE id = $1`, boundID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("as_of exactly at valid_to of a closed window: got %d, want 0 (half-open interval)", count)
		}
	})

	t.Run("edge_close_and_open_in_one_transaction", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ('sec-t-reads-txn', 'SECURITY', 'em-t-reads-txn-dst', 'ENTITY', 'ISSUED_BY', '2026-01-01', 'test', 'SEED_LOAD', 'edge open', 'test')`)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ('sec-t-reads-txn', 'SECURITY', 'em-t-reads-txn-dst', 'ENTITY', 'ISSUED_BY', '2026-01-01', '2026-06-01', 'test', 'VALID_TIME_AMEND', 'edge close', 'test')`)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ('sec-t-reads-txn', 'SECURITY', 'em-t-reads-txn-dst', 'ENTITY', 'ISSUED_BY', '2026-06-01', 'test', 'SEED_LOAD', 'edge reopen', 'test')`)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var validTo string
		if err := pool.QueryRow(ctx, `
			SELECT valid_to::text FROM sec_edge_as_of('2026-03-01'::date)
			WHERE src_id = 'sec-t-reads-txn' AND rel_type = 'ISSUED_BY'`).Scan(&validTo); err != nil {
			t.Fatalf("edge as_of(Mar) for txn: %v", err)
		}
		if validTo != "2026-06-01" {
			t.Fatalf("got valid_to=%s, want 2026-06-01 — the closing row must win by record_id", validTo)
		}
	})

	t.Run("edge_as_of_exactly_at_valid_from", func(t *testing.T) {
		var count int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM sec_edge_as_of('2026-01-01'::date)
			WHERE src_id = 'sec-t-reads-src' AND rel_type = 'ISSUED_BY'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("edge as_of exactly at valid_from: got %d, want 1", count)
		}
	})

	t.Run("edge_as_of_exactly_at_valid_to", func(t *testing.T) {
		const edgeBoundSrc = "sec-t-reads-ebound"
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-reads-ebound-dst', 'ENTITY', 'ISSUED_BY', '2026-01-01', '2026-06-01', 'test', 'SEED_LOAD', 'single closed edge', 'test')`, edgeBoundSrc)
		if err != nil {
			t.Fatal(err)
		}

		var count int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM sec_edge_as_of('2026-06-01'::date)
			WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`, edgeBoundSrc).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("edge as_of exactly at valid_to: got %d, want 0 (half-open interval)", count)
		}
	})

	t.Run("node_as_of_kind_exactly_at_valid_from", func(t *testing.T) {
		var count int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM sec_node_as_of_kind('2026-01-01'::date, 'ENTITY') WHERE id = $1`, nodeID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("as_of_kind exactly at valid_from: got %d, want 1", count)
		}
	})

	t.Run("node_as_of_kind_exactly_at_valid_to", func(t *testing.T) {
		const kindBoundID = "em-t-reads-kindbound"
		insertNode(ctx, t, pool, kindBoundID, "ACTIVE", "2026-01-01", "'2026-06-01'", "single closed window for kind boundary")

		var count int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM sec_node_as_of_kind('2026-06-01'::date, 'ENTITY') WHERE id = $1`, kindBoundID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("as_of_kind exactly at valid_to: got %d, want 0 (half-open interval)", count)
		}
	})

	t.Run("overlapping_windows_as_of_picks_latest_valid_from", func(t *testing.T) {
		var validFrom string
		if err := pool.QueryRow(ctx, `
			SELECT valid_from::text FROM sec_node_as_of('2026-04-01'::date) WHERE id = $1`, overlapNodeID).Scan(&validFrom); err != nil {
			t.Fatalf("sec_node_as_of(2026-04-01) for %s: %v", overlapNodeID, err)
		}
		if validFrom != "2026-03-01" {
			t.Fatalf("got valid_from=%s, want 2026-03-01 (the later window)", validFrom)
		}
	})

	t.Run("overlapping_windows_as_of_kind_picks_latest_valid_from", func(t *testing.T) {
		var validFrom string
		if err := pool.QueryRow(ctx, `
			SELECT valid_from::text FROM sec_node_as_of_kind('2026-04-01'::date, 'ENTITY') WHERE id = $1`, overlapNodeID).Scan(&validFrom); err != nil {
			t.Fatalf("sec_node_as_of_kind(2026-04-01, ENTITY) for %s: %v", overlapNodeID, err)
		}
		if validFrom != "2026-03-01" {
			t.Fatalf("got valid_from=%s, want 2026-03-01 (the later window)", validFrom)
		}
	})

	t.Run("overlapping_windows_edge_as_of_picks_latest_valid_from", func(t *testing.T) {
		var validFrom string
		if err := pool.QueryRow(ctx, `
			SELECT valid_from::text FROM sec_edge_as_of('2026-04-01'::date)
			WHERE src_id = 'sec-t-reads-overlap-src' AND rel_type = 'ISSUED_BY'`).Scan(&validFrom); err != nil {
			t.Fatalf("sec_edge_as_of(2026-04-01) for overlap edge: %v", err)
		}
		if validFrom != "2026-03-01" {
			t.Fatalf("got valid_from=%s, want 2026-03-01 (the later window)", validFrom)
		}
	})

	t.Run("retry_dedup_identical_reappend", func(t *testing.T) {
		const dedupID = "em-t-reads-dedup"
		insertNode(ctx, t, pool, dedupID, "ACTIVE", "2026-01-01", "'infinity'", "first append")

		var before int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sec_node WHERE id = $1`, dedupID).Scan(&before); err != nil {
			t.Fatal(err)
		}

		// Identical re-append with ON CONFLICT DO NOTHING
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, processing_version, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'ACTIVE', '2026-01-01', 'infinity', 0, 'test', 'SEED_LOAD', 'first append', 'test')
			ON CONFLICT DO NOTHING`, dedupID)
		if err != nil {
			t.Fatalf("ON CONFLICT DO NOTHING: %v", err)
		}

		var after int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sec_node WHERE id = $1`, dedupID).Scan(&after); err != nil {
			t.Fatal(err)
		}
		if after != before {
			t.Fatalf("ON CONFLICT DO NOTHING added a row: before=%d, after=%d", before, after)
		}
	})

	t.Run("retry_dedup_divergent_payload_without_conflict_clause_is_23505", func(t *testing.T) {
		const divID = "em-t-reads-div"
		insertNode(ctx, t, pool, divID, "ACTIVE", "2026-01-01", "'infinity'", "first append")

		// Same PK columns, different status (payload divergence)
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, processing_version, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'INACTIVE', '2026-01-01', 'infinity', 0, 'test', 'SEED_LOAD', 'divergent payload', 'test')`, divID)
		assertSQLState(t, err, "23505", "divergent payload without ON CONFLICT")
	})

	t.Run("retry_dedup_divergent_payload_with_do_nothing_is_silent", func(t *testing.T) {
		// The loader's obligation is to surface this; the store drops it silently.
		const divID2 = "em-t-reads-div2"
		insertNode(ctx, t, pool, divID2, "ACTIVE", "2026-01-01", "'infinity'", "first append")

		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, processing_version, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'INACTIVE', '2026-01-01', 'infinity', 0, 'test', 'SEED_LOAD', 'divergent payload', 'test')
			ON CONFLICT DO NOTHING`, divID2)
		if err != nil {
			t.Fatalf("ON CONFLICT DO NOTHING with divergent payload: %v", err)
		}

		var status string
		if err := pool.QueryRow(ctx, `
			SELECT status FROM sec_node_current WHERE id = $1`, divID2).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "ACTIVE" {
			t.Fatalf("got status=%s, want ACTIVE — the original wins", status)
		}
	})

	// -----------------------------------------------------------------------
	// WI-2 continued: snapshot reads (ADR-0006 §5)
	// -----------------------------------------------------------------------

	t.Run("snapshot_in_flight_writer_excluded", func(t *testing.T) {
		const snapID = "em-t-reads-snap"

		// txA: insert, don't commit yet
		txA, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txA.Rollback(ctx)

		_, err = txA.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'ACTIVE', '2026-01-01', 'test', 'SEED_LOAD', 'in-flight', 'test')`, snapID)
		if err != nil {
			t.Fatal(err)
		}

		// txB: capture snapshot while txA is uncommitted
		var snapshot string
		if err := pool.QueryRow(ctx, `SELECT pg_current_snapshot()::text`).Scan(&snapshot); err != nil {
			t.Fatal(err)
		}

		// Commit txA
		if err := txA.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		// Verify: sec_node_as_of(date) sees it, sec_node_as_of(date, snapshot) does not
		var countDate int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM sec_node_as_of('2026-01-01'::date) WHERE id = $1`, snapID).Scan(&countDate); err != nil {
			t.Fatal(err)
		}
		if countDate != 1 {
			t.Fatalf("sec_node_as_of(date) after commit: got %d, want 1", countDate)
		}

		var countSnap int
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(*) FROM sec_node_as_of('2026-01-01'::date, '%s'::pg_snapshot) WHERE id = $1`, snapshot), snapID).Scan(&countSnap); err != nil {
			t.Fatal(err)
		}
		if countSnap != 0 {
			t.Fatalf("sec_node_as_of(date, snapshot) with pre-commit snapshot: got %d, want 0", countSnap)
		}
	})

	t.Run("snapshot_before_tombstone_still_sees_row", func(t *testing.T) {
		const stID = "em-t-reads-snaptomb"

		insertNode(ctx, t, pool, stID, "ACTIVE", "2026-01-01", "'infinity'", "row before tombstone")

		// Capture snapshot after the row lands
		var snapshot string
		if err := pool.QueryRow(ctx, `SELECT pg_current_snapshot()::text`).Scan(&snapshot); err != nil {
			t.Fatal(err)
		}

		// Add tombstone
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'ACTIVE', '2026-01-01', '2026-01-01', 'test', 'RETRACTION', 'tombstone after snapshot', 'test')`, stID)
		if err != nil {
			t.Fatal(err)
		}

		// Current read: tombstoned, absent
		var countCurr int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM sec_node_current WHERE id = $1`, stID).Scan(&countCurr); err != nil {
			t.Fatal(err)
		}
		if countCurr != 0 {
			t.Fatalf("current after tombstone: got %d, want 0", countCurr)
		}

		// Snapshot read: the snapshot predates the tombstone, so the row is visible
		var countSnap int
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(*) FROM sec_node_as_of('2026-01-01'::date, '%s'::pg_snapshot) WHERE id = $1`, snapshot), stID).Scan(&countSnap); err != nil {
			t.Fatal(err)
		}
		if countSnap != 1 {
			t.Fatalf("snapshot read before tombstone: got %d, want 1", countSnap)
		}
	})

	t.Run("edge_snapshot_in_flight_writer_excluded", func(t *testing.T) {
		txA, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txA.Rollback(ctx)

		_, err = txA.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ('sec-t-esnap-src', 'SECURITY', 'em-t-esnap-dst', 'ENTITY', 'ISSUED_BY', '2026-01-01', 'test', 'SEED_LOAD', 'in-flight edge', 'test')`)
		if err != nil {
			t.Fatal(err)
		}

		var snapshot string
		if err := pool.QueryRow(ctx, `SELECT pg_current_snapshot()::text`).Scan(&snapshot); err != nil {
			t.Fatal(err)
		}

		if err := txA.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var countSnap int
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(*) FROM sec_edge_as_of('2026-01-01'::date, '%s'::pg_snapshot)
			WHERE src_id = 'sec-t-esnap-src'`, snapshot)).Scan(&countSnap); err != nil {
			t.Fatal(err)
		}
		if countSnap != 0 {
			t.Fatalf("edge snapshot read before commit: got %d, want 0", countSnap)
		}
	})

	// -----------------------------------------------------------------------
	// WI-2: plan-shape assertions (decision 2)
	// -----------------------------------------------------------------------

	t.Run("plan_shape", func(t *testing.T) {
		planShapeCases := []struct {
			name, query, expectedIndex string
		}{
			{
				"sec_node_as_of_uses_resolve_idx",
				"EXPLAIN (FORMAT JSON) SELECT * FROM sec_node_as_of('2026-03-01'::date)",
				"sec_node_resolve_idx",
			},
			{
				"sec_node_as_of_kind_uses_type_idx",
				"EXPLAIN (FORMAT JSON) SELECT * FROM sec_node_as_of_kind('2026-03-01'::date, 'ENTITY')",
				"sec_node_type_idx",
			},
			{
				"sec_edge_as_of_uses_resolve_idx",
				"EXPLAIN (FORMAT JSON) SELECT * FROM sec_edge_as_of('2026-03-01'::date)",
				"sec_edge_resolve_idx",
			},
		}

		for _, tc := range planShapeCases {
			t.Run(tc.name, func(t *testing.T) {
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(ctx)

				if _, err := tx.Exec(ctx, "SET LOCAL enable_sort = off"); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Exec(ctx, "SET LOCAL enable_bitmapscan = off"); err != nil {
					t.Fatal(err)
				}

				var planJSON string
				if err := tx.QueryRow(ctx, tc.query).Scan(&planJSON); err != nil {
					t.Fatalf("EXPLAIN: %v", err)
				}

				if !strings.Contains(planJSON, tc.expectedIndex) {
					t.Errorf("plan does not reference %s\nplan: %s", tc.expectedIndex, planJSON)
				}
				sortCount := strings.Count(planJSON, `"Node Type": "Sort"`) + strings.Count(planJSON, `"Node Type": "Incremental Sort"`)
				if sortCount > 1 {
					t.Errorf("plan contains %d sort nodes (expected at most 1 for the outer DISTINCT ON) — index direction mutations would survive\nplan: %s", sortCount, planJSON)
				}
			})
		}
	})
}

// TestSecStoreCurrentViewBoundaries tests the valid-time window boundaries on the _current
// views using current_date so the test works regardless of when it runs.
func TestSecStoreCurrentViewBoundaries(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	t.Run("node_valid_from_equals_today_appears", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		_, err = tx.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ('em-t-curr-lb', 'ENTITY', 'ACTIVE', (now() AT TIME ZONE 'utc')::date, 'infinity', 'test', 'SEED_LOAD', 'valid_from = today', 'test')`)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		var count int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM sec_node_current WHERE id = 'em-t-curr-lb'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("node with valid_from = current_date: got %d in _current, want 1 (lower bound is inclusive)", count)
		}
	})

	t.Run("node_valid_to_equals_today_absent", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		_, err = tx.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ('em-t-curr-ub', 'ENTITY', 'ACTIVE', '2020-01-01', (now() AT TIME ZONE 'utc')::date, 'test', 'SEED_LOAD', 'valid_to = today', 'test')`)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		var count int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM sec_node_current WHERE id = 'em-t-curr-ub'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("node with valid_to = current_date: got %d in _current, want 0 (upper bound is exclusive)", count)
		}
	})

	t.Run("edge_valid_from_equals_today_appears", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		_, err = tx.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ('sec-t-curr-lb', 'SECURITY', 'em-t-curr-elb', 'ENTITY', 'ISSUED_BY', (now() AT TIME ZONE 'utc')::date, 'infinity', 'test', 'SEED_LOAD', 'valid_from = today', 'test')`)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		var count int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM sec_edge_current WHERE src_id = 'sec-t-curr-lb'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("edge with valid_from = current_date: got %d in _current, want 1", count)
		}
	})

	t.Run("edge_valid_to_equals_today_absent", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		_, err = tx.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ('sec-t-curr-ub', 'SECURITY', 'em-t-curr-eub', 'ENTITY', 'ISSUED_BY', '2020-01-01', (now() AT TIME ZONE 'utc')::date, 'test', 'SEED_LOAD', 'valid_to = today', 'test')`)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		var count int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM sec_edge_current WHERE src_id = 'sec-t-curr-ub'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("edge with valid_to = current_date: got %d in _current, want 0", count)
		}
	})
}

// TestSecStoreRecordIDTiebreakAcrossAllReadObjects inserts two rows in the same transaction
// so they share (id, valid_from, pv, ingest_xid) and differ only in record_id. The higher
// record_id must win in every read object. Index scans are disabled so the SQL ORDER BY
// clause is exercised rather than being supplied by the index.
func TestSecStoreRecordIDTiebreakAcrossAllReadObjects(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const nodeID = "em-t-recid-tb"
	const edgeSrc = "sec-t-recid-tb"

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `
		INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
		VALUES ($1, 'ENTITY', 'ACTIVE', '2026-01-01', 'infinity', 'test', 'SEED_LOAD', 'lower record_id', 'test')`, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
		VALUES ($1, 'ENTITY', 'INACTIVE', '2026-01-01', '9999-12-31', 'test', 'SEED_LOAD', 'higher record_id', 'test')`, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
		VALUES ($1, 'SECURITY', 'em-t-recid-edst', 'ENTITY', 'ISSUED_BY', '2026-01-01', 'infinity', 'test', 'SEED_LOAD', 'lower record_id', 'test')`, edgeSrc)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
		VALUES ($1, 'SECURITY', 'em-t-recid-edst', 'ENTITY', 'ISSUED_BY', '2026-01-01', '9999-12-31', 'test', 'SEED_LOAD', 'higher record_id', 'test')`, edgeSrc)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	qtx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer qtx.Rollback(ctx)

	for _, ddl := range []string{
		`DROP INDEX sec_node_resolve_idx`,
		`CREATE INDEX sec_node_resolve_idx ON sec_node (id, valid_from, processing_version DESC, ingest_xid DESC)`,
		`DROP INDEX sec_node_type_idx`,
		`CREATE INDEX sec_node_type_idx ON sec_node (record_type, id, valid_from, processing_version DESC, ingest_xid DESC)`,
		`DROP INDEX sec_edge_resolve_idx`,
		`CREATE INDEX sec_edge_resolve_idx ON sec_edge (rel_type, src_id, dst_id, edge_disc, valid_from, processing_version DESC, ingest_xid DESC)`,
	} {
		if _, err := qtx.Exec(ctx, ddl); err != nil {
			t.Fatalf("DDL %q: %v", ddl, err)
		}
	}

	reads := []struct {
		name, sql string
		args      []any
	}{
		{"sec_node_current", `SELECT change_reason FROM sec_node_current WHERE id = $1`, []any{nodeID}},
		{"sec_node_as_of", `SELECT change_reason FROM sec_node_as_of('2026-03-01'::date) WHERE id = $1`, []any{nodeID}},
		{"sec_node_as_of_kind", `SELECT change_reason FROM sec_node_as_of_kind('2026-03-01'::date, 'ENTITY') WHERE id = $1`, []any{nodeID}},
		{"sec_node_as_of_snapshot", `SELECT change_reason FROM sec_node_as_of('2026-03-01'::date, pg_current_snapshot()) WHERE id = $1`, []any{nodeID}},
		{"sec_edge_current", `SELECT change_reason FROM sec_edge_current WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`, []any{edgeSrc}},
		{"sec_edge_as_of", `SELECT change_reason FROM sec_edge_as_of('2026-03-01'::date) WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`, []any{edgeSrc}},
		{"sec_edge_as_of_snapshot", `SELECT change_reason FROM sec_edge_as_of('2026-03-01'::date, pg_current_snapshot()) WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`, []any{edgeSrc}},
	}
	for _, q := range reads {
		var reason string
		if err := qtx.QueryRow(ctx, q.sql, q.args...).Scan(&reason); err != nil {
			t.Fatalf("%s: %v", q.name, err)
		}
		if reason != "higher record_id" {
			t.Fatalf("%s: got change_reason=%q, want \"higher record_id\" — record_id DESC tiebreak failed", q.name, reason)
		}
	}
}

// TestSecStoreIngestXidTiebreakAcrossAllReadObjects verifies ingest_xid wins over record_id
// on all read objects. Two interleaved transactions: txEarly gets a lower xid, txLate a
// higher one. txLate inserts first (lower record_id) and txEarly second (higher record_id).
// ingest_xid DESC must beat record_id DESC — the late-xid row wins.
func TestSecStoreIngestXidTiebreakAcrossAllReadObjects(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const nodeID = "em-t-xid-ntb"

	txEarly, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer txEarly.Rollback(ctx)
	if _, err := txEarly.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
		t.Fatal(err)
	}

	txLate, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer txLate.Rollback(ctx)
	if _, err := txLate.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
		t.Fatal(err)
	}

	_, err = txLate.Exec(ctx, `
		INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
		VALUES ($1, 'ENTITY', 'INACTIVE', '2026-01-01', 'infinity', 'test', 'SEED_LOAD', 'late-xid wins', 'test')`, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = txEarly.Exec(ctx, `
		INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
		VALUES ($1, 'ENTITY', 'ACTIVE', '2026-01-01', '9999-12-31', 'test', 'SEED_LOAD', 'early-xid loses', 'test')`, nodeID)
	if err != nil {
		t.Fatal(err)
	}

	const edgeSrc = "sec-t-xid-etb"
	_, err = txLate.Exec(ctx, `
		INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
		VALUES ($1, 'SECURITY', 'em-t-xid-edst', 'ENTITY', 'ISSUED_BY', '2026-01-01', 'infinity', 'test', 'SEED_LOAD', 'late-xid wins', 'test')`, edgeSrc)
	if err != nil {
		t.Fatal(err)
	}
	_, err = txEarly.Exec(ctx, `
		INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
		VALUES ($1, 'SECURITY', 'em-t-xid-edst', 'ENTITY', 'ISSUED_BY', '2026-01-01', '9999-12-31', 'test', 'SEED_LOAD', 'early-xid loses', 'test')`, edgeSrc)
	if err != nil {
		t.Fatal(err)
	}

	if err := txLate.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := txEarly.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	nodeReads := []struct {
		name, sql string
	}{
		{"sec_node_current", `SELECT change_reason FROM sec_node_current WHERE id = $1`},
		{"sec_node_as_of", `SELECT change_reason FROM sec_node_as_of('2026-03-01'::date) WHERE id = $1`},
		{"sec_node_as_of_kind", `SELECT change_reason FROM sec_node_as_of_kind('2026-03-01'::date, 'ENTITY') WHERE id = $1`},
	}
	for _, q := range nodeReads {
		t.Run(q.name, func(t *testing.T) {
			var reason string
			if err := pool.QueryRow(ctx, q.sql, nodeID).Scan(&reason); err != nil {
				t.Fatalf("%s: %v", q.name, err)
			}
			if reason != "late-xid wins" {
				t.Fatalf("%s: got change_reason=%q, want \"late-xid wins\" — ingest_xid DESC must beat record_id DESC", q.name, reason)
			}
		})
	}

	edgeReads := []struct {
		name, sql string
	}{
		{"sec_edge_current", `SELECT change_reason FROM sec_edge_current WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`},
		{"sec_edge_as_of", `SELECT change_reason FROM sec_edge_as_of('2026-03-01'::date) WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`},
	}
	for _, q := range edgeReads {
		t.Run(q.name, func(t *testing.T) {
			var reason string
			if err := pool.QueryRow(ctx, q.sql, edgeSrc).Scan(&reason); err != nil {
				t.Fatalf("%s: %v", q.name, err)
			}
			if reason != "late-xid wins" {
				t.Fatalf("%s: got change_reason=%q, want \"late-xid wins\" — ingest_xid DESC must beat record_id DESC", q.name, reason)
			}
		})
	}
}

// TestSecStoreOverlappingWindowsOnCurrentViews tests that _current views pick the latest
// valid_from when two open windows overlap today.
func TestSecStoreOverlappingWindowsOnCurrentViews(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	t.Run("node_current_picks_latest_valid_from", func(t *testing.T) {
		const id = "em-t-curr-overlap"
		insertNode(ctx, t, pool, id, "ACTIVE", "2020-01-01", "'infinity'", "older window")
		insertNode(ctx, t, pool, id, "INACTIVE", "2025-01-01", "'infinity'", "newer window")

		var status string
		if err := pool.QueryRow(ctx, `
			SELECT status FROM sec_node_current WHERE id = $1`, id).Scan(&status); err != nil {
			t.Fatalf("sec_node_current: %v", err)
		}
		if status != "INACTIVE" {
			t.Fatalf("got status=%s, want INACTIVE — the later valid_from must win", status)
		}
	})

	t.Run("edge_current_picks_latest_valid_from", func(t *testing.T) {
		const src = "sec-t-curr-overlap"
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-curr-eol', 'ENTITY', 'ISSUED_BY', '2020-01-01', 'test', 'SEED_LOAD', 'older window', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}
		_, err = pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-curr-eol', 'ENTITY', 'ISSUED_BY', '2025-01-01', 'test', 'SEED_LOAD', 'newer window', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}

		var reason string
		if err := pool.QueryRow(ctx, `
			SELECT change_reason FROM sec_edge_current WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`, src).Scan(&reason); err != nil {
			t.Fatalf("sec_edge_current: %v", err)
		}
		if reason != "newer window" {
			t.Fatalf("got change_reason=%q, want \"newer window\"", reason)
		}
	})
}

// TestSecStoreSnapshotVariantResolutionAndBoundaries exercises the 2-arg (pg_snapshot)
// overloads of sec_node_as_of and sec_edge_as_of. The existing tests cover the 1-arg
// variants; these mirror them for the snapshot variants that have their own SQL.
func TestSecStoreSnapshotVariantResolutionAndBoundaries(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	// Helper: capture a snapshot that sees everything committed so far.
	captureSnapshot := func(t *testing.T) string {
		t.Helper()
		var snap string
		if err := pool.QueryRow(ctx, `SELECT pg_current_snapshot()::text`).Scan(&snap); err != nil {
			t.Fatalf("capture snapshot: %v", err)
		}
		return snap
	}

	// --- Node snapshot: pv wins ---
	t.Run("node_snapshot_pv_wins_over_later_append", func(t *testing.T) {
		const id = "em-t-snap-pv"
		insertNode(ctx, t, pool, id, "ACTIVE", "2026-01-01", "'infinity'", "the original")

		pvN := correctionVersion(ctx, t, pool, "sec_node")
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, processing_version, `+secstoreSpine+`)
			VALUES ('em-t-snap-pv', 'ENTITY', 'INACTIVE', '2026-01-01', 'infinity', %d, 'test', 'RESTATEMENT', 'correction', 'test')`, pvN))
		if err != nil {
			t.Fatalf("insert correction: %v", err)
		}

		snap := captureSnapshot(t)
		// Also insert a pv-0 row AFTER the snapshot (in a new xid)
		_, err = pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, processing_version, `+secstoreSpine+`)
			VALUES ('em-t-snap-pv', 'ENTITY', 'ACTIVE', '2026-01-01', '9999-12-31', 0, 'test', 'SEED_LOAD', 'late pv-0', 'test')`)
		if err != nil {
			t.Fatalf("late pv-0: %v", err)
		}

		var pv int
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT processing_version FROM sec_node_as_of('2026-03-01'::date, '%s'::pg_snapshot) WHERE id = $1`, snap), id).Scan(&pv); err != nil {
			t.Fatalf("snapshot node read: %v", err)
		}
		if pv != pvN {
			t.Fatalf("got pv=%d, want %d — the correction must win over a pv-0 append", pv, pvN)
		}
	})

	// --- Node snapshot: xid tiebreak (kills M025, M094) ---
	t.Run("node_snapshot_xid_tiebreak", func(t *testing.T) {
		const id = "em-t-snap-xid"
		txEarly, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txEarly.Rollback(ctx)
		if _, err := txEarly.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
			t.Fatal(err)
		}

		txLate, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txLate.Rollback(ctx)
		if _, err := txLate.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
			t.Fatal(err)
		}

		_, err = txLate.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'INACTIVE', '2026-01-01', 'infinity', 'test', 'SEED_LOAD', 'late-xid wins', 'test')`, id)
		if err != nil {
			t.Fatal(err)
		}
		_, err = txEarly.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'ACTIVE', '2026-01-01', '9999-12-31', 'test', 'SEED_LOAD', 'early-xid loses', 'test')`, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := txLate.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := txEarly.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		snap := captureSnapshot(t)
		var reason string
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT change_reason FROM sec_node_as_of('2026-03-01'::date, '%s'::pg_snapshot) WHERE id = $1`, snap), id).Scan(&reason); err != nil {
			t.Fatalf("snapshot node read: %v", err)
		}
		if reason != "late-xid wins" {
			t.Fatalf("got change_reason=%q, want \"late-xid wins\"", reason)
		}
	})

	// --- Node snapshot: record_id tiebreak (kills M026, M093) ---
	t.Run("node_snapshot_record_id_tiebreak", func(t *testing.T) {
		const id = "em-t-snap-recid"
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		_, err = tx.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'ACTIVE', '2026-01-01', 'infinity', 'test', 'SEED_LOAD', 'lower record_id', 'test')`, id)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'INACTIVE', '2026-01-01', '9999-12-31', 'test', 'SEED_LOAD', 'higher record_id', 'test')`, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		snap := captureSnapshot(t)
		var reason string
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT change_reason FROM sec_node_as_of('2026-03-01'::date, '%s'::pg_snapshot) WHERE id = $1`, snap), id).Scan(&reason); err != nil {
			t.Fatalf("snapshot node read: %v", err)
		}
		if reason != "higher record_id" {
			t.Fatalf("got change_reason=%q, want \"higher record_id\"", reason)
		}
	})

	// --- Node snapshot: swap_pv_ingest_xid (kills M095) ---
	t.Run("node_snapshot_pv_beats_xid", func(t *testing.T) {
		// Two transactions: txEarly has a lower xid but inserts at a higher pv;
		// txLate has a higher xid but inserts at pv=0. The pv must win.
		const id = "em-t-snap-pvxid"

		txEarly, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txEarly.Rollback(ctx)
		if _, err := txEarly.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
			t.Fatal(err)
		}
		txLate, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txLate.Rollback(ctx)
		if _, err := txLate.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
			t.Fatal(err)
		}

		// txLate at pv=0 (higher xid)
		_, err = txLate.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, processing_version, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'ACTIVE', '2026-01-01', 'infinity', 0, 'test', 'SEED_LOAD', 'pv0 higher xid', 'test')`, id)
		if err != nil {
			t.Fatal(err)
		}
		// txEarly at pv=1 (lower xid)
		_, err = txEarly.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, processing_version, `+secstoreSpine+`)
			VALUES ($1, 'ENTITY', 'INACTIVE', '2026-01-01', '9999-12-31', 1, 'test', 'RESTATEMENT', 'pv1 lower xid', 'test')`, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := txLate.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := txEarly.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		snap := captureSnapshot(t)
		var reason string
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT change_reason FROM sec_node_as_of('2026-03-01'::date, '%s'::pg_snapshot) WHERE id = $1`, snap), id).Scan(&reason); err != nil {
			t.Fatalf("snapshot read: %v", err)
		}
		if reason != "pv1 lower xid" {
			t.Fatalf("got change_reason=%q, want \"pv1 lower xid\" — processing_version must rank before ingest_xid", reason)
		}
	})

	// --- Node snapshot: overlapping windows outer ORDER BY valid_from DESC (kills M027) ---
	t.Run("node_snapshot_overlapping_windows", func(t *testing.T) {
		const id = "em-t-snap-overlap"
		insertNode(ctx, t, pool, id, "ACTIVE", "2026-01-01", "'infinity'", "older window")
		insertNode(ctx, t, pool, id, "INACTIVE", "2026-03-01", "'infinity'", "newer window")

		snap := captureSnapshot(t)
		var validFrom string
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT valid_from::text FROM sec_node_as_of('2026-04-01'::date, '%s'::pg_snapshot) WHERE id = $1`, snap), id).Scan(&validFrom); err != nil {
			t.Fatalf("snapshot read: %v", err)
		}
		if validFrom != "2026-03-01" {
			t.Fatalf("got valid_from=%s, want 2026-03-01 (the later window)", validFrom)
		}
	})

	// --- Node snapshot: step_reversal (kills M043) ---
	t.Run("node_snapshot_step_reversal_correction_narrows_window", func(t *testing.T) {
		// The step_reversal mutation moves the window filter into the inner CTE.
		// It survives pv=0 close-and-open because both rows pass the filter.
		// To catch it: a pv=N correction that narrows valid_to, queried at a date
		// inside the original window but outside the corrected one.
		//
		// pv=0: valid_from=Jan, valid_to=infinity (open)
		// pv=N: valid_from=Jan, valid_to=Jun (correction narrows the window)
		// Query as_of(Jul): correct SQL resolves first (pv=N wins, valid_to=Jun),
		// then filters (Jul < Jun → false → no row). Mutated SQL filters first
		// (pv=0 passes Jul < infinity, pv=N fails Jul < Jun), then resolves (pv=0
		// is the only candidate → returns the original). Different result.
		const id = "em-t-snap-steprev"
		insertNode(ctx, t, pool, id, "ACTIVE", "2026-01-01", "'infinity'", "original open window")

		pvN := correctionVersion(ctx, t, pool, "sec_node")
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, processing_version, `+secstoreSpine+`)
			VALUES ('em-t-snap-steprev', 'ENTITY', 'ACTIVE', '2026-01-01', '2026-06-01', %d, 'test', 'RESTATEMENT', 'correction narrows window', 'test')`, pvN))
		if err != nil {
			t.Fatalf("insert correction: %v", err)
		}

		snap := captureSnapshot(t)
		var count int
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(*) FROM sec_node_as_of('2026-07-01'::date, '%s'::pg_snapshot) WHERE id = $1`, snap), id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("got count=%d, want 0 — the pv-N correction narrowed valid_to to Jun; Jul must be excluded (resolve before window)", count)
		}
	})

	// --- Node snapshot: boundary at valid_to (kills M057) ---
	t.Run("node_snapshot_at_valid_to_excluded", func(t *testing.T) {
		const id = "em-t-snap-bound"
		insertNode(ctx, t, pool, id, "ACTIVE", "2026-01-01", "'2026-06-01'", "single closed window")

		snap := captureSnapshot(t)
		var count int
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(*) FROM sec_node_as_of('2026-06-01'::date, '%s'::pg_snapshot) WHERE id = $1`, snap), id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("snapshot at valid_to: got %d, want 0 (half-open interval)", count)
		}
	})

	// --- Edge snapshot: pv wins (kills M036) ---
	t.Run("edge_snapshot_pv_wins", func(t *testing.T) {
		const src = "sec-t-snap-epv"
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-snap-epv', 'ENTITY', 'ISSUED_BY', '2026-01-01', 'test', 'SEED_LOAD', 'the original', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}
		edgePv := correctionVersion(ctx, t, pool, "sec_edge")
		_, err = pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, processing_version, `+secstoreSpine+`)
			VALUES ('sec-t-snap-epv', 'SECURITY', 'em-t-snap-epv', 'ENTITY', 'ISSUED_BY', '2026-01-01', %d, 'test', 'RESTATEMENT', 'edge correction', 'test')`, edgePv))
		if err != nil {
			t.Fatal(err)
		}

		snap := captureSnapshot(t)
		var pv int
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT processing_version FROM sec_edge_as_of('2026-03-01'::date, '%s'::pg_snapshot)
			WHERE src_id = 'sec-t-snap-epv' AND rel_type = 'ISSUED_BY'`, snap)).Scan(&pv); err != nil {
			t.Fatalf("snapshot edge read: %v", err)
		}
		if pv != edgePv {
			t.Fatalf("got pv=%d, want %d", pv, edgePv)
		}
	})

	// --- Edge snapshot: xid tiebreak (kills M037, M103) ---
	t.Run("edge_snapshot_xid_tiebreak", func(t *testing.T) {
		const src = "sec-t-snap-exid"
		txEarly, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txEarly.Rollback(ctx)
		if _, err := txEarly.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
			t.Fatal(err)
		}
		txLate, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txLate.Rollback(ctx)
		if _, err := txLate.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
			t.Fatal(err)
		}

		_, err = txLate.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-snap-exid', 'ENTITY', 'ISSUED_BY', '2026-01-01', 'infinity', 'test', 'SEED_LOAD', 'late-xid wins', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}
		_, err = txEarly.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-snap-exid', 'ENTITY', 'ISSUED_BY', '2026-01-01', '9999-12-31', 'test', 'SEED_LOAD', 'early-xid loses', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}
		if err := txLate.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := txEarly.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		snap := captureSnapshot(t)
		var reason string
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT change_reason FROM sec_edge_as_of('2026-03-01'::date, '%s'::pg_snapshot)
			WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`, snap), src).Scan(&reason); err != nil {
			t.Fatalf("snapshot edge xid: %v", err)
		}
		if reason != "late-xid wins" {
			t.Fatalf("got change_reason=%q, want \"late-xid wins\"", reason)
		}
	})

	// --- Edge snapshot: record_id tiebreak (kills M038, M102) ---
	t.Run("edge_snapshot_record_id_tiebreak", func(t *testing.T) {
		const src = "sec-t-snap-erecid"
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		_, err = tx.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-snap-erecid', 'ENTITY', 'ISSUED_BY', '2026-01-01', 'infinity', 'test', 'SEED_LOAD', 'lower record_id', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-snap-erecid', 'ENTITY', 'ISSUED_BY', '2026-01-01', '9999-12-31', 'test', 'SEED_LOAD', 'higher record_id', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		snap := captureSnapshot(t)
		var reason string
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT change_reason FROM sec_edge_as_of('2026-03-01'::date, '%s'::pg_snapshot)
			WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`, snap), src).Scan(&reason); err != nil {
			t.Fatalf("snapshot edge recid: %v", err)
		}
		if reason != "higher record_id" {
			t.Fatalf("got change_reason=%q, want \"higher record_id\"", reason)
		}
	})

	// --- Edge snapshot: swap_pv_ingest_xid (kills M104) ---
	t.Run("edge_snapshot_pv_beats_xid", func(t *testing.T) {
		const src = "sec-t-snap-epvxid"

		txEarly, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txEarly.Rollback(ctx)
		if _, err := txEarly.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
			t.Fatal(err)
		}
		txLate, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txLate.Rollback(ctx)
		if _, err := txLate.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
			t.Fatal(err)
		}

		// txLate at pv=0 (higher xid)
		_, err = txLate.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, processing_version, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-snap-epvxid', 'ENTITY', 'ISSUED_BY', '2026-01-01', 'infinity', 0, 'test', 'SEED_LOAD', 'pv0 higher xid', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}
		// txEarly at pv=1 (lower xid)
		_, err = txEarly.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, processing_version, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-snap-epvxid', 'ENTITY', 'ISSUED_BY', '2026-01-01', '9999-12-31', 1, 'test', 'RESTATEMENT', 'pv1 lower xid', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}
		if err := txLate.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := txEarly.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		snap := captureSnapshot(t)
		var reason string
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT change_reason FROM sec_edge_as_of('2026-03-01'::date, '%s'::pg_snapshot)
			WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`, snap), src).Scan(&reason); err != nil {
			t.Fatalf("snapshot edge pvxid: %v", err)
		}
		if reason != "pv1 lower xid" {
			t.Fatalf("got change_reason=%q, want \"pv1 lower xid\"", reason)
		}
	})

	// --- Edge snapshot: overlapping windows (kills M039) ---
	t.Run("edge_snapshot_overlapping_windows", func(t *testing.T) {
		const src = "sec-t-snap-eovrlp"
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-snap-eovrlp', 'ENTITY', 'ISSUED_BY', '2026-01-01', 'test', 'SEED_LOAD', 'older', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}
		_, err = pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-snap-eovrlp', 'ENTITY', 'ISSUED_BY', '2026-03-01', 'test', 'SEED_LOAD', 'newer', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}

		snap := captureSnapshot(t)
		var validFrom string
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT valid_from::text FROM sec_edge_as_of('2026-04-01'::date, '%s'::pg_snapshot)
			WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`, snap), src).Scan(&validFrom); err != nil {
			t.Fatalf("snapshot edge overlap: %v", err)
		}
		if validFrom != "2026-03-01" {
			t.Fatalf("got valid_from=%s, want 2026-03-01", validFrom)
		}
	})

	// --- Edge snapshot: step_reversal (kills M046) ---
	t.Run("edge_snapshot_step_reversal_correction_narrows_window", func(t *testing.T) {
		const src = "sec-t-snap-estep"
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-snap-estep', 'ENTITY', 'ISSUED_BY', '2026-01-01', 'test', 'SEED_LOAD', 'original open', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}

		edgePv := correctionVersion(ctx, t, pool, "sec_edge")
		_, err = pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, processing_version, `+secstoreSpine+`)
			VALUES ('sec-t-snap-estep', 'SECURITY', 'em-t-snap-estep', 'ENTITY', 'ISSUED_BY', '2026-01-01', '2026-06-01', %d, 'test', 'RESTATEMENT', 'correction narrows window', 'test')`, edgePv))
		if err != nil {
			t.Fatal(err)
		}

		snap := captureSnapshot(t)
		var count int
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(*) FROM sec_edge_as_of('2026-07-01'::date, '%s'::pg_snapshot)
			WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`, snap), src).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("got count=%d, want 0 — pv-N correction narrowed valid_to to Jun; Jul must be excluded", count)
		}
	})

	// --- Edge snapshot: boundary at valid_from (kills M062) ---
	t.Run("edge_snapshot_at_valid_from_included", func(t *testing.T) {
		const src = "sec-t-snap-elf"
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-snap-elf', 'ENTITY', 'ISSUED_BY', '2026-06-01', 'test', 'SEED_LOAD', 'at boundary', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}

		snap := captureSnapshot(t)
		var count int
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(*) FROM sec_edge_as_of('2026-06-01'::date, '%s'::pg_snapshot)
			WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`, snap), src).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("edge snapshot at valid_from: got %d, want 1 (lower bound inclusive)", count)
		}
	})

	// --- Edge snapshot: boundary at valid_to (kills M063) ---
	t.Run("edge_snapshot_at_valid_to_excluded", func(t *testing.T) {
		const src = "sec-t-snap-eut"
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-snap-eut', 'ENTITY', 'ISSUED_BY', '2026-01-01', '2026-06-01', 'test', 'SEED_LOAD', 'closed', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}

		snap := captureSnapshot(t)
		var count int
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(*) FROM sec_edge_as_of('2026-06-01'::date, '%s'::pg_snapshot)
			WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`, snap), src).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("edge snapshot at valid_to: got %d, want 0 (upper bound exclusive)", count)
		}
	})

	// --- Edge snapshot: delete_window_filter (kills M070) ---
	t.Run("edge_snapshot_window_filter_excludes_future", func(t *testing.T) {
		const src = "sec-t-snap-efilt"
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-snap-efilt', 'ENTITY', 'ISSUED_BY', '2099-01-01', 'test', 'SEED_LOAD', 'future edge', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}

		snap := captureSnapshot(t)
		var count int
		if err := pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(*) FROM sec_edge_as_of('2026-06-01'::date, '%s'::pg_snapshot)
			WHERE src_id = $1`, snap), src).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("snapshot read of future edge: got %d, want 0 (window filter must exclude)", count)
		}
	})
}

// TestSecStoreAsOfKindWindowFilter ensures sec_node_as_of_kind has a working window filter
// that excludes future and past-expired rows.
func TestSecStoreAsOfKindWindowFilter(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	// A node starting far in the future: sec_node_as_of_kind should not return it for today.
	const futureID = "em-t-kind-future"
	insertNode(ctx, t, pool, futureID, "ACTIVE", "2099-01-01", "'infinity'", "future node")

	var count int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM sec_node_as_of_kind('2026-06-01'::date, 'ENTITY') WHERE id = $1`, futureID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("as_of_kind returned a future node: got %d, want 0", count)
	}

	// A node that expired in the past.
	const expiredID = "em-t-kind-expired"
	insertNode(ctx, t, pool, expiredID, "ACTIVE", "2020-01-01", "'2025-01-01'", "expired node")

	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM sec_node_as_of_kind('2026-06-01'::date, 'ENTITY') WHERE id = $1`, expiredID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("as_of_kind returned an expired node: got %d, want 0", count)
	}
}

// TestSecStoreStepReversalOnAsOfKindAndEdgeAsOf verifies that the window filter runs AFTER
// resolution on sec_node_as_of_kind and 1-arg sec_edge_as_of. A pv=N correction narrows
// valid_to; querying at a date between the original and corrected valid_to distinguishes the
// two orderings.
func TestSecStoreStepReversalOnAsOfKindAndEdgeAsOf(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	t.Run("sec_node_as_of_kind", func(t *testing.T) {
		const id = "em-t-steprev-kind"
		insertNode(ctx, t, pool, id, "ACTIVE", "2026-01-01", "'infinity'", "original open window")

		pvN := correctionVersion(ctx, t, pool, "sec_node")
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, processing_version, `+secstoreSpine+`)
			VALUES ('em-t-steprev-kind', 'ENTITY', 'ACTIVE', '2026-01-01', '2026-06-01', %d, 'test', 'RESTATEMENT', 'correction narrows window', 'test')`, pvN))
		if err != nil {
			t.Fatalf("insert correction: %v", err)
		}

		var count int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM sec_node_as_of_kind('2026-07-01'::date, 'ENTITY') WHERE id = $1`, id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("got count=%d, want 0 — pv-N correction narrowed valid_to to Jun; Jul must be excluded (resolve before window)", count)
		}
	})

	t.Run("sec_edge_as_of", func(t *testing.T) {
		const src = "sec-t-steprev-eas"
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-steprev-eas', 'ENTITY', 'ISSUED_BY', '2026-01-01', 'test', 'SEED_LOAD', 'original open', 'test')`, src)
		if err != nil {
			t.Fatal(err)
		}

		edgePv := correctionVersion(ctx, t, pool, "sec_edge")
		_, err = pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, processing_version, `+secstoreSpine+`)
			VALUES ('sec-t-steprev-eas', 'SECURITY', 'em-t-steprev-eas', 'ENTITY', 'ISSUED_BY', '2026-01-01', '2026-06-01', %d, 'test', 'RESTATEMENT', 'correction narrows window', 'test')`, edgePv))
		if err != nil {
			t.Fatal(err)
		}

		var count int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM sec_edge_as_of('2026-07-01'::date)
			WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`, src).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("got count=%d, want 0 — pv-N correction narrowed valid_to to Jun; Jul must be excluded (resolve before window)", count)
		}
	})
}

// TestSecStorePlanShapeEdgeSrcIdx checks that sec_edge_src_idx is present in plans that
// traverse edges by source.
func TestSecStorePlanShapeEdgeSrcIdx(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	// A query that would use sec_edge_src_idx: lookup by src_id + rel_type with ordering
	// that matches the index (valid_from DESC, processing_version DESC).
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "SET LOCAL enable_sort = off"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SET LOCAL enable_bitmapscan = off"); err != nil {
		t.Fatal(err)
	}

	var planJSON string
	if err := tx.QueryRow(ctx, `
		EXPLAIN (FORMAT JSON)
		SELECT * FROM sec_edge
		WHERE src_id = 'sec-dummy' AND rel_type = 'ISSUED_BY'
		ORDER BY valid_from DESC, processing_version DESC`).Scan(&planJSON); err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}

	if !strings.Contains(planJSON, "sec_edge_src_idx") {
		t.Errorf("plan does not reference sec_edge_src_idx\nplan: %s", planJSON)
	}
	sortCount := strings.Count(planJSON, `"Node Type": "Sort"`) + strings.Count(planJSON, `"Node Type": "Incremental Sort"`)
	if sortCount > 0 {
		t.Errorf("plan contains %d sort nodes — the index should provide the order\nplan: %s", sortCount, planJSON)
	}
}

// TestSecStoreSupersessionChainThreeLinks verifies that a 3-link correction
// chain (pv=0 → pv=1 → pv=2) resolves to the latest correction across all
// read objects, and that a forked chain (two independent corrections of the
// same original) also resolves to the highest processing_version.
func TestSecStoreSupersessionChainThreeLinks(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const chainNodeID = "em-t-chain3"
	const chainEdgeSrc = "sec-t-chain3-src"
	const chainEdgeDst = "em-t-chain3-dst"

	// pv=0: original
	_, err := pool.Exec(ctx, `
		INSERT INTO sec_node (id, record_type, status, valid_from, processing_version, `+secstoreSpine+`)
		VALUES ($1, 'ENTITY', 'ACTIVE', '2026-01-01', 0, 'test', 'SEED_LOAD', 'pv0 original', 'test')`, chainNodeID)
	if err != nil {
		t.Fatalf("insert node pv0: %v", err)
	}
	// pv=1: first correction
	_, err = pool.Exec(ctx, `
		INSERT INTO sec_node (id, record_type, status, valid_from, processing_version, `+secstoreSpine+`)
		VALUES ($1, 'ENTITY', 'ACTIVE', '2026-01-01', 1, 'test', 'RESTATEMENT', 'pv1 first correction', 'test')`, chainNodeID)
	if err != nil {
		t.Fatalf("insert node pv1: %v", err)
	}
	// pv=2: second correction
	_, err = pool.Exec(ctx, `
		INSERT INTO sec_node (id, record_type, status, valid_from, processing_version, `+secstoreSpine+`)
		VALUES ($1, 'ENTITY', 'INACTIVE', '2026-01-01', 2, 'test', 'RESTATEMENT', 'pv2 second correction', 'test')`, chainNodeID)
	if err != nil {
		t.Fatalf("insert node pv2: %v", err)
	}

	// Same 3-link chain for edges
	_, err = pool.Exec(ctx, `
		INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, processing_version, `+secstoreSpine+`)
		VALUES ($1, 'SECURITY', $2, 'ENTITY', 'ISSUED_BY', '2026-01-01', 0, 'test', 'SEED_LOAD', 'edge pv0', 'test')`, chainEdgeSrc, chainEdgeDst)
	if err != nil {
		t.Fatalf("insert edge pv0: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, processing_version, `+secstoreSpine+`)
		VALUES ($1, 'SECURITY', $2, 'ENTITY', 'ISSUED_BY', '2026-01-01', 1, 'test', 'RESTATEMENT', 'edge pv1', 'test')`, chainEdgeSrc, chainEdgeDst)
	if err != nil {
		t.Fatalf("insert edge pv1: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, processing_version, `+secstoreSpine+`)
		VALUES ($1, 'SECURITY', $2, 'ENTITY', 'ISSUED_BY', '2026-01-01', 2, 'test', 'RESTATEMENT', 'edge pv2', 'test')`, chainEdgeSrc, chainEdgeDst)
	if err != nil {
		t.Fatalf("insert edge pv2: %v", err)
	}

	t.Run("node_current_returns_pv2", func(t *testing.T) {
		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_node_current WHERE id = $1`, chainNodeID).Scan(&pv); err != nil {
			t.Fatalf("sec_node_current: %v", err)
		}
		if pv != 2 {
			t.Fatalf("got processing_version=%d, want 2", pv)
		}
	})

	t.Run("node_as_of_returns_pv2", func(t *testing.T) {
		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_node_as_of('2026-03-01'::date) WHERE id = $1`, chainNodeID).Scan(&pv); err != nil {
			t.Fatalf("sec_node_as_of: %v", err)
		}
		if pv != 2 {
			t.Fatalf("got processing_version=%d, want 2", pv)
		}
	})

	t.Run("node_as_of_kind_returns_pv2", func(t *testing.T) {
		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_node_as_of_kind('2026-03-01'::date, 'ENTITY') WHERE id = $1`, chainNodeID).Scan(&pv); err != nil {
			t.Fatalf("sec_node_as_of_kind: %v", err)
		}
		if pv != 2 {
			t.Fatalf("got processing_version=%d, want 2", pv)
		}
	})

	t.Run("edge_current_returns_pv2", func(t *testing.T) {
		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_edge_current
			WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`, chainEdgeSrc).Scan(&pv); err != nil {
			t.Fatalf("sec_edge_current: %v", err)
		}
		if pv != 2 {
			t.Fatalf("got processing_version=%d, want 2", pv)
		}
	})

	t.Run("edge_as_of_returns_pv2", func(t *testing.T) {
		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_edge_as_of('2026-03-01'::date)
			WHERE src_id = $1 AND rel_type = 'ISSUED_BY'`, chainEdgeSrc).Scan(&pv); err != nil {
			t.Fatalf("sec_edge_as_of: %v", err)
		}
		if pv != 2 {
			t.Fatalf("got processing_version=%d, want 2", pv)
		}
	})

	t.Run("node_status_reflects_latest_correction", func(t *testing.T) {
		var status string
		if err := pool.QueryRow(ctx, `
			SELECT status FROM sec_node_current WHERE id = $1`, chainNodeID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "INACTIVE" {
			t.Fatalf("got status=%s, want INACTIVE (pv=2 changed status from ACTIVE)", status)
		}
	})
}

// TestSecStoreSupersessionFork verifies that when two independent corrections
// target the same (id, valid_from), the one with the higher processing_version wins.
func TestSecStoreSupersessionFork(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const forkNodeID = "em-t-fork"

	// pv=0: original
	_, err := pool.Exec(ctx, `
		INSERT INTO sec_node (id, record_type, status, valid_from, processing_version, `+secstoreSpine+`)
		VALUES ($1, 'ENTITY', 'ACTIVE', '2026-01-01', 0, 'test', 'SEED_LOAD', 'original', 'test')`, forkNodeID)
	if err != nil {
		t.Fatal(err)
	}

	// Two independent corrections: pv=3 and pv=5 (non-sequential)
	_, err = pool.Exec(ctx, `
		INSERT INTO sec_node (id, record_type, status, valid_from, processing_version, `+secstoreSpine+`)
		VALUES ($1, 'ENTITY', 'ACTIVE', '2026-01-01', 3, 'test', 'RESTATEMENT', 'correction A (pv=3)', 'test')`, forkNodeID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO sec_node (id, record_type, status, valid_from, processing_version, `+secstoreSpine+`)
		VALUES ($1, 'ENTITY', 'INACTIVE', '2026-01-01', 5, 'test', 'RESTATEMENT', 'correction B (pv=5)', 'test')`, forkNodeID)
	if err != nil {
		t.Fatal(err)
	}

	var status string
	if err := pool.QueryRow(ctx, `
		SELECT status FROM sec_node_current WHERE id = $1`, forkNodeID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "INACTIVE" {
		t.Fatalf("got status=%s, want INACTIVE (pv=5 must beat pv=3)", status)
	}

	var pv int
	if err := pool.QueryRow(ctx, `
		SELECT processing_version FROM sec_node_as_of('2026-06-01'::date) WHERE id = $1`, forkNodeID).Scan(&pv); err != nil {
		t.Fatal(err)
	}
	if pv != 5 {
		t.Fatalf("got processing_version=%d, want 5", pv)
	}
}
