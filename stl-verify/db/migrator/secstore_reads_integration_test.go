//go:build integration

package migrator_test

import (
	"context"
	"fmt"
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
			t.Skip("window-first and two-step agree on this fixture — the superseded row is outside the window")
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

	t.Run("pv0_append_after_correction_does_not_win", func(t *testing.T) {
		// A pv-0 append with a newer ingest_xid must not beat the pv-N restatement.
		// Use a different valid_to so the PK (id, pv, valid_from, valid_to) does not collide
		// with the existing pv-0 Jun-open row; the resolution groups on (id, valid_from),
		// so both rows compete and pvN must still win.
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, processing_version, `+secstoreSpine+`)
			VALUES ('em-t-reads', 'ENTITY', 'ACTIVE', '2026-06-01', '2026-12-01', 0, 'test', 'SEED_LOAD', 'late pv-0 append', 'test')`)
		if err != nil {
			t.Fatalf("late pv-0 append: %v", err)
		}

		var pv int
		if err := pool.QueryRow(ctx, `
			SELECT processing_version FROM sec_node_current WHERE id = $1`, nodeID).Scan(&pv); err != nil {
			t.Fatalf("sec_node_current after late pv-0: %v", err)
		}
		if pv != pvN {
			t.Fatalf("got pv=%d, want %d — a pv-0 append must not beat a correction", pv, pvN)
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

				if !containsSubstring(planJSON, tc.expectedIndex) {
					t.Errorf("plan does not reference %s\nplan: %s", tc.expectedIndex, planJSON)
				}
				if containsSubstring(planJSON, `"Node Type": "Sort"`) {
					t.Errorf("plan contains a Sort node — the index direction mutations would survive")
				}
			})
		}
	})
}

func containsSubstring(haystack, needle string) bool {
	return len(haystack) > 0 && len(needle) > 0 && indexOf(haystack, needle) >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
