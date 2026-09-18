//go:build integration

package migrator_test

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSecStoreSeedIsExactlyWhatTheMigrationClaims(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	t.Run("node_count_is_352", func(t *testing.T) {
		var count int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM sec_node WHERE actor = 'migration:20260904_120100'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 352 {
			t.Fatalf("got %d seed nodes, want 352", count)
		}
	})

	t.Run("edge_count_is_149", func(t *testing.T) {
		var count int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM sec_edge WHERE actor = 'migration:20260904_120100'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 149 {
			t.Fatalf("got %d seed edges, want 149", count)
		}
	})

	t.Run("every_seed_node_is_concept_seed_load_pv0_open", func(t *testing.T) {
		var violations int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM sec_node
			WHERE actor = 'migration:20260904_120100'
			  AND (record_type <> 'CONCEPT'
			    OR change_reason_code <> 'SEED_LOAD'
			    OR run_id IS NOT NULL
			    OR processing_version <> 0
			    OR valid_from <> '2026-08-26'
			    OR valid_to <> 'infinity')`).Scan(&violations); err != nil {
			t.Fatal(err)
		}
		if violations != 0 {
			t.Fatalf("%d seed nodes violate the contract (want CONCEPT, SEED_LOAD, NULL run_id, pv 0, 2026-08-26..infinity)", violations)
		}
	})

	t.Run("every_edge_endpoint_is_a_current_concept", func(t *testing.T) {
		var orphans int
		if err := pool.QueryRow(ctx, `
			WITH current_concepts AS (
				SELECT * FROM sec_node_current WHERE record_type = 'CONCEPT'
			)
			SELECT count(*) FROM sec_edge e
			WHERE e.actor = 'migration:20260904_120100'
			  AND (NOT EXISTS (SELECT 1 FROM current_concepts c WHERE c.id = e.src_id)
			    OR NOT EXISTS (SELECT 1 FROM current_concepts c WHERE c.id = e.dst_id))`).Scan(&orphans); err != nil {
			t.Fatal(err)
		}
		if orphans != 0 {
			t.Fatalf("%d seed edges reference a non-current or non-CONCEPT node", orphans)
		}
	})

	t.Run("all_seed_edges_are_narrower_than", func(t *testing.T) {
		var nonNT int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM sec_edge
			WHERE actor = 'migration:20260904_120100' AND rel_type <> 'NARROWER_THAN'`).Scan(&nonNT); err != nil {
			t.Fatal(err)
		}
		if nonNT != 0 {
			t.Fatalf("%d seed edges are not NARROWER_THAN", nonNT)
		}
	})

	t.Run("root_count_per_class", func(t *testing.T) {
		rows, err := pool.Query(ctx, `
			WITH roots AS (
				SELECT n.id, n.attrs->>'concept_class' AS cc
				FROM sec_node n
				WHERE n.actor = 'migration:20260904_120100'
				  AND NOT EXISTS (
					SELECT 1 FROM sec_edge e
					WHERE e.actor = 'migration:20260904_120100'
					  AND e.src_id = n.id AND e.rel_type = 'NARROWER_THAN')
			)
			SELECT cc, count(*) FROM roots GROUP BY cc ORDER BY cc`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		want := map[string]int{
			"counterparty_role":  14,
			"credit_rating":      23,
			"currency":           57,
			"entity_type":        17,
			"instrument_subtype": 0,
			"instrument_type":    1,
			"jurisdiction":       76,
			"sector":             15,
		}
		got := make(map[string]int)
		for rows.Next() {
			var cc string
			var count int
			if err := rows.Scan(&cc, &count); err != nil {
				t.Fatal(err)
			}
			got[cc] = count
		}

		for cc, w := range want {
			if w == 0 {
				continue
			}
			if g := got[cc]; g != w {
				t.Errorf("root count for %s: got %d, want %d", cc, g, w)
			}
		}
		if got["instrument_subtype"] != 0 {
			t.Errorf("instrument_subtype should have 0 roots, got %d", got["instrument_subtype"])
		}
	})

	t.Run("every_concept_class_in_vocabulary", func(t *testing.T) {
		var orphans int
		if err := pool.QueryRow(ctx, `
			SELECT count(DISTINCT n.cc) FROM (
				SELECT attrs->>'concept_class' AS cc FROM sec_node
				WHERE actor = 'migration:20260904_120100'
				  AND attrs->>'concept_class' IS NOT NULL
			) n
			WHERE n.cc NOT IN (
				SELECT concept_class FROM concept_class_vocabulary)`).Scan(&orphans); err != nil {
			t.Fatal(err)
		}
		if orphans != 0 {
			t.Fatalf("%d concept classes not in concept_class_vocabulary", orphans)
		}
	})

	t.Run("no_cycles", func(t *testing.T) {
		var cycles int
		if err := pool.QueryRow(ctx, `
			WITH RECURSIVE walk AS (
				SELECT src_id, dst_id, ARRAY[src_id] AS path
				FROM sec_edge WHERE actor = 'migration:20260904_120100' AND rel_type = 'NARROWER_THAN'
				UNION ALL
				SELECT e.src_id, e.dst_id, w.path || e.src_id
				FROM sec_edge e JOIN walk w ON e.src_id = w.dst_id
				WHERE e.actor = 'migration:20260904_120100' AND e.rel_type = 'NARROWER_THAN'
				  AND NOT e.src_id = ANY(w.path)
				  AND array_length(w.path, 1) < 20
			)
			SELECT count(*) FROM walk WHERE dst_id = ANY(path)`).Scan(&cycles); err != nil {
			t.Fatal(err)
		}
		if cycles != 0 {
			t.Fatalf("%d cycles found in NARROWER_THAN taxonomy", cycles)
		}
	})

	t.Run("no_multi_parent", func(t *testing.T) {
		var multiParent int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM (
				SELECT src_id, count(*) AS parents
				FROM sec_edge
				WHERE actor = 'migration:20260904_120100' AND rel_type = 'NARROWER_THAN'
				GROUP BY src_id
				HAVING count(*) > 1
			) t`).Scan(&multiParent); err != nil {
			t.Fatal(err)
		}
		if multiParent != 0 {
			t.Fatalf("%d concepts have multiple NARROWER_THAN parents", multiParent)
		}
	})

	t.Run("vocabularies_pinned_as_content", func(t *testing.T) {
		assertVocabContent(t, ctx, pool)
	})

	t.Run("501_distinct_hashes", func(t *testing.T) {
		var count int
		if err := pool.QueryRow(ctx, `
			SELECT count(DISTINCT content_hash)
			FROM (
				SELECT content_hash FROM sec_node WHERE actor = 'migration:20260904_120100'
				UNION ALL
				SELECT content_hash FROM sec_edge WHERE actor = 'migration:20260904_120100'
			) t`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 501 {
			t.Fatalf("got %d distinct hashes, want 501 (352 nodes + 149 edges)", count)
		}
	})

	t.Run("node_hashes_recompute", func(t *testing.T) {
		assertNodeHashesRecompute(t, ctx, pool)
	})

	t.Run("edge_hashes_recompute", func(t *testing.T) {
		assertEdgeHashesRecompute(t, ctx, pool)
	})
}

func assertVocabContent(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	var relTypeCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM rel_type_vocabulary WHERE maturity = 'ratified'`).Scan(&relTypeCount); err != nil {
		t.Fatal(err)
	}
	if relTypeCount != 13 {
		t.Errorf("ratified rel_types: got %d, want 13", relTypeCount)
	}

	var basisCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM weight_basis_vocabulary`).Scan(&basisCount); err != nil {
		t.Fatal(err)
	}
	if basisCount != 3 {
		t.Errorf("weight bases: got %d, want 3", basisCount)
	}

	var reasonCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM change_reason_vocabulary`).Scan(&reasonCount); err != nil {
		t.Fatal(err)
	}
	if reasonCount != 11 {
		t.Errorf("change reason codes: got %d, want 11", reasonCount)
	}

	var statusCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM node_status_vocabulary`).Scan(&statusCount); err != nil {
		t.Fatal(err)
	}
	if statusCount != 27 {
		t.Errorf("node status rows: got %d, want 27", statusCount)
	}

	// SPLIT_FROM must have cluster_key = '{ex_date}'
	var ck string
	if err := pool.QueryRow(ctx, `
		SELECT cluster_key::text FROM rel_type_vocabulary WHERE rel_type = 'SPLIT_FROM'`).Scan(&ck); err != nil {
		t.Fatal(err)
	}
	if ck != "{ex_date}" {
		t.Errorf("SPLIT_FROM cluster_key: got %s, want {ex_date}", ck)
	}

	// NARROWER_THAN cardinality is '1'
	var card string
	if err := pool.QueryRow(ctx, `
		SELECT cardinality FROM rel_type_vocabulary WHERE rel_type = 'NARROWER_THAN'`).Scan(&card); err != nil {
		t.Fatal(err)
	}
	if card != "1" {
		t.Errorf("NARROWER_THAN cardinality: got %s, want 1", card)
	}

	// RESTATEMENT requires_approval = true
	var reqApproval bool
	if err := pool.QueryRow(ctx, `
		SELECT requires_approval FROM change_reason_vocabulary WHERE code = 'RESTATEMENT'`).Scan(&reqApproval); err != nil {
		t.Fatal(err)
	}
	if !reqApproval {
		t.Error("RESTATEMENT.requires_approval must be true")
	}

	// ACTIVE/ENTITY is_terminal = false
	var isTerminal bool
	if err := pool.QueryRow(ctx, `
		SELECT is_terminal FROM node_status_vocabulary WHERE record_type = 'ENTITY' AND status = 'ACTIVE'`).Scan(&isTerminal); err != nil {
		t.Fatal(err)
	}
	if isTerminal {
		t.Error("ENTITY/ACTIVE.is_terminal must be false")
	}

	// MATURED is terminal
	if err := pool.QueryRow(ctx, `
		SELECT is_terminal FROM node_status_vocabulary WHERE record_type = 'SECURITY' AND status = 'MATURED'`).Scan(&isTerminal); err != nil {
		t.Fatal(err)
	}
	if !isTerminal {
		t.Error("SECURITY/MATURED.is_terminal must be true")
	}

	// The complete set of codes that require approval — a mutation flipping one true→false
	// in the seed is caught only if we pin the full set, not just one sample row.
	approvalRows, err := pool.Query(ctx, `
		SELECT code FROM change_reason_vocabulary WHERE requires_approval ORDER BY code`)
	if err != nil {
		t.Fatal(err)
	}
	defer approvalRows.Close()
	var gotApproval []string
	for approvalRows.Next() {
		var code string
		if err := approvalRows.Scan(&code); err != nil {
			t.Fatal(err)
		}
		gotApproval = append(gotApproval, code)
	}
	wantApproval := []string{"DEDUP_SUPERSEDE", "RECLASSIFICATION", "REPOINT", "RESTATEMENT", "RETRACTION"}
	if len(gotApproval) != len(wantApproval) {
		t.Fatalf("requires_approval codes: got %v, want %v", gotApproval, wantApproval)
	}
	for i := range wantApproval {
		if gotApproval[i] != wantApproval[i] {
			t.Errorf("requires_approval[%d]: got %s, want %s", i, gotApproval[i], wantApproval[i])
		}
	}

	// The complete set of terminal statuses — a mutation flipping one true→false is caught
	// only if we pin every (record_type, status) pair that is terminal.
	termRows, err := pool.Query(ctx, `
		SELECT record_type, status FROM node_status_vocabulary WHERE is_terminal ORDER BY record_type, status`)
	if err != nil {
		t.Fatal(err)
	}
	defer termRows.Close()
	type rtStatus struct{ rt, st string }
	var gotTerminal []rtStatus
	for termRows.Next() {
		var rt, st string
		if err := termRows.Scan(&rt, &st); err != nil {
			t.Fatal(err)
		}
		gotTerminal = append(gotTerminal, rtStatus{rt, st})
	}
	wantTerminal := []rtStatus{
		{"ACCOUNT", "CLOSED"},
		{"CONCEPT", "RETIRED"},
		{"CONCEPT", "SUPERSEDED"},
		{"ENTITY", "DISSOLVED"},
		{"ENTITY", "MERGED"},
		{"ENTITY", "SUPERSEDED"},
		{"SECURITY", "CONVERTED"},
		{"SECURITY", "EXPIRED"},
		{"SECURITY", "MATURED"},
		{"SECURITY", "MERGED"},
		{"SECURITY", "REDEEMED"},
		{"SECURITY", "RETIRED"},
		{"SOURCE", "DECOMMISSIONED"},
		{"SOURCE", "SUPERSEDED"},
	}
	if len(gotTerminal) != len(wantTerminal) {
		t.Fatalf("terminal statuses: got %d, want %d — %v", len(gotTerminal), len(wantTerminal), gotTerminal)
	}
	for i := range wantTerminal {
		if gotTerminal[i] != wantTerminal[i] {
			t.Errorf("terminal[%d]: got %s/%s, want %s/%s", i, gotTerminal[i].rt, gotTerminal[i].st, wantTerminal[i].rt, wantTerminal[i].st)
		}
	}
}

func assertNodeHashesRecompute(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	// Recompute using to_jsonb(row) minus platform-assigned fields — same approach the
	// migration uses. A future column addition would change to_jsonb of stored rows,
	// but that is the migration's responsibility to handle, not this test's.
	rows, err := pool.Query(ctx, `
		SELECT
			n.record_id,
			n.content_hash,
			sha256(convert_to((
				to_jsonb(n.*) - 'record_id' - 'ingest_xid' - 'ingested_at' - 'content_hash' - 'supersedes_record_id'
			)::text, 'UTF8')) AS recomputed
		FROM sec_node n
		WHERE n.actor = 'migration:20260904_120100'
		  AND n.supersedes_record_id IS NULL`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var checked int
	var mismatches int
	for rows.Next() {
		var recordID int64
		var stored, recomputed []byte
		if err := rows.Scan(&recordID, &stored, &recomputed); err != nil {
			t.Fatal(err)
		}
		checked++
		if hex.EncodeToString(stored) != hex.EncodeToString(recomputed) {
			mismatches++
			if mismatches <= 3 {
				t.Errorf("record_id %d: stored %s, recomputed %s",
					recordID, hex.EncodeToString(stored), hex.EncodeToString(recomputed))
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if checked == 0 {
		t.Fatal("zero seed node rows matched — fixture missing or predicate wrong")
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d seed node hashes do not recompute", mismatches, checked)
	}
}

func assertEdgeHashesRecompute(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT
			e.record_id,
			e.content_hash,
			sha256(convert_to((
				to_jsonb(e.*) - 'record_id' - 'ingest_xid' - 'ingested_at' - 'content_hash' - 'edge_id' - 'supersedes_record_id'
			)::text, 'UTF8')) AS recomputed
		FROM sec_edge e
		WHERE e.actor = 'migration:20260904_120100'
		  AND e.supersedes_record_id IS NULL`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var checked int
	var mismatches int
	for rows.Next() {
		var recordID int64
		var stored, recomputed []byte
		if err := rows.Scan(&recordID, &stored, &recomputed); err != nil {
			t.Fatal(err)
		}
		checked++
		if hex.EncodeToString(stored) != hex.EncodeToString(recomputed) {
			mismatches++
			if mismatches <= 3 {
				t.Errorf("record_id %d: stored %s, recomputed %s",
					recordID, hex.EncodeToString(stored), hex.EncodeToString(recomputed))
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if checked == 0 {
		t.Fatal("zero seed edge rows matched — fixture missing or predicate wrong")
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d seed edge hashes do not recompute", mismatches, checked)
	}
}
