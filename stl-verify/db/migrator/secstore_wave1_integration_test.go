//go:build integration

package migrator_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The VEC-617 acceptance tests for combined-master wave 1 (ADR-0007, #652): the four
// behaviours the migrations claim and that nothing else in CI would catch if they broke.
//
// They live in db/migrator, alongside append_only_grants_integration_test.go, because they
// need a database whose migration order this package controls, and because the append-only
// assertions read the catalogue the same way that file does. The package is already listed
// in ci/integration-shards/1.txt, so no shard manifest changes.
//
// A note that applies to all four: the harness migrates as the container's BOOTSTRAP
// SUPERUSER (testutil.StartTimescaleDBForMain sets POSTGRES_USER=test), and a superuser
// bypasses privilege checks entirely. That is the #574 trap these migrations were reviewed
// against, and it means has_table_privilege() on the table OWNER reports true here no
// matter what the migration revoked. Owner-side assertions therefore read the ACL itself
// through aclexplode(), which records what production will enforce; role-side assertions
// use the NOLOGIN group role and the real login user, where the checks do apply.

const secstoreSpine = `actor, change_reason_code, change_reason, source_system`

// insertNode appends one sec_node row. valid_to is passed as SQL rather than a parameter so a
// test can write 'infinity' (the open-window sentinel) without pgx date-infinity handling.
func insertNode(ctx context.Context, t *testing.T, pool *pgxpool.Pool, id, status, validFrom, validToSQL, reason string) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
		VALUES ($1, 'ENTITY', $2, $3::date, `+validToSQL+`, 'test', 'SEED_LOAD', $4, 'test')`,
		id, status, validFrom, reason)
	if err != nil {
		t.Fatalf("insert %s (%s .. %s): %v", id, validFrom, validToSQL, err)
	}
}

// TestSecStoreClosingRowSupersedesRatherThanResurrects is acceptance item 1.
//
// Close-and-open appends a row with the SAME (id, valid_from) as the row it closes, differing
// only in valid_to. Two things have to hold for that to be a data change rather than a
// correction: it must LAND at processing_version 0 (valid_to is in the primary key precisely so
// it does not collide — ADR-0006 §3 reserves processing_version > 0 for correction runs, one
// allocation per ticket, which an issuer re-point is not), and the resolved reads must return
// the CLOSED row for a date inside that window, not the superseded open one.
//
// The second half is the ordering the ticket asks to be pinned: resolve the latest append per
// (id, valid_from) FIRST, then filter the valid-time window. The test also runs the inverted
// order inline and asserts the two disagree — filtering the window first leaves the superseded
// open row a live candidate, so a read written that way silently resurrects it.
//
// The tombstone subtests cover the other half of supersession: a zero-length window
// (valid_to = valid_from) withdraws THAT WINDOW from every resolved read while its history
// stays in the base table, which is how ADR-0007 §3's retraction and a valid-time amendment
// work. Scope matters and is asserted in both directions — one tombstone clears a one-window
// record entirely, and leaves a closed-and-reopened record's later window live and current.
func TestSecStoreClosingRowSupersedesRatherThanResurrects(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const id = "em-t-supersede"
	insertNode(ctx, t, pool, id, "ACTIVE", "2026-01-01", "'infinity'", "open the first window")
	insertNode(ctx, t, pool, id, "ACTIVE", "2026-01-01", "'2026-06-01'", "close it")
	insertNode(ctx, t, pool, id, "INACTIVE", "2026-06-01", "'infinity'", "open the next window")

	t.Run("close_and_open_lands_at_processing_version_0", func(t *testing.T) {
		// min and max, not count(DISTINCT): the assertion is that the version is 0, and
		// "they all agree" is satisfied by any uniform value.
		var rows, lo, hi int
		if err := pool.QueryRow(ctx, `
			SELECT count(*), min(processing_version), max(processing_version)
			FROM sec_node WHERE id = $1`, id,
		).Scan(&rows, &lo, &hi); err != nil {
			t.Fatalf("count rows: %v", err)
		}
		if rows != 3 {
			t.Errorf("got %d rows, want 3 — a closing row that collides on the PK is the failure this guards", rows)
		}
		if lo != 0 || hi != 0 {
			t.Errorf("processing_version spans [%d, %d], want [0, 0]: a close is a valid-time change and must not burn a correction version (ADR-0006 §3)", lo, hi)
		}
	})

	t.Run("as_of_inside_the_closed_window_resolves_the_closed_row", func(t *testing.T) {
		var validTo, status string
		if err := pool.QueryRow(ctx, `
			SELECT valid_to::text, status FROM sec_node_as_of('2026-03-01') WHERE id = $1`, id,
		).Scan(&validTo, &status); err != nil {
			t.Fatalf("sec_node_as_of(2026-03-01): %v", err)
		}
		if validTo != "2026-06-01" {
			t.Errorf("as_of resolved valid_to=%s, want 2026-06-01 — the open row it superseded was returned instead", validTo)
		}
		if status != "ACTIVE" {
			t.Errorf("as_of resolved status=%s, want ACTIVE", status)
		}
	})

	t.Run("window_first_ordering_would_resurrect_the_superseded_row", func(t *testing.T) {
		// The inverted read: filter the valid-time window before resolving supersession. Both
		// the closed row and the open row it superseded satisfy the window, so the superseded
		// row is still a candidate and which one comes back is left to the planner. The shipped
		// read returns exactly one row, and it is the closed one (asserted above).
		var candidates, resurrected int
		if err := pool.QueryRow(ctx, `
			SELECT count(*), count(*) FILTER (WHERE valid_to = 'infinity')
			FROM sec_node
			WHERE id = $1 AND valid_from <= '2026-03-01'::date AND '2026-03-01'::date < valid_to`, id,
		).Scan(&candidates, &resurrected); err != nil {
			t.Fatalf("window-first candidates: %v", err)
		}
		if candidates != 2 || resurrected != 1 {
			t.Errorf("window-first left %d candidates (%d of them the superseded open row), want 2 and 1: the two orderings must demonstrably differ, which is why the views resolve the version first",
				candidates, resurrected)
		}

		var viaView int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sec_node_as_of('2026-03-01') WHERE id = $1`, id).Scan(&viaView); err != nil {
			t.Fatalf("count via as_of: %v", err)
		}
		if viaView != 1 {
			t.Errorf("sec_node_as_of returned %d rows for one logical record, want 1", viaView)
		}
	})

	t.Run("current_returns_the_open_second_window", func(t *testing.T) {
		var validFrom, status string
		if err := pool.QueryRow(ctx, `
			SELECT valid_from::text, status FROM sec_node_current WHERE id = $1`, id,
		).Scan(&validFrom, &status); err != nil {
			t.Fatalf("sec_node_current: %v", err)
		}
		if validFrom != "2026-06-01" || status != "INACTIVE" {
			t.Errorf("_current resolved (%s, %s), want (2026-06-01, INACTIVE)", validFrom, status)
		}
	})

	// A tombstone withdraws ONE WINDOW, and this is where that scope gets pinned. The
	// single-window subtest below passes under either reading, which is exactly why the
	// multi-window case is asserted first: a record that has been closed and reopened takes one
	// tombstone per window, and a curator who tombstones only the first leaves the second live
	// and current. Withdrawing a logical record in one append is VEC-622's.
	t.Run("tombstone_withdraws_one_window_not_the_whole_record", func(t *testing.T) {
		// The same three appends as the parent test: Jan open, Jan closed to Jun, Jun open.
		const multi = "em-t-multiwindow"
		insertNode(ctx, t, pool, multi, "ACTIVE", "2026-01-01", "'infinity'", "open the first window")
		insertNode(ctx, t, pool, multi, "ACTIVE", "2026-01-01", "'2026-06-01'", "close it")
		insertNode(ctx, t, pool, multi, "INACTIVE", "2026-06-01", "'infinity'", "open the next window")
		if _, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, actor,
			                      change_reason_code, change_reason, approved_by,
			                      supersedes_record_id, source_system)
			SELECT $1, 'ENTITY', 'ACTIVE', '2026-01-01', '2026-01-01', 'test',
			       'RETRACTION', 'tombstone the first window only', 'approver', record_id, 'test'
			FROM sec_node
			WHERE id = $1 AND valid_from = '2026-01-01' AND valid_to = '2026-06-01'`, multi); err != nil {
			t.Fatalf("tombstone the first window: %v", err)
		}

		var insideRetracted, afterRetracted, current int
		if err := pool.QueryRow(ctx, `
			SELECT (SELECT count(*) FROM sec_node_as_of('2026-03-01') WHERE id = $1),
			       (SELECT count(*) FROM sec_node_as_of('2026-08-01') WHERE id = $1),
			       (SELECT count(*) FROM sec_node_current WHERE id = $1)`, multi,
		).Scan(&insideRetracted, &afterRetracted, &current); err != nil {
			t.Fatalf("resolved reads after the single tombstone: %v", err)
		}
		if insideRetracted != 0 {
			t.Errorf("as_of inside the tombstoned window returned %d rows, want 0", insideRetracted)
		}
		if afterRetracted != 1 || current != 1 {
			t.Errorf("as_of after the tombstoned window returned %d rows and _current %d, want 1 and 1: a tombstone must not withdraw a window it does not name",
				afterRetracted, current)
		}
	})

	t.Run("tombstone_withdraws_a_single_window_record_entirely", func(t *testing.T) {
		const victim = "em-t-tombstone"
		insertNode(ctx, t, pool, victim, "ACTIVE", "2026-01-01", "'infinity'", "should never have existed")
		// A zero-length window, carrying the reason code and the record it supersedes.
		if _, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, actor,
			                      change_reason_code, change_reason, approved_by,
			                      supersedes_record_id, source_system)
			SELECT $1, 'ENTITY', 'ACTIVE', '2026-01-01', '2026-01-01', 'test',
			       'RETRACTION', 'tombstone', 'approver', record_id, 'test'
			FROM sec_node WHERE id = $1 AND change_reason_code = 'SEED_LOAD'`, victim); err != nil {
			t.Fatalf("append tombstone: %v", err)
		}

		var live int
		if err := pool.QueryRow(ctx, `
			SELECT (SELECT count(*) FROM sec_node_current WHERE id = $1)
			     + (SELECT count(*) FROM sec_node_as_of('2026-01-01') WHERE id = $1)
			     + (SELECT count(*) FROM sec_node_as_of('2027-01-01') WHERE id = $1)`, victim,
		).Scan(&live); err != nil {
			t.Fatalf("resolved reads after tombstone: %v", err)
		}
		if live != 0 {
			t.Errorf("tombstoned record still visible in %d resolved read(s), want 0", live)
		}

		var history, chained int
		if err := pool.QueryRow(ctx, `
			SELECT (SELECT count(*) FROM sec_node WHERE id = $1),
			       (SELECT count(*) FROM sec_node t JOIN sec_node r ON r.record_id = t.supersedes_record_id
			          WHERE t.id = $1)`, victim,
		).Scan(&history, &chained); err != nil {
			t.Fatalf("history after tombstone: %v", err)
		}
		if history != 2 || chained != 1 {
			t.Errorf("after tombstone: %d rows in the base table and %d resolvable supersession link(s), want 2 and 1 — a retraction hides the record, it never deletes it (AR-1.4)",
				history, chained)
		}
	})
}

// TestSecStoreRejectsAnIllegalRelTypeTriple is acceptance item 2, as far as wave 1 can carry it.
//
// The engine half is real and asserted: an endpoint kind outside the closed record_type set and
// a rel_type outside the governed vocabulary are both refused at the write boundary.
//
// The triple itself — (rel_type, src_kind, dst_kind) against rel_type_vocabulary.src_kinds /
// dst_kinds — lives in two array columns that no FK or CHECK on sec_edge can consult, so
// ADR-0007 §3 assigns it to the loader/validator (GQ-11), which is VEC-622's. The last subtest
// pins today's boundary: the vocabulary holds everything needed to decide the triple, and the
// write is accepted.
//
// When VEC-622 lands the validator — or when the vocabulary grows the legal-pairs table that
// would turn this into a composite FK, which is also what SAME_AS and SUPERSEDES need before
// they ratify — invert that subtest. It is deliberately written so it fails the moment the gap
// closes, because a skipped test would go quiet instead.
func TestSecStoreRejectsAnIllegalRelTypeTriple(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	// Every case gets its own src_id. Sharing one meant the cases shared a primary key, so if
	// sec_edge_src_kind_chk were dropped the first case would land and the last would fail on a
	// duplicate key — reported as "the GQ-11 gap has closed" when a CHECK had actually been lost.
	insertEdge := func(caseID, relType, srcID, srcKind, dstID, dstKind string) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ($2, $3, $4, $5, $1, '2026-01-01', 'test', 'SEED_LOAD', $6, 'test')`,
			relType, srcID, srcKind, dstID, dstKind, "triple case "+caseID)
		return err
	}

	t.Run("endpoint_kind_outside_the_record_type_set_is_rejected", func(t *testing.T) {
		err := insertEdge("kind", "ISSUED_BY", "sec-t-triple-a", "NOT_A_KIND", "em-t-triple-a", "ENTITY")
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
			t.Fatalf("insert with src_kind NOT_A_KIND failed with %v, want SQLSTATE 23514 (check_violation)", err)
		}
	})

	t.Run("rel_type_outside_the_governed_vocabulary_is_rejected", func(t *testing.T) {
		err := insertEdge("reltype", "INVENTED_BY", "sec-t-triple-b", "SECURITY", "em-t-triple-b", "ENTITY")
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23503" {
			t.Fatalf("insert with rel_type INVENTED_BY failed with %v, want SQLSTATE 23503 (foreign_key_violation) — GQ-01", err)
		}
	})

	t.Run("endpoint_kind_contradicting_its_own_id_prefix_is_rejected", func(t *testing.T) {
		// sec_node_id_prefix_chk makes record_type a function of the prefix, so this much is
		// single-row checkable even though endpoint EXISTENCE is not.
		for _, c := range []struct{ name, srcID, srcKind, dstID, dstKind string }{
			{"em- declared SECURITY", "em-90001", "SECURITY", "sec-t-x", "SECURITY"},
			{"unprefixed src", "total-garbage", "ENTITY", "sec-t-x", "SECURITY"},
			{"empty dst", "sec-t-y", "SECURITY", "", "SECURITY"},
		} {
			err := insertEdge(c.name, "HAS_UNDERLYING", c.srcID, c.srcKind, c.dstID, c.dstKind)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
				t.Errorf("%s: failed with %v, want SQLSTATE 23514 — a declared kind must agree with its endpoint's prefix", c.name, err)
			}
		}
	})

	t.Run("the_vocabulary_can_decide_the_triple", func(t *testing.T) {
		// ISSUED_BY is SECURITY -> ENTITY. A SECURITY -> CONCEPT edge of that type is illegal,
		// and this is the query that says so — the one a validator resolves per write.
		var legal bool
		if err := pool.QueryRow(ctx, `
			SELECT 'CONCEPT' = ANY (dst_kinds) FROM rel_type_vocabulary WHERE rel_type = 'ISSUED_BY'`,
		).Scan(&legal); err != nil {
			t.Fatalf("read endpoint rule for ISSUED_BY: %v", err)
		}
		if legal {
			t.Error("rel_type_vocabulary says ISSUED_BY may point at a CONCEPT; the seeded rule is SECURITY -> ENTITY (ADR-0007 §5)")
		}
	})

	t.Run("illegal_triple_is_not_yet_rejected_at_the_write_boundary", func(t *testing.T) {
		// The documented gap, asserted so it cannot be forgotten. INVERT THIS SUBTEST when
		// VEC-622's validator or a legal-pairs FK starts refusing the write.
		if err := insertEdge("triple", "ISSUED_BY", "sec-t-triple-c", "SECURITY", "concept-t-triple-c", "CONCEPT"); err != nil {
			t.Fatalf("SECURITY -> CONCEPT ISSUED_BY was refused with %v — if that is deliberate, this subtest is now inverted: assert the rejection and delete this comment (GQ-11, VEC-622)", err)
		}
		t.Log("known gap: an illegal (rel_type, src_kind, dst_kind) triple lands. Legality is two array columns on the vocabulary, which no CHECK or FK on sec_edge can read, so ADR-0007 §3 assigns GQ-11 to the validator (VEC-622)")
	})
}

// TestSecStoreSingleValuedRepointPassesTheWriteAndIsCaughtByTheDQRule is acceptance item 3.
//
// ISSUED_BY is single-valued per ADR-0007 §5, and the rule runs over RESOLVED CURRENT STATE
// (GQ-20) because a re-point always time-overlaps the edge it supersedes. So a badly executed
// re-point — open the new issuer without closing the old — passes the write and shows up in the
// check.
//
// Both halves are asserted here. The check itself is GQ-20's query against sec_edge_current;
// VEC-619 will surface it as a DQ view over the pivot, and when it does this test should read
// that view instead of spelling the query out. What must not change is the shape: two current
// edges for one security is a data-quality finding, not a rejected write.
func TestSecStoreSingleValuedRepointPassesTheWriteAndIsCaughtByTheDQRule(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	insertIssuer := func(t *testing.T, src, dst, validFrom, validToSQL, reason string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to,
			                      actor, change_reason_code, change_reason, approved_by, source_system)
			VALUES ($1, 'SECURITY', $2, 'ENTITY', 'ISSUED_BY', $3::date, `+validToSQL+`,
			        'test', 'REPOINT', $4, 'approver', 'test')`, src, dst, validFrom, reason); err != nil {
			t.Fatalf("insert ISSUED_BY %s -> %s: %v", src, dst, err)
		}
	}

	// GQ-20 over resolved current state: securities carrying more than one current issuer.
	const gq20 = `
		SELECT count(*) FROM (
			SELECT src_id FROM sec_edge_current
			WHERE rel_type = 'ISSUED_BY' AND src_id = $1
			GROUP BY src_id HAVING count(*) > 1
		) offenders`

	t.Run("a_botched_repoint_passes_the_write", func(t *testing.T) {
		const sec = "sec-t-botched"
		insertIssuer(t, sec, "em-t-issuer-old", "2026-01-01", "'infinity'", "original issuer")
		// The mistake: a second open issuer, without closing the first.
		insertIssuer(t, sec, "em-t-issuer-new", "2026-06-01", "'infinity'", "re-point without closing")

		var current int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sec_edge_current WHERE rel_type = 'ISSUED_BY' AND src_id = $1`, sec).Scan(&current); err != nil {
			t.Fatalf("count current issuers: %v", err)
		}
		if current != 2 {
			t.Fatalf("resolved %d current issuers, want 2 — cardinality must not be enforced at write time (a re-point always overlaps the edge it supersedes)", current)
		}

		var offenders int
		if err := pool.QueryRow(ctx, gq20, sec).Scan(&offenders); err != nil {
			t.Fatalf("GQ-20 query: %v", err)
		}
		if offenders != 1 {
			t.Errorf("GQ-20 flagged %d offenders, want 1: a single-valued type with two current edges is exactly what the DQ rule exists to catch", offenders)
		}
	})

	t.Run("a_correct_repoint_is_not_flagged", func(t *testing.T) {
		const sec = "sec-t-correct"
		insertIssuer(t, sec, "em-t-issuer-old", "2026-01-01", "'infinity'", "original issuer")
		insertIssuer(t, sec, "em-t-issuer-old", "2026-01-01", "'2026-06-01'", "close the old edge")
		insertIssuer(t, sec, "em-t-issuer-new", "2026-06-01", "'infinity'", "open the new one")

		var current int
		var dst string
		if err := pool.QueryRow(ctx, `
			SELECT count(*), min(dst_id) FROM sec_edge_current WHERE rel_type = 'ISSUED_BY' AND src_id = $1`, sec,
		).Scan(&current, &dst); err != nil {
			t.Fatalf("count current issuers: %v", err)
		}
		if current != 1 || dst != "em-t-issuer-new" {
			t.Errorf("after a correct re-point: %d current issuer(s) (%s), want 1 (em-t-issuer-new)", current, dst)
		}

		var offenders int
		if err := pool.QueryRow(ctx, gq20, sec).Scan(&offenders); err != nil {
			t.Fatalf("GQ-20 query: %v", err)
		}
		if offenders != 0 {
			t.Errorf("GQ-20 flagged a correctly re-pointed security; the rule would false-positive on every close-and-open")
		}
	})
}

// TestSecStoreWave1IsAppendOnlyUnderTheRealRoles is acceptance item 4.
//
// Wave 1 enforces append-only by table class, and the two classes differ on purpose:
//
//   - sec_node / sec_edge: every mutation privilege revoked INCLUDING the owner's, the
//     position_state pattern. Nothing FKs these tables, so the owner-side revoke cannot break
//     an integrity probe.
//   - the vocabularies: FK parents, so the owner KEEPS UPDATE — the FK integrity probe
//     (SELECT ... FOR KEY SHARE) executes as the parent's owner and requires it (#574,
//     20260714_160000, invisible under a superuser). append-only there is the
//     reference_table_immutable() trigger.
//
// So this test asserts four different things, and the last one is the regression guard that
// matters most: an INSERT into sec_edge as the real login role must still pass the FK probes
// against the revoked vocabulary tables. Get the revoke wrong and every append fails in
// production while CI stays green.
func TestSecStoreWave1IsAppendOnlyUnderTheRealRoles(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	stores := []string{"sec_node", "sec_edge"}
	vocabularies := []string{
		"rel_type_vocabulary", "weight_basis_vocabulary", "change_reason_vocabulary",
		"concept_class_vocabulary", "node_status_vocabulary",
	}

	t.Run("app_role_keeps_select_insert_and_holds_no_update_delete", func(t *testing.T) {
		for _, table := range append(append([]string{}, stores...), vocabularies...) {
			var canSelect, canInsert, canUpdate, canDelete bool
			if err := pool.QueryRow(ctx, `
				SELECT has_table_privilege('stl_readwrite', $1, 'SELECT'),
				       has_table_privilege('stl_readwrite', $1, 'INSERT'),
				       has_table_privilege('stl_readwrite', $1, 'UPDATE'),
				       has_table_privilege('stl_readwrite', $1, 'DELETE')`, table,
			).Scan(&canSelect, &canInsert, &canUpdate, &canDelete); err != nil {
				t.Fatalf("read grants for %s: %v", table, err)
			}
			if !canSelect {
				t.Errorf("%s: stl_readwrite must keep SELECT", table)
			}
			if canUpdate || canDelete {
				t.Errorf("%s: stl_readwrite holds update=%v delete=%v, want neither — ALTER DEFAULT PRIVILEGES grants full DML on every migrator-owned table, so the REVOKE is load-bearing",
					table, canUpdate, canDelete)
			}
			var canTruncate bool
			if err := pool.QueryRow(ctx, `SELECT has_table_privilege('stl_readwrite', $1, 'TRUNCATE')`, table).Scan(&canTruncate); err != nil {
				t.Fatalf("read TRUNCATE grant for %s: %v", table, err)
			}
			if canTruncate {
				t.Errorf("%s: stl_readwrite holds TRUNCATE", table)
			}
			if isStore := table == "sec_node" || table == "sec_edge"; isStore && !canInsert {
				t.Errorf("%s: stl_readwrite must keep INSERT — the stores are append-only, not read-only", table)
			}
		}
	})

	// The owner-side assertions read the ACL rather than has_table_privilege(), because the
	// owner here is the harness superuser and privilege checks report true for a superuser
	// whatever the ACL says. aclexplode() is what the migration actually changed, and it is
	// what production (a non-superuser stl_migrator) will enforce.
	//
	// coalesce(relacl, acldefault(...)) is load-bearing: relacl is NULL on a table whose
	// privileges were never touched, the owner implicitly holds everything, and
	// aclexplode(NULL) returns no rows — so reading relacl directly would report "the owner
	// holds nothing" for a table where the REVOKE never ran, which is precisely the failure
	// these subtests exist to catch. acldefault('r', relowner) materialises the implicit
	// default so a missing revoke reads as the privilege still being held.
	ownerHas := func(t *testing.T, table, priv string) bool {
		t.Helper()
		var held bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_class c,
				     aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a
				WHERE c.oid = $1::regclass
				  AND a.grantee = c.relowner
				  AND a.privilege_type = $2
			)`, table, priv).Scan(&held); err != nil {
			t.Fatalf("read %s ACL for %s: %v", table, priv, err)
		}
		return held
	}

	t.Run("owner_holds_no_update_delete_or_truncate_on_the_stores", func(t *testing.T) {
		for _, table := range stores {
			if ownerHas(t, table, "UPDATE") {
				t.Errorf("%s: the owner still holds UPDATE in the ACL — the full revoke (position_state pattern) did not land", table)
			}
			if ownerHas(t, table, "DELETE") {
				t.Errorf("%s: the owner still holds DELETE in the ACL", table)
			}
			// TRUNCATE is revoked by the migration and was asserted nowhere, so dropping the word
			// from the REVOKE left the whole suite green while stl_migrator — the role that runs
			// every migration — could erase all history in one statement. No row trigger can catch
			// it either: TRUNCATE fires none.
			if ownerHas(t, table, "TRUNCATE") {
				t.Errorf("%s: the owner still holds TRUNCATE in the ACL", table)
			}
		}
	})

	t.Run("owner_keeps_update_on_the_vocabularies_for_the_fk_probe", func(t *testing.T) {
		for _, table := range vocabularies {
			if !ownerHas(t, table, "UPDATE") {
				t.Errorf("%s: the owner lost UPDATE — the FK integrity probe runs as the parent's owner and needs it, so every INSERT into sec_node/sec_edge would fail under the prod roles (20260714_160000, #574)", table)
			}
			if ownerHas(t, table, "DELETE") {
				t.Errorf("%s: the owner still holds DELETE; append-only leaves no delete channel", table)
			}
			if ownerHas(t, table, "TRUNCATE") {
				t.Errorf("%s: the owner still holds TRUNCATE, which no row trigger can intercept", table)
			}
		}
	})

	t.Run("vocabulary_update_raises_the_immutability_trigger", func(t *testing.T) {
		// Not an ACL refusal: the owner holds UPDATE by design here, and the trigger is what
		// makes the table append-only. Row locks do not fire row triggers, so this does not
		// affect the FK probe asserted below.
		_, err := pool.Exec(ctx, `UPDATE rel_type_vocabulary SET description = description WHERE rel_type = 'ISSUED_BY'`)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
			t.Fatalf("UPDATE on rel_type_vocabulary failed with %v, want SQLSTATE P0001 from reference_table_immutable()", err)
		}
	})

	t.Run("the_login_role_cannot_update_but_can_still_append", func(t *testing.T) {
		appPool, err := pgxpool.New(ctx, loginRoleDSN(t, pool))
		if err != nil {
			t.Fatalf("connect as stl_read_write: %v", err)
		}
		defer appPool.Close()

		// A WHERE that matches nothing: privileges are checked at executor start, so the
		// refusal cannot be confused with a row-level effect.
		_, err = appPool.Exec(ctx, `UPDATE sec_node SET status = status WHERE id = 'nothing-matches'`)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Fatalf("UPDATE on sec_node as stl_read_write failed with %v, want SQLSTATE 42501 (insufficient_privilege)", err)
		}

		// The regression guard: this INSERT probes change_reason_vocabulary and
		// rel_type_vocabulary, both of which just had privileges revoked. Under the #574
		// pattern it fails with "permission denied for table change_reason_vocabulary".
		if _, err := appPool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ('sec-t-acl', 'SECURITY', 'em-t-acl', 'ENTITY', 'ISSUED_BY', '2026-01-01',
			        'test', 'SEED_LOAD', 'fk probe under the app role', 'test')`); err != nil {
			t.Fatalf("INSERT into sec_edge as stl_read_write: %v — the FK integrity probe against the revoked vocabulary tables is failing, which is the #574 regression", err)
		}

		var appended int
		if err := appPool.QueryRow(ctx, `SELECT count(*) FROM sec_edge WHERE src_id = 'sec-t-acl'`).Scan(&appended); err != nil {
			t.Fatalf("read back the appended edge: %v", err)
		}
		if appended != 1 {
			t.Errorf("appended %d rows, want 1", appended)
		}
	})
}

// TestSecStoreAppendGuardChainsAndRejectsForgedProvenance covers sec_store_append_guard(),
// which is not one of the four acceptance items but is the other thing wave 1 enforces at the
// write boundary — and the part that a review caught getting the round-trip wrong.
//
// The pre-image excludes record_id so a hash survives a re-import that reassigns the identity
// sequence, and it must exclude supersedes_record_id for the same reason: that column IS a
// record_id, so leaving it in defeated the exclusion for every correction and tombstone. The
// guard now substitutes the predecessor's content_hash, which both fixes the round-trip and
// makes the digest an actual chain. The last two subtests pin exactly that: the stored hash
// reproduces when the pointer is replaced by the predecessor's hash, and does NOT reproduce
// when the raw record_id is left in place — the pre-fix behaviour.
func TestSecStoreAppendGuardChainsAndRejectsForgedProvenance(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	t.Run("forged_ingest_xid_is_rejected", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, ingest_xid, `+secstoreSpine+`)
			VALUES ('em-t-guard-xid', 'ENTITY', 'ACTIVE', '2026-01-01', '12345'::xid8, 'test', 'SEED_LOAD', 'forged', 'test')`)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
			t.Fatalf("forged ingest_xid failed with %v, want P0001 from the guard", err)
		}
	})

	t.Run("supplied_hash_that_does_not_match_is_rejected", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, content_hash, `+secstoreSpine+`)
			VALUES ('em-t-guard-hash', 'ENTITY', 'ACTIVE', '2026-01-01', '\xdeadbeef'::bytea, 'test', 'SEED_LOAD', 'bad hash', 'test')`)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
			t.Fatalf("mismatched content_hash failed with %v, want P0001 from the guard", err)
		}
	})

	t.Run("run_id_must_name_a_real_writer_run", func(t *testing.T) {
		// The FK exists so a governed row always resolves to the artefact that wrote it
		// (run_id -> writer_run.build_id -> build_registry, ADR-0006 §2). NULL stays legal and
		// means written before run tracking, which is what the 501 seeded rows are.
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, run_id, `+secstoreSpine+`)
			VALUES ('em-t-run-bad', 'ENTITY', 'ACTIVE', '2026-01-01', 999999999, 'test', 'SEED_LOAD', 'no such run', 'test')`)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23503" {
			t.Fatalf("unknown run_id failed with %v, want SQLSTATE 23503 (foreign_key_violation)", err)
		}

		// A real run is accepted. build_registry id 0 is the pre-tracking row every database
		// carries, so the fixture needs no artefact of its own.
		var runID int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO writer_run (build_id, reference_snapshot, reference_effective_at)
			VALUES (0, pg_current_snapshot()::text, now()) RETURNING id`).Scan(&runID); err != nil {
			t.Fatalf("open a writer run: %v", err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, run_id, `+secstoreSpine+`)
			VALUES ('em-t-run-good', 'ENTITY', 'ACTIVE', '2026-01-01', $1, 'test', 'SEED_LOAD', 'attributed', 'test')`, runID); err != nil {
			t.Fatalf("a row naming a real writer run must land: %v", err)
		}

		// The point of the FK: the row resolves to its build artefact by join, not by trust.
		var gitHash string
		if err := pool.QueryRow(ctx, `
			SELECT b.git_hash FROM sec_node n
			JOIN writer_run w ON w.id = n.run_id
			JOIN build_registry b ON b.id = w.build_id
			WHERE n.id = 'em-t-run-good'`).Scan(&gitHash); err != nil {
			t.Fatalf("resolve the row to its artefact: %v", err)
		}
		if gitHash == "" {
			t.Error("empty git_hash from the provenance join")
		}
	})

	t.Run("a_window_starting_at_infinity_is_rejected", func(t *testing.T) {
		// Every read tests valid_from <= effective_at, so a window starting at infinity would be
		// unreachable by any as-of date; the CHECK turns that into a failed write.
		for _, store := range []string{"sec_node", "sec_edge"} {
			var err error
			if store == "sec_node" {
				_, err = pool.Exec(ctx, `
					INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
					VALUES ('em-t-inf', 'ENTITY', 'ACTIVE', 'infinity', 'infinity', 'test', 'SEED_LOAD', 'never visible', 'test')`)
			} else {
				_, err = pool.Exec(ctx, `
					INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
					VALUES ('sec-t-inf', 'SECURITY', 'em-t-inf', 'ENTITY', 'ISSUED_BY', 'infinity', 'infinity', 'test', 'SEED_LOAD', 'never visible', 'test')`)
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
				t.Errorf("%s: valid_from 'infinity' failed with %v, want SQLSTATE 23514 — 'infinity' is the open-END sentinel only", store, err)
			}
		}
	})

	t.Run("a_matching_supplied_hash_is_accepted", func(t *testing.T) {
		// Without this, a guard that rejected EVERY supplied hash would pass the mismatch subtest
		// above and still break the re-import path the substitution exists to enable.
		const id = "em-t-guard-verified"
		if _, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, content_hash, `+secstoreSpine+`)
			VALUES ($1::text, 'ENTITY', 'ACTIVE', '2026-01-01', 'infinity',
			        sha256(convert_to(jsonb_build_object(
			            'id', $1::text, 'record_type', 'ENTITY', 'chain_id', NULL, 'status', 'ACTIVE',
			            'attrs', '{}'::jsonb, 'valid_from', '2026-01-01'::date, 'valid_to', 'infinity'::date,
			            'processing_version', 0, 'run_id', NULL, 'actor', 'test',
			            'change_reason_code', 'SEED_LOAD', 'change_reason', 'export re-import',
			            'approved_by', NULL, 'source_system', 'test')::text, 'UTF8')),
			        'test', 'SEED_LOAD', 'export re-import', 'test')`, id); err != nil {
			t.Fatalf("a correctly computed supplied hash must be accepted: %v", err)
		}
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sec_node WHERE id = $1`, id).Scan(&n); err != nil {
			t.Fatalf("read back: %v", err)
		}
		if n != 1 {
			t.Errorf("row count %d, want 1", n)
		}
	})

	t.Run("supersedes_record_id_naming_no_stored_row_is_rejected", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, supersedes_record_id, `+secstoreSpine+`)
			VALUES ('em-t-guard-orphan', 'ENTITY', 'ACTIVE', '2026-01-01', 999999999, 'test', 'RESTATEMENT', 'orphan pointer', 'test')`)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
			t.Fatalf("unresolvable supersedes_record_id failed with %v, want P0001 — the chain cannot be computed without the predecessor", err)
		}
	})

	t.Run("hash_chains_on_the_predecessors_content_not_its_record_id", func(t *testing.T) {
		const id = "em-t-guard-chain"
		insertNode(ctx, t, pool, id, "ACTIVE", "2026-01-01", "'infinity'", "the original")
		// processing_version 1, not 0: a restatement keeps the valid window it corrects, so at 0
		// it collides with the row it supersedes — which is the definition ADR-0006 §3 gives a
		// correction, and the one case that does allocate a version. A valid-time change stays
		// at 0 because valid_to differs; see the close-and-open test.
		if _, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, processing_version,
			                      actor, change_reason_code, change_reason, approved_by,
			                      supersedes_record_id, source_system)
			SELECT $1, 'ENTITY', 'INACTIVE', '2026-01-01', 'infinity', 1,
			       'test', 'RESTATEMENT', 'the correction', 'approver', record_id, 'test'
			FROM sec_node WHERE id = $1 AND change_reason_code = 'SEED_LOAD'`, id); err != nil {
			t.Fatalf("append the correction: %v", err)
		}

		// Recompute the correction's hash two ways: substituting the predecessor's hash (what
		// the guard does, and what re-import can reproduce) and leaving the raw record_id in
		// (the pre-fix pre-image, which a re-import cannot reproduce).
		var chained, rawPointer bool
		if err := pool.QueryRow(ctx, `
			WITH correction AS (
				SELECT * FROM sec_node WHERE id = $1 AND change_reason_code = 'RESTATEMENT'
			), parent AS (
				SELECT content_hash FROM sec_node
				WHERE record_id = (SELECT supersedes_record_id FROM correction)
			)
			SELECT c.content_hash = sha256(convert_to((
			           to_jsonb(c) - 'record_id' - 'ingest_xid' - 'ingested_at' - 'content_hash'
			                       - 'supersedes_record_id'
			           || jsonb_build_object('supersedes_content_hash',
			                                 encode((SELECT content_hash FROM parent), 'hex'))
			       )::text, 'UTF8')),
			       c.content_hash = sha256(convert_to((
			           to_jsonb(c) - 'record_id' - 'ingest_xid' - 'ingested_at' - 'content_hash'
			       )::text, 'UTF8'))
			FROM correction c`, id,
		).Scan(&chained, &rawPointer); err != nil {
			t.Fatalf("recompute the correction hash: %v", err)
		}
		if !chained {
			t.Error("the stored hash does not reproduce with supersedes_record_id replaced by the predecessor's content_hash — the chain is not what the guard computes")
		}
		if rawPointer {
			t.Error("the stored hash still reproduces with the raw supersedes_record_id in the pre-image: the pointer is a record_id, so a re-import that reassigns the sequence would invalidate every correction and tombstone")
		}
	})
}

// TestSecStoreKnowledgeTimeReadReplaysWhatWasKnown covers the pg_snapshot overloads of the
// _as_of functions: the second clock. Valid time says when a fact was true in the world,
// knowledge time when we had learned it, and the pair is what makes a backdated late discovery
// distinguishable from something we always knew (RP-4.1, CR-3.5/3.6).
//
// The property that matters is the ordering INSIDE the function: the snapshot filter has to run
// before version resolution. Filtered afterwards, a correction that is invisible in the
// snapshot still wins its (id, valid_from) group and the group then vanishes — the replay would
// return no row where it should return the original. Both are asserted.
func TestSecStoreKnowledgeTimeReadReplaysWhatWasKnown(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const id = "em-t-knowledge"
	insertNode(ctx, t, pool, id, "ACTIVE", "2026-01-01", "'infinity'", "what we believed first")

	// The snapshot a consumer would have recorded alongside its effective date.
	var before string
	if err := pool.QueryRow(ctx, `SELECT pg_current_snapshot()::text`).Scan(&before); err != nil {
		t.Fatalf("capture the snapshot: %v", err)
	}

	// A restatement: same valid window, corrected content, so it is a correction and takes a
	// processing_version (ADR-0006 §3).
	if _, err := pool.Exec(ctx, `
		INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, processing_version,
		                      actor, change_reason_code, change_reason, approved_by,
		                      supersedes_record_id, source_system)
		SELECT $1, 'ENTITY', 'INACTIVE', '2026-01-01', 'infinity', 1,
		       'test', 'RESTATEMENT', 'the earlier record was wrong', 'approver', record_id, 'test'
		FROM sec_node WHERE id = $1 AND change_reason_code = 'SEED_LOAD'`, id); err != nil {
		t.Fatalf("append the restatement: %v", err)
	}

	t.Run("the_recorded_snapshot_replays_the_original", func(t *testing.T) {
		var status string
		if err := pool.QueryRow(ctx, `
			SELECT status FROM sec_node_as_of('2026-03-01', $1::pg_snapshot) WHERE id = $2`, before, id,
		).Scan(&status); err != nil {
			t.Fatalf("replay through the recorded snapshot: %v — an empty result is the failure mode of filtering the snapshot after version resolution", err)
		}
		if status != "ACTIVE" {
			t.Errorf("replay resolved status=%s, want ACTIVE: the snapshot predates the restatement, so it must return what was known then", status)
		}
	})

	t.Run("the_effective_date_read_returns_the_correction", func(t *testing.T) {
		var status string
		if err := pool.QueryRow(ctx, `SELECT status FROM sec_node_as_of('2026-03-01') WHERE id = $1`, id).Scan(&status); err != nil {
			t.Fatalf("sec_node_as_of: %v", err)
		}
		if status != "INACTIVE" {
			t.Errorf("the one-argument read resolved status=%s, want INACTIVE (latest processing_version)", status)
		}
	})

	t.Run("a_current_snapshot_agrees_with_the_effective_date_read", func(t *testing.T) {
		var status string
		if err := pool.QueryRow(ctx, `
			SELECT status FROM sec_node_as_of('2026-03-01', pg_current_snapshot()) WHERE id = $1`, id,
		).Scan(&status); err != nil {
			t.Fatalf("replay through the current snapshot: %v", err)
		}
		if status != "INACTIVE" {
			t.Errorf("current-snapshot replay resolved status=%s, want INACTIVE", status)
		}
	})

	t.Run("edges_replay_the_same_way", func(t *testing.T) {
		const src = "sec-t-knowledge"
		if _, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ($1, 'SECURITY', 'em-t-issuer-first', 'ENTITY', 'ISSUED_BY', '2026-01-01',
			        'test', 'SEED_LOAD', 'issuer as first believed', 'test')`, src); err != nil {
			t.Fatalf("insert the edge: %v", err)
		}
		var beforeEdge string
		if err := pool.QueryRow(ctx, `SELECT pg_current_snapshot()::text`).Scan(&beforeEdge); err != nil {
			t.Fatalf("capture the edge snapshot: %v", err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to,
			                      processing_version, actor, change_reason_code, change_reason,
			                      approved_by, supersedes_record_id, source_system)
			SELECT $1, 'SECURITY', 'em-t-issuer-first', 'ENTITY', 'ISSUED_BY', '2026-01-01', 'infinity',
			       1, 'test', 'RESTATEMENT', 'wrong issuer recorded', 'approver', record_id, 'test'
			FROM sec_edge WHERE src_id = $1 AND change_reason_code = 'SEED_LOAD'`, src); err != nil {
			t.Fatalf("append the edge restatement: %v", err)
		}

		var oldReason, newReason string
		if err := pool.QueryRow(ctx, `
			SELECT (SELECT change_reason_code FROM sec_edge_as_of('2026-03-01', $1::pg_snapshot) WHERE src_id = $2),
			       (SELECT change_reason_code FROM sec_edge_as_of('2026-03-01') WHERE src_id = $2)`, beforeEdge, src,
		).Scan(&oldReason, &newReason); err != nil {
			t.Fatalf("replay the edge: %v", err)
		}
		if oldReason != "SEED_LOAD" || newReason != "RESTATEMENT" {
			t.Errorf("edge replay resolved %s through the recorded snapshot and %s through the effective-date read, want SEED_LOAD and RESTATEMENT", oldReason, newReason)
		}
	})
}

// TestSecStoreEdgeDiscriminatorIsDerivedNotAllocated covers the DM-6 discriminator.
//
// edge_disc is a primary-key component and is rendered into the stored edge_id, so its value is
// inside row identity and inside every content_hash chained from it. A counter put an unstable
// value there: it has no allocator, so a twin is allocated by reading current state, two writers
// creating different twins concurrently compute the same next value and the loser is a primary-key
// violation rather than a second twin, and a replay that recomputes assigns a different one.
//
// It is now a function of the payload subset rel_type_vocabulary.cluster_key declares, which is
// deterministic by construction and needs neither an allocator nor a carried value.
//
// The subtest that matters most is close_and_open_on_a_non_cluster_attribute_keeps_one_identity.
// Deriving the discriminator from ALL of payload would be wrong in a way the other subtests do not
// see: edge_id spans every version and window of an edge and sec_edge_current groups on the
// discriminator, so an attribute corrected under close-and-open would hash to a new identity, the
// old row would never be closed, and GQ-20 would read two current edges where there is one. The
// cluster key is the IMMUTABLE subset for exactly that reason, and this pins it.
func TestSecStoreEdgeDiscriminatorIsDerivedNotAllocated(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	// A twin-bearing type, declared the way the migration that ratifies one would declare it.
	// All thirteen ratified types declare no cluster key, so none of them admits a twin.
	if _, err := pool.Exec(ctx, `
		INSERT INTO rel_type_vocabulary
			(rel_type, family, src_kinds, dst_kinds, cardinality, cluster_key, maturity, description)
		VALUES ('COLLATERALISED_BY','composition','{SECURITY}','{SECURITY}','n','{lien}','draft','test'),
		       ('TRANCHE_OF','composition','{SECURITY}','{SECURITY}','n','{attach}','draft','test')`); err != nil {
		t.Fatalf("declare twin-bearing types: %v", err)
	}

	insertEdge := func(relType, src, dst, payload, validFrom, validToSQL string) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, payload,
			                      valid_from, valid_to, `+secstoreSpine+`)
			VALUES ($2,'SECURITY',$3,'SECURITY',$1,$4::jsonb,$5::date,`+validToSQL+`,
			        'test','SEED_LOAD','discriminator test','test')`,
			relType, src, dst, payload, validFrom)
		return err
	}

	t.Run("seeded_taxonomy_edges_carry_the_base_discriminator", func(t *testing.T) {
		var total, base, rendered int
		if err := pool.QueryRow(ctx, `
			SELECT count(*), count(*) FILTER (WHERE edge_disc = 'base'),
			       count(*) FILTER (WHERE edge_id LIKE '%:base')
			  FROM sec_edge WHERE rel_type = 'NARROWER_THAN'`).Scan(&total, &base, &rendered); err != nil {
			t.Fatalf("read seeded edges: %v", err)
		}
		if total == 0 || base != total || rendered != total {
			t.Fatalf("seeded NARROWER_THAN edges: %d rows, %d at 'base', %d rendering ':base' — "+
				"NARROWER_THAN declares no cluster key, so every row must be the base edge", total, base, rendered)
		}
	})

	t.Run("twins_of_one_pair_are_distinguished_by_the_declared_cluster_key", func(t *testing.T) {
		for _, lien := range []string{"SENIOR", "JUNIOR"} {
			if err := insertEdge("COLLATERALISED_BY", "sec-d-a", "sec-d-b",
				`{"lien":"`+lien+`"}`, "2026-01-01", "'infinity'"); err != nil {
				t.Fatalf("insert %s twin: %v — two deliberate twins of one pair must both land, "+
					"which is what a counter could not do without an allocator", lien, err)
			}
		}
		var discs, ids int
		if err := pool.QueryRow(ctx, `
			SELECT count(DISTINCT edge_disc), count(DISTINCT edge_id) FROM sec_edge
			 WHERE rel_type = 'COLLATERALISED_BY' AND src_id = 'sec-d-a'`).Scan(&discs, &ids); err != nil {
			t.Fatalf("read twins: %v", err)
		}
		if discs != 2 || ids != 2 {
			t.Errorf("two twins produced %d discriminators / %d edge_ids, want 2 / 2", discs, ids)
		}
	})

	t.Run("an_identical_cluster_payload_is_a_duplicate_not_a_twin", func(t *testing.T) {
		// GQ-19's accidental duplicate, refused by the engine rather than found later: with a
		// counter the writer supplied a fresh seq and got a second identity for the same fact.
		err := insertEdge("COLLATERALISED_BY", "sec-d-a", "sec-d-b", `{"lien":"SENIOR"}`, "2026-01-01", "'infinity'")
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			t.Fatalf("re-appending an identical SENIOR twin failed with %v, want SQLSTATE 23505", err)
		}
	})

	t.Run("a_type_declaring_no_cluster_key_admits_no_twins", func(t *testing.T) {
		if err := insertEdge("HAS_UNDERLYING", "sec-d-c", "sec-d-d", `{"note":"first"}`, "2026-01-01", "'infinity'"); err != nil {
			t.Fatalf("first HAS_UNDERLYING edge: %v", err)
		}
		err := insertEdge("HAS_UNDERLYING", "sec-d-c", "sec-d-d", `{"note":"second"}`, "2026-01-01", "'infinity'")
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			t.Fatalf("a second HAS_UNDERLYING edge on the same pair failed with %v, want SQLSTATE 23505 — "+
				"cluster_key IS NULL means one logical edge per (rel_type, src_id, dst_id)", err)
		}
	})

	t.Run("close_and_open_on_a_non_cluster_attribute_keeps_one_identity", func(t *testing.T) {
		// outlook is NOT in the cluster key, so correcting it must close the window rather than
		// fork identity. Deriving from all of payload fails here and nowhere else.
		if err := insertEdge("COLLATERALISED_BY", "sec-d-e", "sec-d-f",
			`{"lien":"SENIOR","outlook":"stable"}`, "2026-01-01", "'2026-06-01'"); err != nil {
			t.Fatalf("closing row: %v", err)
		}
		if err := insertEdge("COLLATERALISED_BY", "sec-d-e", "sec-d-f",
			`{"lien":"SENIOR","outlook":"negative"}`, "2026-06-01", "'infinity'"); err != nil {
			t.Fatalf("reopening row: %v", err)
		}
		var discs, current int
		if err := pool.QueryRow(ctx, `
			SELECT (SELECT count(DISTINCT edge_disc) FROM sec_edge
			         WHERE rel_type = 'COLLATERALISED_BY' AND src_id = 'sec-d-e'),
			       (SELECT count(*) FROM sec_edge_current
			         WHERE rel_type = 'COLLATERALISED_BY' AND src_id = 'sec-d-e')`).Scan(&discs, &current); err != nil {
			t.Fatalf("read closed-and-reopened edge: %v", err)
		}
		if discs != 1 {
			t.Errorf("a corrected non-cluster attribute produced %d discriminators, want 1 — the "+
				"discriminator must be constant across versions and windows, or the old row is never closed", discs)
		}
		if current != 1 {
			t.Errorf("sec_edge_current returns %d rows for one logical edge, want 1 — two would be "+
				"the resurrection GQ-20 exists to catch, moved into identity", current)
		}
	})

	t.Run("numeric_scale_does_not_split_identity", func(t *testing.T) {
		// {"attach":0.5} and {"attach":0.500} are jsonb-equal and hash differently without a
		// canonical form, so without one the same twin gets two identities by how it arrived.
		if err := insertEdge("TRANCHE_OF", "sec-d-g", "sec-d-h", `{"attach":0.5}`, "2026-01-01", "'infinity'"); err != nil {
			t.Fatalf("first tranche edge: %v", err)
		}
		err := insertEdge("TRANCHE_OF", "sec-d-g", "sec-d-h", `{"attach":0.500}`, "2026-01-01", "'infinity'")
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			t.Fatalf("0.500 against a stored 0.5 failed with %v, want SQLSTATE 23505 — "+
				"sec_canonical_jsonb trims scale so the two are one identity", err)
		}
	})

	t.Run("a_supplied_discriminator_is_verified_never_trusted", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, payload, edge_disc,
			                      valid_from, `+secstoreSpine+`)
			VALUES ('sec-d-i','SECURITY','sec-d-j','SECURITY','COLLATERALISED_BY','{"lien":"MEZZ"}',
			        'deadbeefdeadbeef','2026-01-01','test','SEED_LOAD','forged discriminator','test')`)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
			t.Fatalf("a forged edge_disc was accepted or failed with %v, want the guard's P0001", err)
		}
	})

	t.Run("an_unknown_rel_type_is_still_the_fks_to_reject", func(t *testing.T) {
		// The guard looks the cluster key up by rel_type. A type with no vocabulary row must fall
		// through to the foreign key (23503, GQ-01) rather than being shadowed by a guard error.
		err := insertEdge("NO_SUCH_TYPE", "sec-d-k", "sec-d-l", `{}`, "2026-01-01", "'infinity'")
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23503" {
			t.Fatalf("unknown rel_type failed with %v, want SQLSTATE 23503 — the guard must not "+
				"shadow the vocabulary foreign key", err)
		}
	})

	t.Run("the_discriminator_reproduces_from_the_payload_alone", func(t *testing.T) {
		// Replay: nothing but the payload and the declared key is needed to recompute identity,
		// and the hash chain covers the result.
		var reproduces, hashes bool
		if err := pool.QueryRow(ctx, `
			SELECT bool_and(e.edge_disc = sec_edge_discriminator(e.payload, v.cluster_key)),
			       bool_and(e.content_hash = sha256(convert_to((to_jsonb(e)
			           - 'record_id' - 'ingest_xid' - 'ingested_at' - 'content_hash'
			           - 'edge_id' - 'supersedes_record_id')::text, 'UTF8')))
			  FROM sec_edge e JOIN rel_type_vocabulary v USING (rel_type)
			 WHERE e.supersedes_record_id IS NULL`).Scan(&reproduces, &hashes); err != nil {
			t.Fatalf("recompute identity: %v", err)
		}
		if !reproduces {
			t.Error("a stored edge_disc does not recompute from its payload and declared cluster key")
		}
		if !hashes {
			t.Error("content_hash does not recompute over the stored row, so it no longer covers the discriminator")
		}
	})

	t.Run("two_writers_creating_different_twins_concurrently_both_land", func(t *testing.T) {
		// The counter's headline failure: both writers read the same current state, computed the
		// same next value, and the loser got a primary-key violation instead of a second twin.
		// Neither writer reads the store now, so there is nothing to serialise.
		first, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin first writer: %v", err)
		}
		defer first.Rollback(ctx)
		second, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin second writer: %v", err)
		}
		defer second.Rollback(ctx)

		const stmt = `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, payload,
			                      valid_from, ` + secstoreSpine + `)
			VALUES ('sec-d-m','SECURITY','sec-d-n','SECURITY','COLLATERALISED_BY',$1::jsonb,
			        '2026-01-01','test','SEED_LOAD','concurrent twin','test')`
		if _, err := first.Exec(ctx, stmt, `{"lien":"SENIOR"}`); err != nil {
			t.Fatalf("first writer: %v", err)
		}
		if _, err := second.Exec(ctx, stmt, `{"lien":"JUNIOR"}`); err != nil {
			t.Fatalf("second writer blocked or collided with the first: %v — two different twins "+
				"must not contend, which is what removing the read-then-write allocation buys", err)
		}
		if err := first.Commit(ctx); err != nil {
			t.Fatalf("commit first writer: %v", err)
		}
		if err := second.Commit(ctx); err != nil {
			t.Fatalf("commit second writer: %v", err)
		}
		var landed int
		if err := pool.QueryRow(ctx, `
			SELECT count(DISTINCT edge_id) FROM sec_edge
			 WHERE rel_type = 'COLLATERALISED_BY' AND src_id = 'sec-d-m'`).Scan(&landed); err != nil {
			t.Fatalf("count concurrent twins: %v", err)
		}
		if landed != 2 {
			t.Errorf("%d twins landed from two concurrent writers, want 2", landed)
		}
	})
}
