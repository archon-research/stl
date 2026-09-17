//go:build integration

package migrator_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The VEC-616 acceptance tests for the combined master's two identifier registers: the
// behaviours the migrations claim, plus the two key defects the ticket reproduced against the
// drafted DDL. They sit in db/migrator beside the wave-1 file for the same reason those do —
// they need a database whose migration order this package controls — and the package is already
// in ci/integration-shards/1.txt.
//
// setupMigratedPostgres hands each test its own freshly migrated database, so the held-book
// seed is readable without the cross-test interference AGENTS.md warns about. That same seed is
// why every key below is synthetic except in the held-book test: a fixture sharing a key with a
// seeded row makes an assertion count seeded rows by accident.

const registerSpine = `actor, change_reason_code, change_reason, source_system`

// instrumentRow is a fixture factory: the register's identity columns with defaults, so a test
// states only what it varies. validToSQL is SQL rather than a parameter so a test can write
// 'infinity' without pgx date-infinity handling.
type instrumentRow struct {
	key        string
	namespace  string
	securityID string
	chainID    *int32
	validFrom  string
	validToSQL string
}

func newInstrumentRow(key, securityID string) instrumentRow {
	return instrumentRow{
		key:        key,
		namespace:  "token_address",
		securityID: securityID,
		chainID:    chainID(1),
		validFrom:  "2026-01-01",
		validToSQL: "'infinity'",
	}
}

func chainID(id int32) *int32 { return &id }

func insertInstrument(ctx context.Context, t *testing.T, pool *pgxpool.Pool, r instrumentRow) error {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO instrument_register
			(instrument_key, key_namespace, security_id, chain_id, valid_from, valid_to, `+registerSpine+`)
		VALUES ($1, $2, $3, $4, $5::date, `+r.validToSQL+`, 'test', 'SEED_LOAD', 'register acceptance', 'test')`,
		r.key, r.namespace, r.securityID, r.chainID, r.validFrom)
	return err
}

func mustInsertInstrument(ctx context.Context, t *testing.T, pool *pgxpool.Pool, r instrumentRow) {
	t.Helper()
	if err := insertInstrument(ctx, t, pool, r); err != nil {
		chain := "none"
		if r.chainID != nil {
			chain = strconv.Itoa(int(*r.chainID))
		}
		t.Fatalf("insert %s (chain %s, from %s): %v", r.key, chain, r.validFrom, err)
	}
}

// uniqueViolation reports whether err is a 23505, the code both register keys raise on a
// collision. Distinguished from any other failure so a test cannot pass on the wrong error.
func uniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// raisedWith reports whether err is a trigger RAISE (P0001) whose message contains want. The
// substring matters: every guard raises P0001, so the code alone cannot tell them apart and a
// test asserting only the code passes on the wrong rejection.
func raisedWith(err error, want string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "P0001" && strings.Contains(pgErr.Message, want)
}

// TestInstrumentRegisterRepointLandsAtProcessingVersionZero is the defect the ticket reproduced
// against the drafted key (instrument_key, processing_version), as a test.
//
// A re-point — MKR redenominating to SKY, a key moved to a corrected security — is a VALID-TIME
// change, so ADR-0006 §3 keeps it at processing_version 0: versions above 0 are allocated per
// correction run, one per ticket, which a re-point is not. Under the drafted key that second row
// collided with the row it re-points, and the ticket's warning is that a re-point shipped
// seed-style with ON CONFLICT DO NOTHING would then be swallowed rather than fail. valid_from in
// the key is what makes it an ordinary append.
func TestInstrumentRegisterRepointLandsAtProcessingVersionZero(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const key = "d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4"
	mustInsertInstrument(ctx, t, pool, newInstrumentRow(key, "sec-mkr"))

	repoint := newInstrumentRow(key, "sec-sky")
	repoint.validFrom = "2026-06-01"
	mustInsertInstrument(ctx, t, pool, repoint)

	t.Run("both rows land at processing_version 0", func(t *testing.T) {
		var rows, lo, hi int
		if err := pool.QueryRow(ctx, `
			SELECT count(*), min(processing_version), max(processing_version)
			FROM instrument_register WHERE instrument_key = $1`, key,
		).Scan(&rows, &lo, &hi); err != nil {
			t.Fatalf("count rows: %v", err)
		}
		if rows != 2 {
			t.Errorf("got %d rows, want 2 — a re-point colliding on the PK is the defect this guards", rows)
		}
		if lo != 0 || hi != 0 {
			t.Errorf("processing_version spans [%d, %d], want [0, 0]: a re-point is a valid-time change and must not burn a correction version (ADR-0006 §3)", lo, hi)
		}
	})

	t.Run("the later mapping is current", func(t *testing.T) {
		var securityID string
		if err := pool.QueryRow(ctx,
			`SELECT security_id FROM instrument_register_current WHERE instrument_key = $1`, key,
		).Scan(&securityID); err != nil {
			t.Fatalf("resolve current: %v", err)
		}
		if securityID != "sec-sky" {
			t.Errorf("current resolves %s, want sec-sky", securityID)
		}
	})

	t.Run("as_of before the re-point resolves the old mapping", func(t *testing.T) {
		var securityID string
		if err := pool.QueryRow(ctx,
			`SELECT security_id FROM instrument_register_as_of('2026-03-01') WHERE instrument_key = $1`, key,
		).Scan(&securityID); err != nil {
			t.Fatalf("instrument_register_as_of(2026-03-01): %v", err)
		}
		if securityID != "sec-mkr" {
			t.Errorf("as_of 2026-03-01 resolves %s, want sec-mkr — resolving positions at now() applies today's mapping to yesterday's holding (ADR-0007 §9.3)", securityID)
		}
	})
}

// TestInstrumentRegisterHoldsOneAddressOnTwoChains is the second defect: chain_id sat on the row
// but not in the key, so the same contract address on two chains could not both register. It
// bites now rather than later — 13 addresses are deployed on more than one chain today, 3 of
// them held — and both rows must also resolve independently, which is what chain_scope in the
// read predicate buys.
func TestInstrumentRegisterHoldsOneAddressOnTwoChains(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const key = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
	mainnet := newInstrumentRow(key, "sec-multi-mainnet")
	base := newInstrumentRow(key, "sec-multi-base")
	base.chainID = chainID(8453)
	mustInsertInstrument(ctx, t, pool, mainnet)
	mustInsertInstrument(ctx, t, pool, base)

	var rows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM instrument_register_current WHERE instrument_key = $1`, key,
	).Scan(&rows); err != nil {
		t.Fatalf("count current: %v", err)
	}
	if rows != 2 {
		t.Fatalf("got %d current rows for one address on two chains, want 2", rows)
	}

	// Assert the SECURITY each chain resolves to, not the chain_scope the query already filtered
	// on: scanning the filter value back proves only that a row matched.
	for _, tc := range []struct {
		chain int32
		want  string
	}{
		{chain: 1, want: "sec-multi-mainnet"},
		{chain: 8453, want: "sec-multi-base"},
	} {
		var got string
		if err := pool.QueryRow(ctx, `
			SELECT security_id FROM instrument_register_current
			WHERE instrument_key = $1 AND chain_scope = coalesce($2::int4, 0)`, key, tc.chain,
		).Scan(&got); err != nil {
			t.Errorf("resolve chain %d: %v — the read predicate the position stream uses", tc.chain, err)
			continue
		}
		if got != tc.want {
			t.Errorf("chain %d resolves %s, want %s — the two deployments must not cross", tc.chain, got, tc.want)
		}
	}
}

// TestInstrumentRegisterAcceptsANamespaceWithNoChain is why chain_scope exists as a generated
// column rather than chain_id going into the key: a PRIMARY KEY forces NOT NULL, and two of the
// four namespaces have no chain by construction — Anchorage is off-chain custody, and a Sky ilk
// comes from a source carrying no chain column. chain_id must stay honestly NULL while the row
// still takes a key.
func TestInstrumentRegisterAcceptsANamespaceWithNoChain(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	anchorage := newInstrumentRow("t616-provider:pkg-7f3a", "sec-usd-custody")
	anchorage.namespace = "provider_package"
	anchorage.chainID = nil
	mustInsertInstrument(ctx, t, pool, anchorage)

	var storedChain *int32
	var scope int32
	if err := pool.QueryRow(ctx,
		`SELECT chain_id, chain_scope FROM instrument_register_current WHERE instrument_key = $1`,
		anchorage.key,
	).Scan(&storedChain, &scope); err != nil {
		t.Fatalf("resolve chainless key: %v", err)
	}
	if storedChain != nil {
		t.Errorf("chain_id is %d, want NULL — a chainless namespace must not carry a sentinel in real data", *storedChain)
	}
	if scope != 0 {
		t.Errorf("chain_scope is %d, want 0", scope)
	}
}

// TestInstrumentRegisterRejectsTwoSecuritiesInOneWindow is the ticket's "one identifier
// resolving to two targets in the same valid window is rejected at write". The key rejects the
// exact-window case; two OVERLAPPING but distinct windows are not rejected here and cannot be,
// because an open mapping always time-overlaps its own re-point — that stays a DQ check over
// current state, which is the position wave 1 took for single-valued edge cardinality.
func TestInstrumentRegisterRejectsTwoSecuritiesInOneWindow(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const key = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"
	mustInsertInstrument(ctx, t, pool, newInstrumentRow(key, "sec-usdc"))

	err := insertInstrument(ctx, t, pool, newInstrumentRow(key, "sec-not-usdc"))
	if !uniqueViolation(err) {
		t.Fatalf("second security for the same key and window gave %v, want a unique violation", err)
	}
}

// TestInstrumentRegisterRejectsOneKeyUnderTwoNamespaces pins the rule that one key means one
// instrument. key_namespace is deliberately absent from the primary key — a position row carries
// no namespace, so if one key sat under two the read would have no way to choose — which leaves
// the key unable to enforce the rule it exists for: two rows differing in valid_from do not
// collide at all. instrument_register_namespace_guard is what actually refuses it.
//
// The subtests vary the dates on purpose. An earlier version of this test held valid_from
// constant and so passed on the primary key alone, proving nothing about the case that matters:
// the full-population load lands one namespace per slice, weeks apart.
func TestInstrumentRegisterRejectsOneKeyUnderTwoNamespaces(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const key = "T616-ALLOCATOR-A"
	ilk := newInstrumentRow(key, "sec-sky-debt")
	ilk.namespace = "sky_ilk"
	mustInsertInstrument(ctx, t, pool, ilk)

	for _, tc := range []struct {
		name      string
		validFrom string
	}{
		{name: "a later valid_from, which the key cannot catch", validFrom: "2026-06-01"},
		{name: "an earlier valid_from", validFrom: "2025-01-01"},
		{name: "the same valid_from", validFrom: "2026-01-01"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collision := newInstrumentRow(key, "sec-something-else")
			collision.namespace = "loan_address"
			collision.validFrom = tc.validFrom

			err := insertInstrument(ctx, t, pool, collision)
			if !raisedWith(err, "is already registered under namespace") {
				t.Fatalf("second namespace at valid_from %s gave %v, want the namespace guard to refuse it", tc.validFrom, err)
			}
		})
	}

	t.Run("the original mapping is untouched", func(t *testing.T) {
		var namespace, security string
		if err := pool.QueryRow(ctx,
			`SELECT key_namespace, security_id FROM instrument_register_current WHERE instrument_key = $1`, key,
		).Scan(&namespace, &security); err != nil {
			t.Fatalf("resolve current: %v", err)
		}
		if namespace != "sky_ilk" || security != "sec-sky-debt" {
			t.Errorf("current resolves %s/%s, want sky_ilk/sec-sky-debt — a refused append must not shadow the row it collided with", namespace, security)
		}
	})
}

// TestInstrumentRegisterAcceptsTheSameKeyOnAnotherChain is the boundary of the rule above: the
// namespace guard is scoped to (key, chain_scope), so one address on two chains under the SAME
// namespace stays legal. Without this the guard would break the cross-chain case the primary key
// was widened for.
func TestInstrumentRegisterAcceptsTheSameKeyOnAnotherChain(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const key = "e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1"
	mainnet := newInstrumentRow(key, "sec-multi-mainnet")
	base := newInstrumentRow(key, "sec-multi-base")
	base.chainID = chainID(8453)
	base.validFrom = "2026-06-01"

	mustInsertInstrument(ctx, t, pool, mainnet)
	mustInsertInstrument(ctx, t, pool, base)

	// Distinct securities per chain, asserted per chain: two inserts not erroring says only that
	// the guard let them through, not that each deployment still resolves to its own security.
	for _, tc := range []struct {
		chain int32
		want  string
	}{
		{chain: 1, want: "sec-multi-mainnet"},
		{chain: 8453, want: "sec-multi-base"},
	} {
		var got string
		if err := pool.QueryRow(ctx, `
			SELECT security_id FROM instrument_register_current
			WHERE instrument_key = $1 AND chain_scope = $2`, key, tc.chain,
		).Scan(&got); err != nil {
			t.Errorf("chain %d does not resolve: %v", tc.chain, err)
			continue
		}
		if got != tc.want {
			t.Errorf("chain %d resolves %s, want %s", tc.chain, got, tc.want)
		}
	}
}

// TestInstrumentRegisterRefusesASecondRowOnOneWindow is the shadow-row defect, found in review.
//
// valid_to is in the primary key but NOT in what the reads group on, so two rows sharing
// (key, chain_scope, valid_from) and differing only in valid_to never collide. Both land, step
// one of the read keeps whichever arrived last, and the displaced row never reaches the
// valid-time filter — so once the survivor's window ends the key resolves to NOTHING while an
// open mapping sits in the table. Reproduced before the fix:
//
//	open   (K, chain 1, 2026-01-01 -> infinity)   sec-first
//	shadow (K, chain 1, 2026-01-01 -> 2027-01-01) sec-second   <- accepted, no error
//	as_of 2027-01-02 -> nothing, with sec-first still open in the table
//
// Nothing downstream can detect it: instrument_register_current distincts on
// (instrument_key, chain_scope) with security_id outside that key, so it returns one row by
// construction and the discarded mapping leaves no trace. Hence a write-time rule.
func TestInstrumentRegisterRefusesASecondRowOnOneWindow(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const key = "b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0"
	var opened int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO instrument_register
			(instrument_key, key_namespace, security_id, chain_id, valid_from, valid_to, `+registerSpine+`)
		VALUES ($1, 'token_address', 'sec-first', 1, '2026-01-01', 'infinity',
		        'test', 'SEED_LOAD', 'open the window', 'test')
		RETURNING record_id`, key).Scan(&opened); err != nil {
		t.Fatalf("open the window: %v", err)
	}

	second := func(supersedes any, security, validTo string) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO instrument_register
				(instrument_key, key_namespace, security_id, chain_id, valid_from, valid_to,
				 supersedes_record_id, `+registerSpine+`)
			VALUES ($1, 'token_address', $2, 1, '2026-01-01', $3::date, $4,
			        'test', 'SEED_LOAD', 'second row on the window', 'test')`,
			key, security, validTo, supersedes)
		return err
	}

	t.Run("an unattributed second row is refused", func(t *testing.T) {
		if err := second(nil, "sec-second", "2027-01-01"); !raisedWith(err, "must name the row it closes") {
			t.Fatalf("shadow row gave %v, want a refusal — it would displace sec-first by insert order alone", err)
		}
	})

	t.Run("superseding a row on another window is refused", func(t *testing.T) {
		var other int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO instrument_register
				(instrument_key, key_namespace, security_id, chain_id, valid_from, valid_to, `+registerSpine+`)
			VALUES ($1, 'token_address', 'sec-later', 1, '2026-06-01', 'infinity',
			        'test', 'SEED_LOAD', 'a different window', 'test')
			RETURNING record_id`, key).Scan(&other); err != nil {
			t.Fatalf("open a second window: %v", err)
		}
		if err := second(other, "sec-second", "2027-01-01"); !raisedWith(err, "not on the window it lands on") {
			t.Fatalf("superseding a row on another window gave %v, want a refusal", err)
		}
	})

	t.Run("a close that names the row it closes lands", func(t *testing.T) {
		if err := second(opened, "sec-first", "2026-09-01"); err != nil {
			t.Fatalf("an attributed close was refused: %v", err)
		}
	})

	// A-open, B-closes-A, C-also-closes-A: all three pass the supersession guard, because C names
	// a row that IS on the window. B then becomes unreachable — the shadow row one level up. A
	// partial unique index answers it declaratively: a row is superseded at most once.
	t.Run("a second row superseding the same predecessor is refused", func(t *testing.T) {
		if err := second(opened, "sec-fork", "2026-10-01"); !uniqueViolation(err) {
			t.Fatalf("a fork on record %d gave %v, want a unique violation — two rows cannot both close one row", opened, err)
		}
	})

	t.Run("only the original row on the window names nothing", func(t *testing.T) {
		var n int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM instrument_register
			WHERE instrument_key = $1 AND valid_from = '2026-01-01' AND supersedes_record_id IS NULL`, key,
		).Scan(&n); err != nil {
			t.Fatalf("count unattributed rows on the window: %v", err)
		}
		if n != 1 {
			t.Errorf("%d rows on the window name nothing, want 1 (the original open row)", n)
		}
	})
}

// TestAliasRegisterEnforcesTheSchemesNodeKinds is the applies_to rule, which was declared and
// documented from the day the vocabulary was seeded and read by nothing until now: an LEI
// declares applies_to {ENTITY}, so it cannot alias a security.
func TestAliasRegisterEnforcesTheSchemesNodeKinds(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	insertAlias := func(scheme, value, node string) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO alias_register (id_scheme, id_value, node_id, valid_from, `+registerSpine+`)
			VALUES ($1, $2, $3, '2026-01-01', 'test', 'SEED_LOAD', 'scheme acceptance', 'test')`,
			scheme, value, node)
		return err
	}

	t.Run("an LEI cannot alias a security", func(t *testing.T) {
		err := insertAlias("LEI", "529900T8BM49AURSDO55", "sec-not-an-entity")
		if !raisedWith(err, "cannot alias") {
			t.Fatalf("LEI pointing at a SECURITY gave %v, want the scheme guard to refuse it", err)
		}
	})

	t.Run("an LEI on an entity lands", func(t *testing.T) {
		if err := insertAlias("LEI", "529900T8BM49AURSDO56", "em-an-entity"); err != nil {
			t.Fatalf("LEI pointing at an ENTITY was refused: %v", err)
		}
	})

	t.Run("a scheme declaring several kinds accepts each", func(t *testing.T) {
		// CONTRACT_ADDRESS is {SECURITY,ENTITY}.
		if err := insertAlias("CONTRACT_ADDRESS", "aaaa0000aaaa0000aaaa0000aaaa0000aaaa0001", "sec-a-token"); err != nil {
			t.Errorf("CONTRACT_ADDRESS on a SECURITY was refused: %v", err)
		}
		if err := insertAlias("CONTRACT_ADDRESS", "aaaa0000aaaa0000aaaa0000aaaa0000aaaa0002", "em-a-contract"); err != nil {
			t.Errorf("CONTRACT_ADDRESS on an ENTITY was refused: %v", err)
		}
	})
}

// TestRegistersRequireAnApproverWhereTheReasonCodeSaysSo closes the other declared-but-inert rule:
// change_reason_vocabulary.requires_approval. REPOINT and RETRACTION are among the five codes that
// carry it, so a re-point now needs a second identity.
func TestRegistersRequireAnApproverWhereTheReasonCodeSaysSo(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	insert := func(key, reason, actor, approver string) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO instrument_register
				(instrument_key, key_namespace, security_id, chain_id, valid_from,
				 actor, change_reason_code, change_reason, approved_by, source_system)
			VALUES ($1, 'token_address', 'sec-x', 1, '2026-01-01', $2, $3, 'approval acceptance', $4, 'test')`,
			key, actor, reason, approver)
		return err
	}
	t.Run("a REPOINT with no approver is refused", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO instrument_register
				(instrument_key, key_namespace, security_id, chain_id, valid_from, `+registerSpine+`)
			VALUES ('f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f101', 'token_address', 'sec-x', 1, '2026-01-01',
			        'test', 'REPOINT', 'no approver', 'test')`)
		if !raisedWith(err, "requires an approver") {
			t.Fatalf("REPOINT without approved_by gave %v, want a refusal", err)
		}
	})

	t.Run("an approver who is the actor is refused", func(t *testing.T) {
		err := insert("f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f102", "REPOINT", "curator", "curator")
		if !raisedWith(err, "must differ from actor") {
			t.Fatalf("self-approval gave %v, want a refusal — four-eyes needs two identities", err)
		}
	})

	t.Run("a REPOINT with a distinct approver lands", func(t *testing.T) {
		if err := insert("f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f103", "REPOINT", "curator", "reviewer"); err != nil {
			t.Fatalf("an approved REPOINT was refused: %v", err)
		}
	})

	t.Run("a code that needs no approval is unaffected", func(t *testing.T) {
		mustInsertInstrument(ctx, t, pool, newInstrumentRow("f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f104", "sec-x"))
	})
}

// TestInstrumentRegisterResolvesEveryHeldKeyToExactlyOneSecurity is the ticket's first
// acceptance criterion, over the seeded held book: 21 native keys, 14 securities, every key
// resolving through the current view and none of them twice. The many-to-one is the point —
// six USDC deployments are six keys and one security.
func TestInstrumentRegisterResolvesEveryHeldKeyToExactlyOneSecurity(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	t.Run("every seeded key resolves exactly once", func(t *testing.T) {
		var seeded, resolved int
		if err := pool.QueryRow(ctx, `
			SELECT (SELECT count(*) FROM instrument_register),
			       (SELECT count(*) FROM instrument_register_current)`,
		).Scan(&seeded, &resolved); err != nil {
			t.Fatalf("count seeded vs resolved: %v", err)
		}
		if seeded != 21 {
			t.Errorf("held book seeded %d rows, want 21", seeded)
		}
		if resolved != seeded {
			t.Errorf("%d of %d seeded keys resolve through the current view — an unresolved key is a position that cannot find its security", resolved, seeded)
		}
	})

	t.Run("the keys collapse onto 14 securities", func(t *testing.T) {
		var securities int
		if err := pool.QueryRow(ctx,
			`SELECT count(DISTINCT security_id) FROM instrument_register_current`,
		).Scan(&securities); err != nil {
			t.Fatalf("count securities: %v", err)
		}
		if securities != 14 {
			t.Errorf("held book resolves to %d securities, want 14", securities)
		}
	})

	// The count above is satisfied by any permutation of the seed, so pin the pairs themselves.
	// A transposition of two rows — the realistic generator bug — passes every count.
	t.Run("each held key resolves to its own security", func(t *testing.T) {
		for _, tc := range []struct {
			key      string
			chain    int32
			security string
		}{
			{"a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", 1, "sec-usdc"},
			{"833589fcd6edb6e08f4c7c32d4f71b54bda02913", 8453, "sec-usdc"},
			{"6b175474e89094c44da98b954eedeac495271d0f", 1, "sec-dai"},
			{"dac17f958d2ee523a2206206994597c13d831ec7", 1, "sec-usdt"},
			{"2c0adff8e114f3ca106051144353ac703d24b901", 43114, "sec-gaclo1"},
			{"a3931d71877c0e7a3148cb7eb4463524fec27fbd", 1, "sec-susds"},
			{"5875eee11cf8398102fdad704c9e96607675467a", 8453, "sec-susds"},
		} {
			var got string
			if err := pool.QueryRow(ctx, `
				SELECT security_id FROM instrument_register_current
				WHERE instrument_key = $1 AND chain_scope = $2`, tc.key, tc.chain,
			).Scan(&got); err != nil {
				t.Errorf("%s on chain %d does not resolve: %v", tc.key, tc.chain, err)
				continue
			}
			if got != tc.security {
				t.Errorf("%s on chain %d resolves %s, want %s", tc.key, tc.chain, got, tc.security)
			}
		}
	})

}

// TestInstrumentRegisterContentHashCoversTheStoredRow is what verifies the append guard's
// treatment of the generated chain_scope. Postgres computes a STORED generated column AFTER a
// BEFORE INSERT trigger, so the guard sees NULL for chain_scope; hashing that would store a
// digest no later verification could reproduce from the row. The guard drops every generated
// column instead, and chain_id — its only input — stays in the hash.
func TestInstrumentRegisterContentHashCoversTheStoredRow(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const key = "c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3"
	row := newInstrumentRow(key, "sec-usdc")
	row.chainID = chainID(8453)
	mustInsertInstrument(ctx, t, pool, row)

	var reproduces bool
	var scope int32
	if err := pool.QueryRow(ctx, `
		SELECT r.content_hash = sha256(convert_to((to_jsonb(r)
			   - 'record_id' - 'ingest_xid' - 'ingested_at' - 'content_hash'
			   - 'supersedes_record_id' - 'chain_scope')::text, 'UTF8')),
		       r.chain_scope
		FROM instrument_register r WHERE r.instrument_key = $1`, key,
	).Scan(&reproduces, &scope); err != nil {
		t.Fatalf("recompute content_hash: %v", err)
	}
	if !reproduces {
		t.Errorf("stored content_hash does not reproduce from the stored row — the tamper-evidence chain is only worth what a re-computation can check (AR-1.2)")
	}
	if scope != 8453 {
		t.Fatalf("chain_scope stored as %d, want 8453 — the assertion above is vacuous unless the column really is populated after the trigger", scope)
	}
}

// TestRegistersRejectAWriterSuppliedIngestXid pins that the wave-1 write boundary reaches the
// registers too: ingest_xid is the knowledge-time ordering key, and a writer that can set it can
// forge the order corrections resolve in (ADR-0006 §5).
func TestRegistersRejectAWriterSuppliedIngestXid(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	for _, tc := range []struct {
		name   string
		insert string
	}{
		{
			name: "instrument_register",
			insert: `INSERT INTO instrument_register
				(instrument_key, key_namespace, security_id, chain_id, valid_from, ingest_xid, ` + registerSpine + `)
				VALUES ('deadbeef', 'token_address', 'sec-x', 1, '2026-01-01', '1'::xid8, 'test', 'SEED_LOAD', 'forged', 'test')`,
		},
		{
			name: "alias_register",
			insert: `INSERT INTO alias_register
				(id_scheme, id_value, node_id, valid_from, ingest_xid, ` + registerSpine + `)
				VALUES ('LEI', '549300FORGED0000000X', 'em-x', '2026-01-01', '1'::xid8, 'test', 'SEED_LOAD', 'forged', 'test')`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pool.Exec(ctx, tc.insert)
			if !raisedWith(err, "ingest_xid is platform-assigned") {
				t.Fatalf("a writer-supplied ingest_xid gave %v, want the guard's own rejection — any error would pass a bare nil check (ADR-0006 §5)", err)
			}
		})
	}
}

// TestAliasRegisterClosesAWindowByAppending is why valid_to joined the alias key. An alias ends:
// an LEI lapses, a wallet changes hands. With valid_to outside the key the closing row collides
// with the open row it closes, leaving an UPDATE as the only route — and the append-only grants
// refuse that, so the fact would be unrecordable.
func TestAliasRegisterClosesAWindowByAppending(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const value = "7a2f1c9e4b8d6a05f3e2c1b0a9d8e7f6c5b4a3c1"
	openWindow := func() int64 {
		t.Helper()
		var id int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO alias_register (id_scheme, id_value, node_id, valid_from, valid_to, `+registerSpine+`)
			VALUES ('BLOCKCHAIN_ADDRESS', $1, 'em-holder-first', '2026-01-01', 'infinity',
			        'test', 'SEED_LOAD', 'alias acceptance', 'test')
			RETURNING record_id`, value).Scan(&id); err != nil {
			t.Fatalf("open the window: %v", err)
		}
		return id
	}
	closeWindow := func(supersedes any) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO alias_register (id_scheme, id_value, node_id, valid_from, valid_to,
			                            supersedes_record_id, `+registerSpine+`)
			VALUES ('BLOCKCHAIN_ADDRESS', $1, 'em-holder-first', '2026-01-01', '2026-06-01', $2,
			        'test', 'SEED_LOAD', 'alias acceptance', 'test')`, value, supersedes)
		return err
	}

	opened := openWindow()

	t.Run("a close that names nothing is refused", func(t *testing.T) {
		if err := closeWindow(nil); !raisedWith(err, "must name the row it closes") {
			t.Fatalf("an unattributed second row on the window gave %v, want a refusal — it would displace the open row by insert order alone", err)
		}
	})

	if err := closeWindow(opened); err != nil {
		t.Fatalf("close the window naming record %d: %v — valid_to in the key is what makes this an append", opened, err)
	}

	t.Run("the closed alias is absent from current", func(t *testing.T) {
		var rows int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM alias_register_current WHERE id_value = $1`, value,
		).Scan(&rows); err != nil {
			t.Fatalf("count current: %v", err)
		}
		if rows != 0 {
			t.Errorf("a closed alias still resolves through current (%d rows)", rows)
		}
	})

	t.Run("as_of inside the closed window still resolves it", func(t *testing.T) {
		var node string
		if err := pool.QueryRow(ctx,
			`SELECT node_id FROM alias_register_as_of('2026-03-01') WHERE id_value = $1`, value,
		).Scan(&node); err != nil {
			t.Fatalf("alias_register_as_of(2026-03-01): %v", err)
		}
		if node != "em-holder-first" {
			t.Errorf("as_of resolved %s, want em-holder-first", node)
		}
	})
}

// TestRegistersReplayWhatWasKnownAtASnapshot covers the _as_of(date, pg_snapshot) overload, which
// nothing exercised before. It is the whole reason ingest_xid exists: a correction appended after
// a calculation ran must not change what that calculation is replayed as having seen. The snapshot
// filter therefore precedes version resolution, which is also why these two functions cannot read
// the shared _latest views.
func TestRegistersReplayWhatWasKnownAtASnapshot(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const key = "c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0"
	mustInsertInstrument(ctx, t, pool, newInstrumentRow(key, "sec-before"))

	var snapshot string
	if err := pool.QueryRow(ctx, `SELECT pg_current_snapshot()::text`).Scan(&snapshot); err != nil {
		t.Fatalf("take snapshot: %v", err)
	}

	correction := newInstrumentRow(key, "sec-after")
	correction.validFrom = "2026-06-01"
	mustInsertInstrument(ctx, t, pool, correction)

	t.Run("the snapshot replays the mapping as it stood", func(t *testing.T) {
		var security string
		if err := pool.QueryRow(ctx,
			`SELECT security_id FROM instrument_register_as_of('2026-12-01'::date, $1::pg_snapshot) WHERE instrument_key = $2`,
			snapshot, key,
		).Scan(&security); err != nil {
			t.Fatalf("as_of with snapshot: %v", err)
		}
		if security != "sec-before" {
			t.Errorf("replay resolved %s, want sec-before — an append made after the snapshot leaked into it", security)
		}
	})

	t.Run("the same date without a snapshot sees the correction", func(t *testing.T) {
		var security string
		if err := pool.QueryRow(ctx,
			`SELECT security_id FROM instrument_register_as_of('2026-12-01'::date) WHERE instrument_key = $1`, key,
		).Scan(&security); err != nil {
			t.Fatalf("as_of without snapshot: %v", err)
		}
		if security != "sec-after" {
			t.Errorf("as_of resolved %s, want sec-after", security)
		}
	})

	t.Run("the alias register replays the same way", func(t *testing.T) {
		const value = "c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c1"
		insertAlias := func(node, validFrom string) {
			t.Helper()
			if _, err := pool.Exec(ctx, `
				INSERT INTO alias_register (id_scheme, id_value, node_id, valid_from, `+registerSpine+`)
				VALUES ('BLOCKCHAIN_ADDRESS', $1, $2, $3::date, 'test', 'SEED_LOAD', 'replay', 'test')`,
				value, node, validFrom); err != nil {
				t.Fatalf("insert alias %s: %v", node, err)
			}
		}
		insertAlias("em-holder-before", "2026-01-01")

		var snap string
		if err := pool.QueryRow(ctx, `SELECT pg_current_snapshot()::text`).Scan(&snap); err != nil {
			t.Fatalf("take snapshot: %v", err)
		}
		insertAlias("em-holder-after", "2026-06-01")

		var node string
		if err := pool.QueryRow(ctx,
			`SELECT node_id FROM alias_register_as_of('2026-12-01'::date, $1::pg_snapshot) WHERE id_value = $2`,
			snap, value,
		).Scan(&node); err != nil {
			t.Fatalf("alias as_of with snapshot: %v", err)
		}
		if node != "em-holder-before" {
			t.Errorf("replay resolved %s, want em-holder-before", node)
		}
	})
}
