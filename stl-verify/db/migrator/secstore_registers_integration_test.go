//go:build integration

package migrator_test

import (
	"context"
	"errors"
	"strconv"
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
	mainnet := newInstrumentRow(key, "sec-gaclo1")
	base := newInstrumentRow(key, "sec-gaclo1")
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

	for _, want := range []int32{1, 8453} {
		var scope int32
		if err := pool.QueryRow(ctx, `
			SELECT chain_scope FROM instrument_register_current
			WHERE instrument_key = $1 AND chain_scope = coalesce($2::int4, 0)`, key, want,
		).Scan(&scope); err != nil {
			t.Errorf("resolve chain %d: %v — the read predicate the position stream uses", want, err)
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

// TestInstrumentRegisterRejectsOneKeyUnderTwoNamespaces pins the reason key_namespace is NOT in
// the primary key. A position row carries instrument_key and chain, never a namespace, so if one
// key could sit under two namespaces the read would have no way to choose between them. Leaving
// the namespace out of the key turns that into a write-time collision instead of a silent
// ambiguity at read time — which is only safe while the namespaces' key SHAPES stay distinct,
// and this is what notices if they stop being.
func TestInstrumentRegisterRejectsOneKeyUnderTwoNamespaces(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	const key = "T616-ALLOCATOR-A"
	ilk := newInstrumentRow(key, "sec-sky-debt")
	ilk.namespace = "sky_ilk"
	mustInsertInstrument(ctx, t, pool, ilk)

	collision := newInstrumentRow(key, "sec-something-else")
	collision.namespace = "loan_address"
	if err := insertInstrument(ctx, t, pool, collision); !uniqueViolation(err) {
		t.Fatalf("same key under a second namespace gave %v, want a unique violation", err)
	}
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

	t.Run("no key appears under two namespaces", func(t *testing.T) {
		var ambiguous int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM (
				SELECT instrument_key, chain_scope
				FROM instrument_register
				GROUP BY instrument_key, chain_scope
				HAVING count(DISTINCT key_namespace) > 1
			) AS x`,
		).Scan(&ambiguous); err != nil {
			t.Fatalf("scan for cross-namespace keys: %v", err)
		}
		if ambiguous != 0 {
			t.Errorf("%d key(s) appear under more than one namespace; a position row carries no namespace, so the read cannot choose", ambiguous)
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
			if _, err := pool.Exec(ctx, tc.insert); err == nil {
				t.Fatal("a writer-supplied ingest_xid was accepted; it is platform-assigned (ADR-0006 §5)")
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

	insertAlias := func(validToSQL, node string) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO alias_register (id_scheme, id_value, node_id, valid_from, valid_to, `+registerSpine+`)
			VALUES ('BLOCKCHAIN_ADDRESS', '7a2f1c9e4b8d6a05f3e2c1b0a9d8e7f6c5b4a3c1', $1, '2026-01-01', `+validToSQL+`,
			        'test', 'SEED_LOAD', 'alias acceptance', 'test')`, node)
		return err
	}

	if err := insertAlias("'infinity'", "em-holder-first"); err != nil {
		t.Fatalf("open the window: %v", err)
	}
	if err := insertAlias("'2026-06-01'", "em-holder-first"); err != nil {
		t.Fatalf("close the window: %v — valid_to in the key is what makes this an append", err)
	}

	t.Run("the closed alias is absent from current", func(t *testing.T) {
		var rows int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM alias_register_current
			WHERE id_value = '7a2f1c9e4b8d6a05f3e2c1b0a9d8e7f6c5b4a3c1'`,
		).Scan(&rows); err != nil {
			t.Fatalf("count current: %v", err)
		}
		if rows != 0 {
			t.Errorf("a closed alias still resolves through current (%d rows)", rows)
		}
	})

	t.Run("as_of inside the closed window still resolves it", func(t *testing.T) {
		var node string
		if err := pool.QueryRow(ctx, `
			SELECT node_id FROM alias_register_as_of('2026-03-01')
			WHERE id_value = '7a2f1c9e4b8d6a05f3e2c1b0a9d8e7f6c5b4a3c1'`,
		).Scan(&node); err != nil {
			t.Fatalf("alias_register_as_of(2026-03-01): %v", err)
		}
		if node != "em-holder-first" {
			t.Errorf("as_of resolved %s, want em-holder-first", node)
		}
	})
}
