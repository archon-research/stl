//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// VEC-407 contract: materialize_prime_allocation() projects allocation_position into position_state on
// the native per-instrument grain. One (prime, proxy, token) holding is one position; balance is a
// post-transaction balanceOf reading, so the last event in a block is that block's quantity, and
// direction and tx_amount are never read. One behaviour per function, each seeding its own database.

// The fixture's native identities. Proxies and tokens are 20-byte addresses, lowercase hex, no 0x.
const (
	allocProxyA = "1111111111111111111111111111111111111111"
	allocProxyB = "2222222222222222222222222222222222222222"
	allocTokenX = "3333333333333333333333333333333333333333"
	allocTokenY = "4444444444444444444444444444444444444444"
	allocVault  = "5555555555555555555555555555555555555555"
)

func allocInstrument(proxy, token string) string { return proxy + ":" + token }

// seedPrimeAllocationBase gives a test its own migrated database with one prime, two proxies and two
// tokens. Everything is looked up by its address, never by name or symbol, because the migrated
// database already carries real primes and tokens.
func seedPrimeAllocationBase(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT (chain_id) DO NOTHING`, nil},
		{`INSERT INTO prime (name, vault_address) VALUES ('itest-alloc', decode($1, 'hex'))`, []any{allocVault}},
		{`INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, decode($1, 'hex'), 'ITX', 18)`, []any{allocTokenX}},
		{`INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, decode($1, 'hex'), 'ITY', 6)`, []any{allocTokenY}},
	} {
		if _, err := pool.Exec(ctx, stmt.sql, stmt.args...); err != nil {
			t.Fatalf("seed base (%.40s): %v", stmt.sql, err)
		}
	}
	return ctx, pool
}

// alloc appends one allocation event. log_index and direction are parameters so a test can prove the
// pick is the highest log_index and that direction is not read.
func alloc(t *testing.T, ctx context.Context, pool *pgxpool.Pool, proxy, token string, balance float64, block, logIndex int, ts, direction string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO allocation_position
		    (chain_id, token_id, proxy_address, balance, block_number, block_version,
		     tx_hash, log_index, tx_amount, direction, created_at, prime_id)
		SELECT 1, t.id, decode($1, 'hex'), $3, $4, 0,
		       decode(lpad(to_hex($5::int), 8, '0'), 'hex'), $5::int, 0, $7, $6::timestamptz, p.id
		FROM token t, prime p
		WHERE t.chain_id = 1 AND t.address = decode($2, 'hex') AND p.name = 'itest-alloc'`,
		proxy, token, balance, block, logIndex, ts, direction); err != nil {
		t.Fatalf("alloc %s/%s block %d log %d: %v", proxy, token, block, logIndex, err)
	}
}

// seedPrimeAllocation seeds the fixture and runs the projection once.
//
//	A: proxy A / token X, block 100 three events (log 1,9,5) -> the log-9 balance, 60, is the block's
//	B: proxy A / token X, block 200 balance 0 -> the closing row for that position
//	C: proxy B / token X -> a second position for the same prime and token, at another proxy
//	D: proxy A / token Y -> a third position: same proxy, another token
//	E: proxy B / token Y, balance 0 then 0 -> never held anything, emits nothing
func seedPrimeAllocation(t *testing.T) (context.Context, *pgxpool.Pool, int64) {
	t.Helper()
	ctx, pool := seedPrimeAllocationBase(t)
	// The highest log_index deliberately does NOT carry the largest balance, so "last event wins" is
	// distinguishable from "largest value wins".
	alloc(t, ctx, pool, allocProxyA, allocTokenX, 50, 100, 1, "2026-01-01T00:00:00Z", "sweep")
	alloc(t, ctx, pool, allocProxyA, allocTokenX, 60, 100, 9, "2026-01-01T00:00:00Z", "out")
	alloc(t, ctx, pool, allocProxyA, allocTokenX, 70, 100, 5, "2026-01-01T00:00:00Z", "in")
	alloc(t, ctx, pool, allocProxyA, allocTokenX, 0, 200, 1, "2026-01-02T00:00:00Z", "out")
	alloc(t, ctx, pool, allocProxyB, allocTokenX, 25, 100, 1, "2026-01-01T00:00:00Z", "in")
	alloc(t, ctx, pool, allocProxyA, allocTokenY, 9, 100, 1, "2026-01-01T00:00:00Z", "in")
	alloc(t, ctx, pool, allocProxyB, allocTokenY, 0, 100, 1, "2026-01-01T00:00:00Z", "in")
	alloc(t, ctx, pool, allocProxyB, allocTokenY, 0, 200, 1, "2026-01-02T00:00:00Z", "in")
	var written int64
	if err := pool.QueryRow(ctx, `SELECT materialize_prime_allocation()`).Scan(&written); err != nil {
		t.Fatalf("materialize_prime_allocation: %v", err)
	}
	return ctx, pool, written
}

// A open + A close + C + D = 4 rows over 3 positions; E never held anything and emits nothing.
func TestMaterializePrimeAllocationProjectionShape(t *testing.T) {
	ctx, pool, written := seedPrimeAllocation(t)
	var rows, positions, badIDs int
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(DISTINCT position_id), count(*) FILTER (WHERE octet_length(position_id) <> 32)
		FROM position_state`).Scan(&rows, &positions, &badIDs); err != nil {
		t.Fatalf("shape: %v", err)
	}
	if written != 4 || rows != 4 || positions != 3 || badIDs != 0 {
		t.Errorf("written=%d rows=%d positions=%d bad ids=%d; want 4/4/3/0", written, rows, positions, badIDs)
	}
}

func TestMaterializePrimeAllocationPerPosition(t *testing.T) {
	ctx, pool, _ := seedPrimeAllocation(t)
	for _, c := range []struct {
		name       string
		instrument string
		wantRows   int
		wantQty    string
	}{
		{"proxy A / token X: the last event in the block sets the balance, then a close", allocInstrument(allocProxyA, allocTokenX), 2, "0"},
		{"proxy B / token X: the same token at another proxy is its own position", allocInstrument(allocProxyB, allocTokenX), 1, "25"},
		{"proxy A / token Y: another token at the same proxy is its own position", allocInstrument(allocProxyA, allocTokenY), 1, "9"},
		{"proxy B / token Y: never held anything, so nothing is emitted", allocInstrument(allocProxyB, allocTokenY), 0, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			var n int
			var qty string
			if err := pool.QueryRow(ctx, `
				SELECT count(*), coalesce((SELECT quantity::text FROM position_state
				  WHERE instrument_key = $1 ORDER BY block_number DESC LIMIT 1), '')
				FROM position_state WHERE instrument_key = $1`, c.instrument).Scan(&n, &qty); err != nil {
				t.Fatalf("query: %v", err)
			}
			if n != c.wantRows || qty != c.wantQty {
				t.Errorf("rows=%d latest quantity=%q; want %d and %q", n, qty, c.wantRows, c.wantQty)
			}
		})
	}
}

// The block's quantity is the highest log_index in it, not the first or the largest value. The winner
// is an 'out' while a 'sweep' sits at a LOWER log_index, so this distinguishes last-event-wins from
// the direction tiebreak -- with the sweep as the winner it could not.
func TestMaterializePrimeAllocationTakesTheLastEventInTheBlock(t *testing.T) {
	ctx, pool, _ := seedPrimeAllocation(t)
	var qty string
	if err := pool.QueryRow(ctx, `
		SELECT quantity::text FROM position_state
		 WHERE instrument_key = $1 AND block_number = 100`, allocInstrument(allocProxyA, allocTokenX)).Scan(&qty); err != nil {
		t.Fatal(err)
	}
	if qty != "60" {
		t.Errorf("block 100 stored %s; want 60, the balance at the highest log_index — 70 is the LARGEST (at log 5), 50 is the first event and a 'sweep', so this separates last-event-wins from both largest-wins and the direction tiebreak", qty)
	}
}

// Every row carries the chain and a NULL protocol: the first projection to use that combination, and
// deliberate, since a token at the prime's own proxy has no protocol contract.
func TestMaterializePrimeAllocationIsChainScopedWithNoProtocol(t *testing.T) {
	ctx, pool, _ := seedPrimeAllocation(t)
	var rows, withChain, withProtocol int
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE chain_id = 1), count(*) FILTER (WHERE protocol_id IS NOT NULL)
		FROM position_state`).Scan(&rows, &withChain, &withProtocol); err != nil {
		t.Fatalf("query: %v", err)
	}
	if withChain != rows || withProtocol != 0 {
		t.Errorf("%d of %d rows carry chain 1 and %d carry a protocol; want all chained and none with a protocol", withChain, rows, withProtocol)
	}
}

// Every row is ALLOCATION, and the observation instant is the event's own created_at rather than the
// run's clock — nothing else in the row would reveal a wrong timestamp, since equal timestamps trip
// no gate.
func TestMaterializePrimeAllocationDealTypeAndInstant(t *testing.T) {
	ctx, pool, _ := seedPrimeAllocation(t)
	var deals []string
	var instants []string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(array_agg(DISTINCT deal_type), '{}'),
		       coalesce(array_agg(DISTINCT to_char(block_timestamp AT TIME ZONE 'utc', 'YYYY-MM-DD HH24:MI:SS') ORDER BY to_char(block_timestamp AT TIME ZONE 'utc', 'YYYY-MM-DD HH24:MI:SS')), '{}')
		FROM position_state`).Scan(&deals, &instants); err != nil {
		t.Fatal(err)
	}
	if len(deals) != 1 || deals[0] != "ALLOCATION" {
		t.Errorf("deal types = %v; want only ALLOCATION", deals)
	}
	want := "2026-01-01 00:00:00,2026-01-02 00:00:00"
	if strings.Join(instants, ",") != want {
		t.Errorf("observation instants = %v; want the fixture's own event times %s", instants, want)
	}
}

// The holder is the prime's vault address, not its name or id.
func TestMaterializePrimeAllocationHolderIsTheVaultAddress(t *testing.T) {
	ctx, pool, _ := seedPrimeAllocation(t)
	var holders []string
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(array_agg(DISTINCT holder_id), '{}') FROM position_state`).Scan(&holders); err != nil {
		t.Fatal(err)
	}
	if len(holders) != 1 || holders[0] != allocVault {
		t.Errorf("holders = %v; want only the prime's vault address %s", holders, allocVault)
	}
}

// A proxy emptied and later refilled keeps all three observations: the open, the close, and the re-open.
func TestMaterializePrimeAllocationSurvivesARefill(t *testing.T) {
	ctx, pool := seedPrimeAllocationBase(t)
	alloc(t, ctx, pool, allocProxyA, allocTokenX, 40, 100, 1, "2026-01-01T00:00:00Z", "in")
	alloc(t, ctx, pool, allocProxyA, allocTokenX, 0, 200, 1, "2026-01-02T00:00:00Z", "out")
	alloc(t, ctx, pool, allocProxyA, allocTokenX, 15, 300, 1, "2026-01-03T00:00:00Z", "in")
	if _, err := pool.Exec(ctx, `SELECT materialize_prime_allocation()`); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	var series []string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(array_agg(block_number::text || '=' || quantity::text ORDER BY block_number), '{}')
		FROM position_state WHERE instrument_key = $1`, allocInstrument(allocProxyA, allocTokenX)).Scan(&series); err != nil {
		t.Fatal(err)
	}
	if strings.Join(series, ",") != "100=40,200=0,300=15" {
		t.Errorf("series = %v; want the open, the close and the refill", series)
	}
}

// The projection needs no unresolved-input guard, and this is why: the source cannot hold an
// allocation whose token or prime does not exist, so the view's joins can never drop a row.
func TestPrimeAllocationSourceCannotHoldAnUnresolvableReference(t *testing.T) {
	ctx, pool := seedPrimeAllocationBase(t)
	for _, c := range []struct{ name, sql, wantConstraint string }{
		{"a token_id with no token row",
			`INSERT INTO allocation_position (chain_id, token_id, proxy_address, balance, block_number,
			   block_version, tx_hash, log_index, tx_amount, direction, created_at, prime_id)
			 SELECT 1, 999999, decode('` + allocProxyA + `','hex'), 5, 100, 0, '\x01', 1, 0, 'in',
			   '2026-01-01T00:00:00Z'::timestamptz, p.id FROM prime p WHERE p.name = 'itest-alloc'`,
			"allocation_position_token_id_fkey"},
		{"a prime_id with no prime row",
			`INSERT INTO allocation_position (chain_id, token_id, proxy_address, balance, block_number,
			   block_version, tx_hash, log_index, tx_amount, direction, created_at, prime_id)
			 SELECT 1, t.id, decode('` + allocProxyA + `','hex'), 5, 100, 0, '\x01', 1, 0, 'in',
			   '2026-01-01T00:00:00Z'::timestamptz, 999999 FROM token t WHERE t.chain_id = 1 AND t.address = decode('` + allocTokenX + `','hex')`,
			"allocation_position_prime_id_fkey"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := pool.Exec(ctx, c.sql)
			if err == nil {
				t.Fatalf("the source accepted an unresolvable reference; the projection would then need a guard")
			}
			if !strings.Contains(err.Error(), c.wantConstraint) {
				t.Errorf("rejected by %v; want the %s foreign key", err, c.wantConstraint)
			}
		})
	}
}

// p_build_id is stamped on every appended row, and a second run appends nothing.
func TestMaterializePrimeAllocationStampsBuildIDAndIsIdempotent(t *testing.T) {
	ctx, pool := seedPrimeAllocationBase(t)
	alloc(t, ctx, pool, allocProxyA, allocTokenX, 40, 100, 1, "2026-01-01T00:00:00Z", "in")
	var first int64
	if err := pool.QueryRow(ctx, `SELECT materialize_prime_allocation(11)`).Scan(&first); err != nil {
		t.Fatalf("first run: %v", err)
	}
	var second int64
	if err := pool.QueryRow(ctx, `SELECT materialize_prime_allocation(11)`).Scan(&second); err != nil {
		t.Fatalf("second run: %v", err)
	}
	var stamped, rows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FILTER (WHERE build_id = 11), count(*) FROM position_state`).Scan(&stamped, &rows); err != nil {
		t.Fatal(err)
	}
	if first != 1 || second != 0 || stamped != 1 || rows != 1 {
		t.Errorf("first=%d second=%d stamped=%d rows=%d; want 1/0/1/1", first, second, stamped, rows)
	}
}

// A reprocess at the same coordinates is its own observation.
func TestMaterializePrimeAllocationKeepsEachProcessingVersion(t *testing.T) {
	ctx, pool := seedPrimeAllocationBase(t)
	alloc(t, ctx, pool, allocProxyA, allocTokenX, 40, 100, 1, "2026-01-01T00:00:00Z", "in")
	// assign_processing_version_allocation_position keys on the whole event identity including
	// tx_hash, log_index, direction and created_at, so a correction repeats all of it under a new
	// build_id. alloc() builds tx_hash from log_index, so log 1 gives '\x00000001'.
	if _, err := pool.Exec(ctx, `
		INSERT INTO allocation_position (chain_id, token_id, proxy_address, balance, block_number,
		   block_version, tx_hash, log_index, tx_amount, direction, created_at, prime_id, build_id)
		 SELECT 1, t.id, decode($1,'hex'), 44, 100, 0, decode('00000001','hex'), 1, 0, 'in',
		   '2026-01-01T00:00:00Z'::timestamptz, p.id, 1
		 FROM token t, prime p WHERE t.chain_id = 1 AND t.address = decode($2,'hex') AND p.name = 'itest-alloc'`,
		allocProxyA, allocTokenX); err != nil {
		t.Fatalf("seed the correction: %v", err)
	}
	var pv0, pv1 int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE processing_version = 0), count(*) FILTER (WHERE processing_version = 1)
		FROM allocation_position`).Scan(&pv0, &pv1); err != nil {
		t.Fatal(err)
	}
	if pv0 != 1 || pv1 != 1 {
		t.Fatalf("the source holds pv0=%d pv1=%d; the fixture must produce one of each or this tests nothing", pv0, pv1)
	}
	if _, err := pool.Exec(ctx, `SELECT materialize_prime_allocation()`); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	var pvs []string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(array_agg(processing_version::text || '=' || quantity::text ORDER BY processing_version), '{}')
		FROM position_state WHERE instrument_key = $1`, allocInstrument(allocProxyA, allocTokenX)).Scan(&pvs); err != nil {
		t.Fatal(err)
	}
	if len(pvs) != 2 || !strings.Contains(fmt.Sprint(pvs), "0=40") || !strings.Contains(fmt.Sprint(pvs), "1=44") {
		t.Errorf("stored %v; want both processing versions, 0=40 and 1=44", pvs)
	}
}

// A sweep row and an event row of one block both carry log_index 0 (a sweep has no transaction, so
// it takes log_index 0 and the zero tx hash), and allocation_position's PK leaves tx_hash and
// direction free within the projection's DISTINCT ON group. log_index alone therefore did not
// determine the pick, and the stored quantity depended on which row happened to be scanned first.
// Two positions receive the same pair in opposite ingest orders; both must store the same reading.
func TestMaterializePrimeAllocationPickIsTotalAcrossASweepEventTie(t *testing.T) {
	ctx, pool := seedPrimeAllocationBase(t)
	const proxyC = "6666666666666666666666666666666666666666"
	const proxyD = "7777777777777777777777777777777777777777"
	row := func(proxy, direction, txHash string, balance float64) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO allocation_position
			    (chain_id, token_id, proxy_address, balance, block_number, block_version,
			     tx_hash, log_index, tx_amount, direction, created_at, prime_id)
			SELECT 1, t.id, decode($1, 'hex'), $2, 300, 0, decode($3, 'hex'), 0, 0, $4,
			       '2026-01-03T00:00:00Z'::timestamptz, p.id
			FROM token t, prime p
			WHERE t.chain_id = 1 AND t.address = decode($5, 'hex') AND p.name = 'itest-alloc'`,
			proxy, balance, txHash, direction, allocTokenX); err != nil {
			t.Fatalf("row %s/%s: %v", proxy, direction, err)
		}
	}
	zeroTx := strings.Repeat("00", 32)
	realTx := strings.Repeat("11", 32)
	// proxy C: the event lands first, then the sweep; proxy D: the sweep first, then the event.
	row(proxyC, "in", realTx, 40)
	row(proxyC, "sweep", zeroTx, 41)
	row(proxyD, "sweep", zeroTx, 41)
	row(proxyD, "in", realTx, 40)
	if _, err := pool.Exec(ctx, `SELECT materialize_prime_allocation()`); err != nil {
		t.Fatalf("materialize_prime_allocation: %v", err)
	}
	var qtyC, qtyD string
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT quantity::text FROM position_state WHERE instrument_key = $1 AND block_number = 300),
		       (SELECT quantity::text FROM position_state WHERE instrument_key = $2 AND block_number = 300)`,
		allocInstrument(proxyC, allocTokenX), allocInstrument(proxyD, allocTokenX)).Scan(&qtyC, &qtyD); err != nil {
		t.Fatal(err)
	}
	if qtyC != qtyD {
		t.Errorf("the same sweep/event pair stored %s for one ingest order and %s for the other; the pick must not depend on arrival order", qtyC, qtyD)
	}
	// direction DESC ranks 'sweep' above 'in' and 'out', matching allocation_position_current, so the
	// sweep's reading is the one materialised.
	if qtyC != "41" {
		t.Errorf("stored %s; want 41, the sweep row that direction DESC ranks first", qtyC)
	}
}

// created_at is the last allocation_position PK column, and it is this view's block_timestamp. Left
// out of the order the pick was ingest-order bound: the same two rows stored 100 one way and 999 the
// other. Ordered created_at DESC above log_index, matching allocation_position_current's rank order.
func TestMaterializePrimeAllocationPickIsTotalOnCreatedAt(t *testing.T) {
	ctx, pool := seedPrimeAllocationBase(t)
	const proxyE = "8888888888888888888888888888888888888888"
	const proxyF = "9999999999999999999999999999999999999999"
	row := func(proxy, createdAt, balance string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO allocation_position
			    (chain_id, token_id, proxy_address, balance, block_number, block_version,
			     tx_hash, log_index, tx_amount, direction, created_at, prime_id)
			SELECT 1, t.id, decode($1, 'hex'), $2::numeric, 300, 0, decode($3, 'hex'), 0, 0, 'sweep',
			       $4::timestamptz, p.id
			FROM token t, prime p
			WHERE t.chain_id = 1 AND t.address = decode($5, 'hex') AND p.name = 'itest-alloc'`,
			proxy, balance, strings.Repeat("00", 32), createdAt, allocTokenX); err != nil {
			t.Fatalf("row %s/%s: %v", proxy, createdAt, err)
		}
	}
	// Identical rows but for created_at, ingested in opposite orders.
	row(proxyE, "2026-05-01T00:00:00Z", "100")
	row(proxyE, "2026-05-01T00:00:01Z", "999")
	row(proxyF, "2026-05-01T00:00:01Z", "999")
	row(proxyF, "2026-05-01T00:00:00Z", "100")
	if _, err := pool.Exec(ctx, `SELECT materialize_prime_allocation()`); err != nil {
		t.Fatalf("materialize_prime_allocation: %v", err)
	}
	var qtyE, qtyF, tsE string
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT quantity::text FROM position_state WHERE instrument_key = $1 AND block_number = 300),
		       (SELECT quantity::text FROM position_state WHERE instrument_key = $2 AND block_number = 300),
		       (SELECT to_char(block_timestamp, 'HH24:MI:SS') FROM position_state WHERE instrument_key = $1 AND block_number = 300)`,
		allocInstrument(proxyE, allocTokenX), allocInstrument(proxyF, allocTokenX)).Scan(&qtyE, &qtyF, &tsE); err != nil {
		t.Fatal(err)
	}
	if qtyE != qtyF {
		t.Errorf("stored %s for one ingest order and %s for the other; the pick must be a function of history", qtyE, qtyF)
	}
	if qtyE != "999" || tsE != "00:00:01" {
		t.Errorf("stored %s @ %s; want 999 @ 00:00:01, the latest created_at", qtyE, tsE)
	}

	// created_at must rank ABOVE log_index, as allocation_position_current ranks block_timestamp:
	// a higher log_index with an EARLIER created_at is the only input where the two orders differ.
	const proxyG = "aaaa000000000000000000000000000000000000"
	row(proxyG, "2026-05-01T00:00:00Z", "111")
	if _, err := pool.Exec(ctx, `
		INSERT INTO allocation_position
		    (chain_id, token_id, proxy_address, balance, block_number, block_version,
		     tx_hash, log_index, tx_amount, direction, created_at, prime_id)
		SELECT 1, t.id, decode($1, 'hex'), 222, 300, 0, decode($2, 'hex'), 0, 0, 'sweep',
		       '2026-05-01T00:00:01Z'::timestamptz, p.id
		FROM token t, prime p
		WHERE t.chain_id = 1 AND t.address = decode($3, 'hex') AND p.name = 'itest-alloc'`,
		proxyG, strings.Repeat("11", 32), allocTokenX); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE allocation_position SET log_index = 5
		WHERE proxy_address = decode($1, 'hex') AND balance = 111`, proxyG); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `SELECT materialize_prime_allocation()`); err != nil {
		t.Fatalf("materialize_prime_allocation: %v", err)
	}
	var qtyG string
	if err := pool.QueryRow(ctx, `SELECT quantity::text FROM position_state
		WHERE instrument_key = $1 AND block_number = 300`, allocInstrument(proxyG, allocTokenX)).Scan(&qtyG); err != nil {
		t.Fatal(err)
	}
	if qtyG != "222" {
		t.Errorf("stored %s; want 222, the later created_at -- log_index 5 carries the EARLIER instant, so ranking log_index first would store 111 and disagree with allocation_position_current", qtyG)
	}
}

// block_version was never exercised: every fixture row carries 0. A reorg at one height is two
// distinct observations, and dropping block_version from the pick loses one of them.
func TestMaterializePrimeAllocationKeepsEachBlockVersion(t *testing.T) {
	ctx, pool := seedPrimeAllocationBase(t)
	for _, c := range []struct{ bv, bal, ts string }{
		{"0", "700", "2026-06-01T00:00:00Z"},
		{"1", "800", "2026-06-01T00:00:05Z"},
	} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO allocation_position
			    (chain_id, token_id, proxy_address, balance, block_number, block_version,
			     tx_hash, log_index, tx_amount, direction, created_at, prime_id)
			SELECT 1, t.id, decode($1, 'hex'), $2::numeric, 600, $3::int, decode($4, 'hex'), 0, 0, 'in',
			       $5::timestamptz, p.id
			FROM token t, prime p
			WHERE t.chain_id = 1 AND t.address = decode($6, 'hex') AND p.name = 'itest-alloc'`,
			allocProxyA, c.bal, c.bv, strings.Repeat("0"+c.bv, 32), c.ts, allocTokenX); err != nil {
			t.Fatalf("reorg row bv=%s: %v", c.bv, err)
		}
	}
	if _, err := pool.Exec(ctx, `SELECT materialize_prime_allocation()`); err != nil {
		t.Fatalf("materialize_prime_allocation: %v", err)
	}
	var got string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(string_agg(block_version || '=' || quantity, ' ' ORDER BY block_version), '<none>')
		FROM position_state WHERE instrument_key = $1 AND block_number = 600`,
		allocInstrument(allocProxyA, allocTokenX)).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "0=700 1=800" {
		t.Errorf("stored %q; want both reorg versions as distinct observations", got)
	}
}

// The wrapper must resolve its own view and the shared function by name, not whatever the
// search_path happens to reach. SET search_path FROM CURRENT snapshots "$user", public, which
// still resolves per role at call time, so qualification is what actually protects it.
func TestMaterializePrimeAllocationPinsItsSearchPath(t *testing.T) {
	ctx, pool, _ := seedPrimeAllocation(t)

	var cfg []string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(proconfig, ARRAY[]::text[]) FROM pg_proc
		 WHERE proname = 'materialize_prime_allocation'`).Scan(&cfg); err != nil {
		t.Fatalf("reading proconfig: %v", err)
	}
	var pinned bool
	for _, c := range cfg {
		if strings.HasPrefix(c, "search_path=") {
			pinned = true
		}
	}
	if !pinned {
		t.Errorf("the wrapper pins no search_path (proconfig %v), so it resolves its view in the caller's path", cfg)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SET search_path = pg_catalog`); err != nil {
		t.Fatalf("setting a caller path without public: %v", err)
	}
	var written int64
	if err := conn.QueryRow(ctx, `SELECT public.materialize_prime_allocation()`).Scan(&written); err != nil {
		t.Fatalf("the wrapper failed under a caller search_path without public: %v", err)
	}
}

// A schema named after the calling role is searched before public under "$user", public, so an
// object placed there shadows an unqualified reference. The wrapper names both its view and the
// shared function with their schema, so neither can be swapped underneath it.
func TestMaterializePrimeAllocationResolvesPastAShadowingSchema(t *testing.T) {
	ctx, pool, _ := seedPrimeAllocation(t)

	var role string
	if err := pool.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		t.Fatalf("current_user: %v", err)
	}
	// A shadow that would be caught: it returns a sentinel instead of appending anything.
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+pgIdent(role)); err != nil {
		t.Fatalf("creating the shadowing schema: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE OR REPLACE FUNCTION `+pgIdent(role)+`.materialize_position_projection(regclass, integer)
	                             RETURNS bigint LANGUAGE sql AS $$ SELECT -1::bigint $$`); err != nil {
		t.Fatalf("creating the shadowing function: %v", err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+pgIdent(role)+` CASCADE`) })

	var got int64
	if err := pool.QueryRow(ctx, `SELECT public.materialize_prime_allocation()`).Scan(&got); err != nil {
		t.Fatalf("materialize_prime_allocation: %v", err)
	}
	if got == -1 {
		t.Error("the wrapper called the shadowing function, so its reference is unqualified")
	}
}

func pgIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
