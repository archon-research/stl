//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The position stack's invariants, end to end from a FRESH database through the real
// write path, over randomised histories, and then through a later feed into days that
// already have an answer.
//
// One database hosts every seed: each seed's holders are namespaced, so the seeds share
// a spine without sharing positions, and the reads run over other seeds' history too.
//
// POSITION_DAILY_SEEDS adds seeds beyond the committed set that CI runs. Keep the committed
// set small, because this package builds a database per fixture and is near its timeout.
func TestPositionDailyStackInvariants(t *testing.T) {
	seeds := envInt(t, "POSITION_DAILY_SEEDS", 6)
	multiplier := envInt(t, "POSITION_DAILY_SEED_MULTIPLIER", 7919)

	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	inv := &stackInvariants{ctx: ctx, t: t, pool: pool}

	t.Run("a fresh database reads nothing", func(t *testing.T) {
		for _, obj := range []struct{ kind, name string }{
			{"function", "position_daily_between"},
			{"function", "position_daily_on"},
		} {
			if !inv.objectExists(obj.kind, obj.name) {
				t.Errorf("a freshly migrated database has no %s %s", obj.kind, obj.name)
			}
		}
		if n := inv.readingCount(); n != 0 {
			t.Errorf("a freshly migrated database already reads %d row(s)", n)
		}
		inv.assertAll(t, "empty spine")
	})

	// Randomised histories through materialize_position_projection, in random batch order,
	// with the invariants checked part-way through so they meet a spine that is still growing.
	rng := rand.New(rand.NewSource(int64(multiplier)))
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed-%02d", seed), func(t *testing.T) {
			r := rand.New(rand.NewSource(int64(seed) * int64(multiplier)))
			rows := namespaceHolders(generateHistory(r), seed)
			view := fmt.Sprintf("pv_inv_%d", seed)

			var arrived []obsRow
			for bi, batch := range splitBatches(r, rows) {
				arrived = append(arrived, batch...)
				inv.materialize(view, arrived, bi)
				if r.Intn(2) == 0 {
					inv.assertAll(t, fmt.Sprintf("seed %d batch %d", seed, bi))
				}
			}
			inv.assertAll(t, fmt.Sprintf("seed %d", seed))
		})
	}

	// This covers today. The generated histories are all in the past, so without this the current
	// date is never read. There is no settling step: both reads carry it at once.
	t.Run("the open day reaches position_current and position_daily_between", func(t *testing.T) {
		today := inv.openDayObservation()
		var inCurrent, inDaily, inOn int
		if err := inv.pool.QueryRow(inv.ctx, `
			SELECT (SELECT count(*) FROM position_current WHERE position_id = $1),
			       (SELECT count(*) FROM position_daily_between('2000-01-01', '2100-01-01') WHERE position_id = $1),
			       (SELECT count(*) FROM position_daily_on((now() AT TIME ZONE 'utc')::date) WHERE position_id = $1)`, today).
			Scan(&inCurrent, &inDaily, &inOn); err != nil {
			t.Fatal(err)
		}
		if inCurrent != 1 || inDaily != 1 || inOn != 1 {
			t.Errorf("a position observed today has %d row(s) in position_current, %d in position_daily_between and %d in "+
				"position_daily_on(today); want 1 each", inCurrent, inDaily, inOn)
		}
		inv.assertAll(t, "after the open day")
	})

	// Pin every answer the stack has given so far. The later feed below must not
	// change any of it.
	pinnedAt := inv.dbNow()
	pinned := inv.readAsOf(pinnedAt)
	if len(pinned) == 0 {
		t.Fatal("no answers were recorded before the later feed; the phases below would prove nothing")
	}
	latestBefore := inv.readAsOf(inv.dbNow())

	// The later feed: four shapes arrive into days that already have an answer: a
	// correction to the winner, a reorg of its block, a higher block on the same day,
	// and an older block that must lose. Each is a legal spine append.
	t.Run("a later feed into answered days", func(t *testing.T) {
		if fed := inv.feedLate(rng); fed == 0 {
			t.Fatal("the later feed appended nothing, so this case is vacuous")
		}
		if latest := inv.readAsOf(inv.dbNow()); maps.Equal(latest, latestBefore) {
			t.Error("the later feed changed no day's answer; the corrections it appends are newer by construction")
		}
		inv.assertAll(t, "after the later feed")
	})

	t.Run("every earlier answer is still reproducible", func(t *testing.T) {
		again := inv.readAsOf(pinnedAt)
		if len(again) != len(pinned) {
			t.Errorf("the pinned read returns %d answers after the later feed, was %d", len(again), len(pinned))
		}
		for key, want := range pinned {
			if got := again[key]; got != want {
				t.Errorf("the answer for %s read %q at the pinned time and %q now", key, want, got)
			}
		}
	})
}

// namespaceHolders gives a seed its own positions, so seeds share a spine without
// sharing a position. position_id is the hash of chain, protocol, instrument and
// holder, so a distinct holder is a distinct position.
func namespaceHolders(rows []obsRow, seed int) []obsRow {
	out := make([]obsRow, len(rows))
	seen := map[string]string{}
	for i, r := range rows {
		ns, ok := seen[r.holder]
		if !ok {
			ns = fmt.Sprintf("%040x", seed*1000+len(seen)+10)
			seen[r.holder] = ns
		}
		r.holder = ns
		out[i] = r
	}
	return out
}

type stackInvariants struct {
	ctx  context.Context
	t    *testing.T
	pool *pgxpool.Pool
}

func (s *stackInvariants) objectExists(kind, name string) bool {
	s.t.Helper()
	q := map[string]string{
		"table":     `SELECT count(*) FROM pg_class WHERE relname = $1 AND relkind = 'r'`,
		"view":      `SELECT count(*) FROM pg_class WHERE relname = $1 AND relkind = 'v'`,
		"function":  `SELECT count(*) FROM pg_proc WHERE proname = $1 AND prokind = 'f'`,
		"procedure": `SELECT count(*) FROM pg_proc WHERE proname = $1 AND prokind = 'p'`,
	}[kind]
	var n int
	if err := s.pool.QueryRow(s.ctx, q, name).Scan(&n); err != nil {
		s.t.Fatalf("looking up %s %s: %v", kind, name, err)
	}
	return n > 0
}

func (s *stackInvariants) materialize(view string, rows []obsRow, batch int) {
	s.t.Helper()
	if _, err := s.pool.Exec(s.ctx, `CREATE OR REPLACE VIEW `+view+` AS `+valuesBody(rows)); err != nil {
		s.t.Fatalf("create %s (batch %d): %v", view, batch, err)
	}
	if _, err := s.pool.Exec(s.ctx,
		`SELECT materialize_position_projection($1::regclass, 0, $2)`, view, 9000+batch); err != nil {
		s.t.Fatalf("materialize %s (batch %d): %v", view, batch, err)
	}
}

// feedLate appends, for a sample of days that already have an answer, observations that a
// running system really produces after a day has closed: a correction at a higher
// processing_version, a reorg at a higher block_version, a higher block on the same
// day, and an older block that must lose. Written straight to the spine, because the
// materializer refuses a re-emission at a stored coordinate and these are new ones.
func (s *stackInvariants) feedLate(rng *rand.Rand) int {
	s.t.Helper()
	rows, err := s.pool.Query(s.ctx, `
		SELECT position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id,
		       block_number, block_version, processing_version, block_timestamp
		  FROM position_daily_between('2000-01-01', '2100-01-01') ORDER BY position_id, as_of_date`)
	if err != nil {
		s.t.Fatalf("read the answered days: %v", err)
	}
	type day struct {
		id           []byte
		date         time.Time
		chain        *int32
		proto        *int64
		inst, holder string
		block        int64
		bver, pver   int32
		ts           time.Time
	}
	var days []day
	for rows.Next() {
		var d day
		if err := rows.Scan(&d.id, &d.date, &d.chain, &d.proto, &d.inst, &d.holder,
			&d.block, &d.bver, &d.pver, &d.ts); err != nil {
			s.t.Fatal(err)
		}
		days = append(days, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		s.t.Fatal(err)
	}

	appended := 0
	for i, d := range days {
		// A sample, not every day: a feed that touched everything would not exercise
		// the days it leaves alone.
		if rng.Intn(3) != 0 {
			continue
		}
		// An off-chain position's block_number IS its instant in epoch seconds, which
		// the spine enforces, so only the same-block shapes are legal for one.
		shapes := []struct {
			block      int64
			bver, pver int32
			ts         time.Time
		}{
			{d.block, d.bver, d.pver + 1, d.ts},                   // correction
			{d.block, d.bver + 1, d.pver, d.ts},                   // reorg
			{d.block - 1, d.bver, d.pver, d.ts.Add(-time.Minute)}, // loses
		}
		if d.chain != nil {
			// Same UTC day, a higher block: a gap-fill that wins.
			later := d.ts.Add(time.Minute)
			if later.UTC().Format("2006-01-02") == d.date.UTC().Format("2006-01-02") {
				shapes = append(shapes, struct {
					block      int64
					bver, pver int32
					ts         time.Time
				}{d.block + 1, 0, 0, later})
			}
		}
		for _, sh := range shapes {
			tag, err := s.pool.Exec(s.ctx, `
				INSERT INTO position_state
				    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
				     block_number, block_version, processing_version, block_timestamp,
				     projection, build_id, deal_type)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'public.pv_late', 0, 'LOAN')
				ON CONFLICT DO NOTHING`,
				d.id, d.chain, d.proto, d.inst, d.holder, 500+i,
				sh.block, sh.bver, sh.pver, sh.ts)
			if err != nil {
				s.t.Fatalf("late feed for %x on %s: %v", d.id, d.date.Format("2006-01-02"), err)
			}
			appended += int(tag.RowsAffected())
		}
	}
	return appended
}

// assertAll runs every invariant and names the phase, so a failure says which property
// broke and when rather than only that something is wrong.
func (s *stackInvariants) assertAll(t *testing.T, phase string) {
	t.Helper()
	for _, inv := range []struct {
		name string
		run  func() string
	}{
		{"position_daily_between equals the spine argmax per (position, UTC date)", s.betweenEqualsSpineArgmax},
		{"position_daily_on over every observed date equals position_daily_between", s.onEqualsBetween},
		{"position_current still equals the spine argmax per position", s.currentEqualsSpineArgmax},
		{"position_current agrees with the newest date position_daily_between reads", s.cachesAgree},
		{"the as-of read is monotone: an earlier bound returns a subset", s.asOfIsMonotone},
	} {
		if bad := inv.run(); bad != "" {
			t.Errorf("[%s] %s: %s", phase, inv.name, bad)
		}
	}
}

// The oracle is a window function over position_state, computed independently of the
// reads' DISTINCT ON.
func (s *stackInvariants) betweenEqualsSpineArgmax() string {
	return s.diff(`
		WITH ranked AS (
		  SELECT position_id, (block_timestamp AT TIME ZONE 'utc')::date AS as_of_date,
		         quantity, block_number, block_version, processing_version, block_timestamp,
		         deal_type, holder_id, instrument_key,
		         row_number() OVER (PARTITION BY position_id, (block_timestamp AT TIME ZONE 'utc')::date
		           ORDER BY block_number DESC, block_version DESC, processing_version DESC,
		                    block_timestamp DESC) rn
		    FROM position_state),
		     oracle AS (SELECT position_id, as_of_date, quantity, block_number, block_version,
		                       processing_version, block_timestamp, deal_type, holder_id, instrument_key
		                  FROM ranked WHERE rn = 1),
		     got AS (SELECT position_id, as_of_date, quantity, block_number, block_version,
		                    processing_version, block_timestamp, deal_type, holder_id, instrument_key
		               FROM position_daily_between('2000-01-01', '2100-01-01'))
		SELECT (SELECT count(*) FROM (SELECT * FROM oracle EXCEPT ALL SELECT * FROM got) a),
		       (SELECT count(*) FROM (SELECT * FROM got EXCEPT ALL SELECT * FROM oracle) b),
		       COALESCE((SELECT a::text FROM (SELECT * FROM oracle EXCEPT ALL SELECT * FROM got) a LIMIT 1), '')`,
		"the spine implies", "position_daily_between holds that the spine does not")
}

// The one-date read, applied to every observed date, is the range read over every date, row for row.
func (s *stackInvariants) onEqualsBetween() string {
	const cols = `position_id, as_of_date, quantity, block_number, block_version, processing_version,
	              block_timestamp, deal_type, holder_id, instrument_key, projection, build_id, run_id, created_at`
	return s.diff(`
		WITH on_every_date AS (
		  SELECT `+cols+` FROM (SELECT DISTINCT (block_timestamp AT TIME ZONE 'utc')::date AS d FROM position_state) dates
		   CROSS JOIN LATERAL position_daily_on(dates.d)),
		     daily AS (SELECT `+cols+` FROM position_daily_between('2000-01-01', '2100-01-01'))
		SELECT (SELECT count(*) FROM (SELECT * FROM daily EXCEPT ALL SELECT * FROM on_every_date) a),
		       (SELECT count(*) FROM (SELECT * FROM on_every_date EXCEPT ALL SELECT * FROM daily) b),
		       COALESCE((SELECT a::text FROM (SELECT * FROM daily EXCEPT ALL SELECT * FROM on_every_date) a LIMIT 1), '')`,
		"position_daily_between holds", "position_daily_on holds that position_daily_between does not")
}

// position_current reads the same spine through the same ordering, so a change that
// broke one would likely break both.
func (s *stackInvariants) currentEqualsSpineArgmax() string {
	return s.diff(`
		WITH ranked AS (
		  SELECT position_id, quantity, block_number, block_version, processing_version,
		         block_timestamp, deal_type,
		         row_number() OVER (PARTITION BY position_id
		           ORDER BY block_number DESC, block_version DESC, processing_version DESC,
		                    block_timestamp DESC) rn
		    FROM position_state),
		     oracle AS (SELECT position_id, quantity, block_number, block_version,
		                       processing_version, block_timestamp, deal_type FROM ranked WHERE rn = 1),
		     got AS (SELECT position_id, quantity, block_number, block_version,
		                    processing_version, block_timestamp, deal_type FROM position_current)
		SELECT (SELECT count(*) FROM (SELECT * FROM oracle EXCEPT ALL SELECT * FROM got) a),
		       (SELECT count(*) FROM (SELECT * FROM got EXCEPT ALL SELECT * FROM oracle) b),
		       COALESCE((SELECT a::text FROM (SELECT * FROM oracle EXCEPT ALL SELECT * FROM got) a LIMIT 1), '')`,
		"the spine implies", "position_current holds that the spine does not")
}

// position_current and the newest date position_daily_between reads must name the same winner.
func (s *stackInvariants) cachesAgree() string {
	return s.count(`
		WITH newest_settled AS (
		    SELECT DISTINCT ON (position_id) position_id, quantity, block_number, block_version,
		           processing_version, block_timestamp, deal_type
		      FROM position_daily_between('2000-01-01', '2100-01-01') ORDER BY position_id, as_of_date DESC)
		SELECT count(*)
		  FROM newest_settled d
		  JOIN position_current c ON c.position_id = d.position_id
		 WHERE ((d.quantity, d.block_number, d.block_version, d.processing_version, d.block_timestamp)
		        IS DISTINCT FROM
		        (c.quantity, c.block_number, c.block_version, c.processing_version, c.block_timestamp)
		        OR d.deal_type IS DISTINCT FROM c.deal_type)`,
		"position(s) where position_current and the newest date position_daily_between reads disagree")
}

// The spine is append-only, so the as-of read only ever grows: every row visible at the
// earliest bound is still visible at the latest.
func (s *stackInvariants) asOfIsMonotone() string {
	return s.count(`
		-- An empty spine has no bounds; the sentinels make both sides empty rather than
		-- passing a NULL to the as-of read, which refuses one.
		WITH bounds AS (SELECT COALESCE(min(created_at), '-infinity'::timestamptz) AS lo,
		                       COALESCE(max(created_at), 'infinity'::timestamptz) AS hi
		                  FROM position_state)
		SELECT count(*) FROM (
		    SELECT position_id, as_of_date, block_number, block_version, processing_version, block_timestamp
		      FROM position_daily_between('2000-01-01', '2100-01-01', (SELECT lo FROM bounds))
		    EXCEPT ALL
		    SELECT position_id, (block_timestamp AT TIME ZONE 'utc')::date, block_number, block_version,
		           processing_version, block_timestamp
		      FROM position_state WHERE created_at <= (SELECT hi FROM bounds)) x`,
		"row(s) visible at the earliest bound are absent at the latest one")
}

func (s *stackInvariants) count(q, what string) string {
	s.t.Helper()
	var n int
	if err := s.pool.QueryRow(s.ctx, q).Scan(&n); err != nil {
		s.t.Fatalf("invariant query: %v", err)
	}
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("%d %s", n, what)
}

func (s *stackInvariants) diff(q, lhs, rhs string) string {
	s.t.Helper()
	var missing, extra int
	var example string
	if err := s.pool.QueryRow(s.ctx, q).Scan(&missing, &extra, &example); err != nil {
		s.t.Fatalf("invariant query: %v", err)
	}
	if missing == 0 && extra == 0 {
		return ""
	}
	return fmt.Sprintf("%d rows %s and are missing, %d %s; e.g. %s", missing, lhs, extra, rhs, example)
}

// openDayObservation appends one observation dated inside the current UTC day and
// returns its position id.
func (s *stackInvariants) openDayObservation() []byte {
	s.t.Helper()
	var id []byte
	if err := s.pool.QueryRow(s.ctx, `
		INSERT INTO position_state
		    (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
		     block_number, block_version, processing_version, block_timestamp,
		     projection, build_id, deal_type)
		VALUES (sha256('open-day'::bytea), 1, 10, 'inv-open', repeat('f', 40), 11,
		        900000, 0, 0, date_trunc('day', now() AT TIME ZONE 'utc') + interval '30 minutes',
		        'public.pv_open', 0, 'LOAN')
		RETURNING position_id`).Scan(&id); err != nil {
		s.t.Fatalf("append an observation on the open day: %v", err)
	}
	return id
}

func (s *stackInvariants) readingCount() int {
	s.t.Helper()
	var n int
	if err := s.pool.QueryRow(s.ctx, `SELECT count(*) FROM position_daily_between('2000-01-01', '2100-01-01')`).Scan(&n); err != nil {
		s.t.Fatal(err)
	}
	return n
}

func (s *stackInvariants) dbNow() time.Time {
	s.t.Helper()
	var at time.Time
	if err := s.pool.QueryRow(s.ctx, `SELECT clock_timestamp()`).Scan(&at); err != nil {
		s.t.Fatal(err)
	}
	return at
}

// readAsOf is every answer the stack gives at time T, keyed by (position, date).
func (s *stackInvariants) readAsOf(at time.Time) map[string]string {
	s.t.Helper()
	rows, err := s.pool.Query(s.ctx, `
		SELECT encode(position_id, 'hex') || '@' || as_of_date::text,
		       quantity::text || '/' || block_number::text || '.' || block_version::text || '.' ||
		       processing_version::text || '/' || COALESCE(deal_type, 'NULL')
		  FROM position_daily_between('2000-01-01', '2100-01-01', $1)`, at)
	if err != nil {
		s.t.Fatalf("as-of read: %v", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			s.t.Fatal(err)
		}
		out[k] = v
	}
	if err := rows.Err(); err != nil {
		s.t.Fatal(err)
	}
	return out
}
