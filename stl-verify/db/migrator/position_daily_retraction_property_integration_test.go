//go:build integration

package migrator_test

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"
)

// coord is the full resolution key of one appended row: the spine coordinate the writer copied,
// plus the correction axis this table owns.
type coord struct {
	block, bv, pv int
	ts            string
	seq           int
}

// after reports whether c outranks o under the read's ordering, which is the only ranking rule the
// model knows. It is written from the ORDER BY rather than from the SQL, so it is an independent
// oracle and not a second copy of the query.
func (c coord) after(o coord) bool {
	if c.block != o.block {
		return c.block > o.block
	}
	if c.bv != o.bv {
		return c.bv > o.bv
	}
	if c.pv != o.pv {
		return c.pv > o.pv
	}
	if c.ts != o.ts {
		return c.ts > o.ts
	}
	return c.seq > o.seq
}

// modelRow is one row the model believes the table holds.
type modelRow struct {
	c         coord
	qty       int
	retracted bool
	at        time.Time // when it was appended, for the as-of axis
}

// model is an independent reimplementation of what position_daily should answer, built from the
// events the test issues rather than from the database.
type model struct {
	spine map[string][]modelRow // position -> observations, keyed by date via ts
	daily map[string][]modelRow // "position|date" -> appended rows
}

func newModel() *model {
	return &model{spine: map[string][]modelRow{}, daily: map[string][]modelRow{}}
}

func dateOf(ts string) string { return ts[:10] }

func key(id, date string) string { return id + "|" + date }

// observe records a spine observation.
func (m *model) observe(id string, r modelRow) { m.spine[id] = append(m.spine[id], r) }

// crystallize appends each settled day's winning observation, if that exact coordinate is not
// already present -- the writer's ON CONFLICT DO NOTHING.
func (m *model) crystallize(at time.Time) {
	for id, obs := range m.spine {
		winners := map[string]modelRow{}
		for _, o := range obs {
			d := dateOf(o.c.ts)
			if w, ok := winners[d]; !ok || o.c.after(w.c) {
				winners[d] = o
			}
		}
		for d, w := range winners {
			k := key(id, d)
			present := false
			for _, existing := range m.daily[k] {
				if existing.c == w.c {
					present = true
					break
				}
			}
			if !present {
				m.daily[k] = append(m.daily[k], modelRow{c: w.c, qty: w.qty, at: at})
			}
		}
	}
}

// winner is the highest-ranked appended row for a key among those appended by at.
func (m *model) winner(k string, at time.Time) (modelRow, bool) {
	var best modelRow
	found := false
	for _, r := range m.daily[k] {
		if r.at.After(at) {
			continue
		}
		if !found || r.c.after(best.c) {
			best, found = r, true
		}
	}
	return best, found
}

// retract appends a tombstone at the winner's coordinate and the next correction_seq. Reports
// whether the procedure should have appended a row.
func (m *model) retract(id, date string, at time.Time) (appended bool, absent bool) {
	k := key(id, date)
	if len(m.daily[k]) == 0 {
		return false, true
	}
	w, _ := m.winner(k, at)
	if w.retracted {
		return false, false
	}
	t := w
	t.c.seq = w.c.seq + 1
	t.retracted = true
	t.at = at
	m.daily[k] = append(m.daily[k], t)
	return true, false
}

// read is what position_daily should return as of at: the winning row per key, dropped when that
// winner is retracted. Absence and a zero quantity are different answers, so the map holds only
// keys that are present.
func (m *model) read(at time.Time) map[string]int {
	out := map[string]int{}
	for k := range m.daily {
		w, ok := m.winner(k, at)
		if !ok || w.retracted {
			continue
		}
		out[k] = w.qty
	}
	return out
}

// dbRead is the same answer taken from the database through the as-of function.
func (f *positionDailyFixture) dbRead(ids []string, at time.Time) map[string]int {
	f.t.Helper()
	out := map[string]int{}
	rows, err := f.pool.Query(f.ctx, `
		SELECT encode(position_id, 'hex'), as_of_date::text, quantity::int
		  FROM position_daily_as_of($1)`, at)
	if err != nil {
		f.t.Fatalf("read position_daily_as_of(%s): %v", at, err)
	}
	defer rows.Close()
	hexOf := map[string]string{}
	for _, id := range ids {
		hexOf[f.positionHex(id)] = id
	}
	for rows.Next() {
		var h, d string
		var q int
		if err := rows.Scan(&h, &d, &q); err != nil {
			f.t.Fatal(err)
		}
		if id, ours := hexOf[h]; ours {
			out[key(id, d)] = q
		}
	}
	if err := rows.Err(); err != nil {
		f.t.Fatalf("position_daily_as_of iteration: %v", err)
	}
	return out
}

func (f *positionDailyFixture) positionHex(id string) string {
	f.t.Helper()
	var h string
	if err := f.pool.QueryRow(f.ctx, `SELECT encode(sha256($1::bytea), 'hex')`, id).Scan(&h); err != nil {
		f.t.Fatalf("hash %s: %v", id, err)
	}
	return h
}

func diff(want, got map[string]int) string {
	var problems []string
	for k, w := range want {
		if g, ok := got[k]; !ok {
			problems = append(problems, fmt.Sprintf("%s: model says %d, the database has no row", k, w))
		} else if g != w {
			problems = append(problems, fmt.Sprintf("%s: model says %d, the database says %d", k, w, g))
		}
	}
	for k, g := range got {
		if _, ok := want[k]; !ok {
			problems = append(problems, fmt.Sprintf("%s: the database says %d, the model says the key is absent", k, g))
		}
	}
	sort.Strings(problems)
	return strings.Join(problems, "; ")
}

// onDate is the model's observations for one position on one UTC date.
func onDate(obs []modelRow, date string) []modelRow {
	var out []modelRow
	for _, o := range obs {
		if dateOf(o.c.ts) == date {
			out = append(out, o)
		}
	}
	return out
}

// The invariants the retraction mechanism rests on, over randomised interleavings of observation,
// crystallization and withdrawal rather than hand-picked fixtures. The oracle is written from the
// ordering rule in Go, so agreement means the SQL implements that rule -- not that two copies of
// the same query agree.
//
// Four invariants per step: the reading equals the model; no appended row is ever modified; the
// table only grows; and every as-of snapshot taken earlier still reproduces the answer given then.
func TestPositionDailyRetractionInvariantsOverRandomHistories(t *testing.T) {
	f := newPositionDailyFixture(t)

	const seeds = 8
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed-%02d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(seed) * 104729))
			m := newModel()

			ids := []string{
				fmt.Sprintf("prop-%02d-a", seed),
				fmt.Sprintf("prop-%02d-b", seed),
			}
			dates := []string{"2026-02-01", "2026-02-02", "2026-02-03"}
			blocks := []int{100, 200, 300}

			type snapshot struct {
				at   time.Time
				want map[string]int
			}
			var snaps []snapshot
			images := f.rowImages()

			for step := 0; step < 45; step++ {
				switch n := rng.Intn(10); {
				case n < 5: // a new spine observation, sometimes a reorg or a reprocess
					id := ids[rng.Intn(len(ids))]
					date := dates[rng.Intn(len(dates))]
					o := dailyObs{
						qty:      rng.Intn(900) + 1,
						block:    blocks[rng.Intn(len(blocks))],
						bv:       rng.Intn(2),
						pv:       rng.Intn(2),
						ts:       date + "T0" + fmt.Sprint(rng.Intn(3)+1) + ":00:00Z",
						dealType: "LOAN",
					}
					// Half the time, tie an existing observation on every leg above block_timestamp
					// and vary only the instant, so the last leg of the ordering actually decides.
					if peers := onDate(m.spine[id], date); len(peers) > 0 && rng.Intn(2) == 0 {
						peer := peers[rng.Intn(len(peers))]
						o.block, o.bv, o.pv = peer.c.block, peer.c.bv, peer.c.pv
						o.ts = date + "T0" + fmt.Sprint(rng.Intn(3)+1) + ":00:00Z"
					}
					// The spine's PK forbids a repeat of one coordinate, and the fixture would fail
					// the insert; skip a duplicate rather than treat it as an event.
					dup := false
					c := coord{block: o.block, bv: o.bv, pv: o.pv, ts: o.ts}
					for _, ex := range m.spine[id] {
						if ex.c == c {
							dup = true
							break
						}
					}
					if dup {
						continue
					}
					f.observe(id, o)
					m.observe(id, modelRow{c: c, qty: o.qty})

				case n < 8: // a crystallization
					f.crystallize()
					m.crystallize(f.dbNow())

				default: // a withdrawal of a day that may or may not be there
					id := ids[rng.Intn(len(ids))]
					date := dates[rng.Intn(len(dates))]
					at := f.dbNow()
					wantAppended, wantAbsent := m.retract(id, date, at)
					n, err := f.callRetract(id, date, "VEC-636", "property run")
					switch {
					case wantAbsent:
						if err == nil {
							t.Fatalf("step %d: retracting %s on %s was accepted; the model holds no rows for it", step, id, date)
						}
						continue
					case err != nil:
						t.Fatalf("step %d: retract %s on %s: %v", step, id, date, err)
					case wantAppended && n != 1:
						t.Fatalf("step %d: retract reported %d, the model expected one appended row", step, n)
					case !wantAppended && n != 0:
						t.Fatalf("step %d: retract reported %d, the model expected none -- the day was already withdrawn", step, n)
					}
				}

				// The reading equals the model, at every step rather than only at the end.
				at := f.dbNow()
				if d := diff(m.read(at), f.dbRead(ids, at)); d != "" {
					t.Fatalf("step %d: the reading and the model disagree: %s", step, d)
				}

				// Append-only: every row that existed is byte-identical and in the same physical
				// place, and the table never shrinks.
				now := f.rowImages()
				for k, img := range images {
					if got, ok := now[k]; !ok {
						t.Fatalf("step %d: row %s disappeared", step, k)
					} else if got != img {
						t.Fatalf("step %d: row %s changed:\nbefore %s\nafter  %s", step, k, img, got)
					}
				}
				if len(now) < len(images) {
					t.Fatalf("step %d: the table shrank from %d rows to %d", step, len(images), len(now))
				}
				images = now

				if rng.Intn(4) == 0 {
					snaps = append(snaps, snapshot{at: at, want: m.read(at)})
				}
			}

			// Every snapshot still reproduces the answer it gave, after everything that followed.
			for i, s := range snaps {
				if d := diff(s.want, f.dbRead(ids, s.at)); d != "" {
					t.Errorf("snapshot %d at %s no longer reproduces: %s", i, s.at.Format(time.RFC3339Nano), d)
				}
			}
		})
	}
}
