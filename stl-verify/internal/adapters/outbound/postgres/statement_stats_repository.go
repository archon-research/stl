package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that StatementStatsRepository implements outbound.StatementStatsReader.
var _ outbound.StatementStatsReader = (*StatementStatsRepository)(nil)

// insertStatementsQuery reads the counters for every tracked INSERT against the
// connected database, one row per queryid.
//
// The GROUP BY is load-bearing, not tidiness. pg_stat_statements keys entries by
// (userid, dbid, queryid, toplevel), so one queryid legitimately returns SEVERAL
// rows — and how many depends on the reading role: an unprivileged role sees only
// its own, while pg_read_all_stats or tsdbadmin (what the pooler URL grants on
// Timescale Cloud) sees every role's. Ungrouped, the consumer's per-queryid
// baseline would keep whichever row arrived last while a delta was added for each,
// so a lower-count row would read as a counter reset and re-emit the full
// cumulative total every tick, forever. Summing to one row per queryid makes the
// reading independent of the role's visibility.
//
// The join on pg_database scopes the read to the current database:
// pg_stat_statements is cluster-wide, so without it a co-tenant database's writes
// would be attributed to our tables. total_exec_time is milliseconds, converted
// here so the port carries seconds and only this file knows the wire unit.
//
// min(query) picks one representative text per queryid: the grouped rows share a
// fingerprint, so they share a normalized statement and the choice is arbitrary.
//
// The anchored filter matches statements that BEGIN with INSERT INTO, so a
// CTE-prefixed write (`WITH ... INSERT INTO ...`) is not counted. That is a
// deliberate limitation, not an oversight: the target of such a statement cannot
// be read without parsing SQL properly, and every ingest path here issues plain
// INSERTs. It under-reports rather than misattributing — extend the filter and the
// target parser in db_statement_stats together if a CTE write is ever introduced.
const insertStatementsQuery = `
	SELECT s.queryid,
	       min(s.query) AS query,
	       sum(s.calls) AS calls,
	       sum(s.total_exec_time) / 1000.0 AS total_exec_seconds,
	       sum(s.rows) AS rows
	FROM pg_stat_statements s
	JOIN pg_database d ON d.oid = s.dbid
	WHERE d.datname = current_database()
	  AND s.query ~* '^\s*INSERT\s+INTO'
	GROUP BY s.queryid`

// StatementStatsRepository reads INSERT execution counters from
// pg_stat_statements.
type StatementStatsRepository struct {
	pool *pgxpool.Pool
}

// NewStatementStatsRepository creates a StatementStatsRepository.
func NewStatementStatsRepository(pool *pgxpool.Pool) *StatementStatsRepository {
	return &StatementStatsRepository{pool: pool}
}

// InsertStatements returns one row per tracked INSERT fingerprint.
//
// A missing or unreadable extension surfaces as an error rather than an empty
// result: reporting no statements would look identical to an idle database, and
// the exporter would publish a healthy tick while measuring nothing.
func (r *StatementStatsRepository) InsertStatements(ctx context.Context) ([]outbound.StatementStat, error) {
	rows, err := r.pool.Query(ctx, insertStatementsQuery)
	if err != nil {
		return nil, fmt.Errorf("querying pg_stat_statements (is the extension created and preloaded "+
			"via shared_preload_libraries?): %w", err)
	}
	defer rows.Close()

	var stats []outbound.StatementStat
	for rows.Next() {
		var s outbound.StatementStat
		if err := rows.Scan(&s.QueryID, &s.Query, &s.Calls, &s.TotalExecTimeSeconds, &s.Rows); err != nil {
			return nil, fmt.Errorf("scanning pg_stat_statements row: %w", err)
		}
		stats = append(stats, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating pg_stat_statements rows: %w", err)
	}
	return stats, nil
}
