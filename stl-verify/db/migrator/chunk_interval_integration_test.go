//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// One hypertable's configured chunk interval. want is an INTERVAL literal for a
// time-partitioned table and a plain block count for the one partitioned on
// block_number; byBlockNumber picks which of the two dimensions columns carries it.
type chunkIntervalCase struct {
	table         string
	want          string
	byBlockNumber bool
}

// protocolEventChunkSeconds is protocol_event's interval in seconds, for the fixture
// that has to build a timestamp range narrower than one chunk.
const protocolEventChunkSeconds = 4 * 24 * 60 * 60

// Every hypertable 20260827_120000_widen_chunk_intervals.sql retunes. Each interval
// comes from that table's measured daily ingest rate, sized so one active uncompressed
// chunk plus its indexes stays inside 25% of shared_buffers; the arithmetic is in the
// migration's header.
func widenedChunkIntervals() []chunkIntervalCase {
	return []chunkIntervalCase{
		{table: "protocol_event", want: "4 days"},
		{table: "morpho_vault_position", want: "14 days"},
		{table: "allocation_position", want: "30 days"},
		{table: "onchain_token_price", want: "30 days"},
		{table: "borrower_collateral", want: "30 days"},
		{table: "morpho_market_position", want: "30 days"},
		{table: "morpho_market_state", want: "30 days"},
		{table: "sparklend_reserve_data", want: "1000000", byBlockNumber: true},
	}
}

// A few hundred MB spread over 150-355 one-day chunks costs hundreds of MB of planner
// and executor memory per query, because the cost tracks the chunk COUNT rather than the
// data volume (VEC-663). Each interval below is what keeps that count in the low tens.
func TestOverChunkedHypertablesCarryWidenedChunkIntervals(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	for _, tc := range widenedChunkIntervals() {
		t.Run(tc.table, func(t *testing.T) {
			got, matches := configuredChunkInterval(t, ctx, pool, tc)
			if !matches {
				t.Errorf("chunk interval = %s; want %s", got, tc.want)
			}
		})
	}
}

// configuredChunkInterval reads one hypertable's interval and reports both its rendered
// value and whether it equals the case's want. The comparison happens in SQL because an
// interval's text rendering is not canonical -- 30 days can come back as 720:00:00 --
// while interval equality is.
func configuredChunkInterval(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tc chunkIntervalCase) (got string, matches bool) {
	t.Helper()

	column, cast := "time_interval", "interval"
	if tc.byBlockNumber {
		column, cast = "integer_interval", "bigint"
	}
	// The transformed schema carries same-named hypertables of its own, so the schema
	// filter is what keeps this reading the ingest table.
	query := fmt.Sprintf(`
		SELECT %[1]s::text, %[1]s = $2::%[2]s
		FROM timescaledb_information.dimensions
		WHERE hypertable_schema = 'public' AND hypertable_name = $1 AND dimension_number = 1`, column, cast)

	if err := pool.QueryRow(ctx, query, tc.table, tc.want).Scan(&got, &matches); err != nil {
		t.Fatalf("read chunk interval of %s: %v", tc.table, err)
	}
	return got, matches
}

// The catalogue value is only worth setting if it is the boundary TimescaleDB actually
// cuts on, so pin that separately on the table with the narrowest new interval.
func TestNewChunksSpanTheConfiguredInterval(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	seedOneChunkWindow(t, ctx, pool)

	if n := chunkCount(t, ctx, pool, "protocol_event"); n != 1 {
		t.Errorf("four daily rows inside one %d-day window landed in %d chunks; want 1",
			protocolEventChunkSeconds/(24*60*60), n)
	}
}

// seedOneChunkWindow writes four rows a day apart, starting on a chunk boundary so the
// window cannot straddle two chunks. Under the 1-day interval this replaces, the same
// four rows opened four chunks.
func seedOneChunkWindow(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	// Triggers and FK checks off: a chunk-boundary fixture has no use for a parent row
	// or a generated processing_version, and writing them itself keeps the setup to one
	// statement.
	if err := inReplicaRoleTx(ctx, pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO protocol_event (chain_id, protocol_id, block_number, block_version, tx_hash,
			                            log_index, contract_address, event_name, event_data, created_at,
			                            processing_version, build_id)
			SELECT 1, 1, 1000000 + g, 0, decode(lpad(to_hex(g + 1), 64, '0'), 'hex'),
			       1, '\x01'::bytea, 'event', '{}'::jsonb,
			       to_timestamp(floor(extract(epoch FROM timestamptz '2035-06-01 00:00:00+00') / $1) * $1)
			           + (g * interval '1 day'),
			       0, 0
			FROM generate_series(0, 3) AS g`, protocolEventChunkSeconds)
		return err
	}); err != nil {
		t.Fatalf("seed protocol_event: %v", err)
	}
}
