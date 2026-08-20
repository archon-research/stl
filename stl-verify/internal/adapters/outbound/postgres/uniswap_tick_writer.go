package postgres

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
)

// The Uniswap tick tables share one append-on-change write path and differ only
// in name and in V3's initialized column.
var (
	uniswapV3TickWriter = newUniswapTickWriter("uniswap_v3_tick", true)
	uniswapV4TickWriter = newUniswapTickWriter("uniswap_v4_tick", false)
)

// uniswapTickRow is one tick observation in the shape the shared writer
// persists. initialized is V3-only; a writer whose table lacks the column
// neither writes nor compares it.
type uniswapTickRow struct {
	poolID                int64
	tick                  int
	blockNumber           int64
	blockVersion          int
	blockTimestamp        time.Time
	liquidityGross        *big.Int
	liquidityNet          *big.Int
	feeGrowthOutside0X128 *big.Int
	feeGrowthOutside1X128 *big.Int
	initialized           bool
}

// uniswapTickWriter persists the append-on-change rows of one Uniswap tick
// table. Its SQL is built once per table at package init so the write path
// below is shared verbatim.
type uniswapTickWriter struct {
	table          string
	hasInitialized bool
	selectSQL      string
	insertSQL      string
}

// newUniswapTickWriter builds one table's statements. valueColumns fixes the
// order of both the scan in readLatestTicks and the bind parameters in
// insertArgs; the three must be changed together.
func newUniswapTickWriter(table string, hasInitialized bool) uniswapTickWriter {
	valueColumns := []string{
		"liquidity_gross", "liquidity_net",
		"fee_growth_outside0_x128", "fee_growth_outside1_x128",
	}
	if hasInitialized {
		valueColumns = append(valueColumns, "initialized")
	}
	return uniswapTickWriter{
		table:          table,
		hasInitialized: hasInitialized,
		selectSQL:      latestUniswapTicksSQL(table, valueColumns),
		insertSQL:      insertUniswapTickSQL(table, valueColumns),
	}
}

// latestUniswapTicksSQL selects the latest row per (pool_id, tick) among the
// keys in ($1, $2), at or below the height in $3. The height bound is
// load-bearing: an out-of-order write (the gap backfiller republishing a missed
// block) compared against a NEWER row is dropped as unchanged whenever the two
// agree, leaving that block a permanent hole in the tick's history.
func latestUniswapTicksSQL(table string, valueColumns []string) string {
	selected := make([]string, 0, len(valueColumns))
	for _, column := range valueColumns {
		selected = append(selected, "t."+column)
	}
	return fmt.Sprintf(
		`SELECT DISTINCT ON (t.pool_id, t.tick)
		        t.pool_id, t.tick, t.block_number, t.block_version, %s
		 FROM %s t
		 JOIN unnest($1::bigint[], $2::int[]) AS k(pool_id, tick)
		   ON t.pool_id = k.pool_id AND t.tick = k.tick
		 WHERE t.block_number <= $3
		 ORDER BY t.pool_id, t.tick,
		          t.block_number DESC, t.block_version DESC, t.processing_version DESC`,
		strings.Join(selected, ", "), table)
}

func insertUniswapTickSQL(table string, valueColumns []string) string {
	columns := append([]string{
		"pool_id", "tick", "block_number", "block_version", "block_timestamp",
	}, valueColumns...)
	columns = append(columns, "build_id")

	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return fmt.Sprintf(
		`INSERT INTO %s (%s)
		 VALUES (%s)
		 ON CONFLICT (pool_id, tick, block_number, block_version, processing_version) DO NOTHING`,
		table, strings.Join(columns, ", "), strings.Join(placeholders, ","))
}

// writeTicks persists one block's tick rows. It locks every affected
// (pool_id, tick) slot in sorted order so the read-latest-then-insert decision
// is serialized against concurrent writers (db/migrations/AGENTS.md
// read-then-write rule), reads each slot's latest row at or below this block's
// height, and appends only the ticks that row does not already reflect.
//
// Each phase is one round-trip for the whole set — locks in a single unnest()
// Exec, the reads in a single DISTINCT ON query, the inserts in one pgx.Batch —
// because a dense pool's first touch covers O(10³) ticks and per-tick
// round-trips would hold the advisory locks for all of them.
//
// Ticks in one block are deduplicated per (pool_id, tick) upstream, so each key
// appears at most once here and needs no in-batch same-key sequencing.
func (w uniswapTickWriter) writeTicks(ctx context.Context, tx pgx.Tx, ticks []uniswapTickRow, buildID buildregistry.BuildID) error {
	if len(ticks) == 0 {
		return nil
	}

	blockNumber, err := w.sharedBlockNumber(ticks)
	if err != nil {
		return err
	}

	keys := distinctSortedUniswapTickKeys(ticks)
	if err := w.lockTickKeys(ctx, tx, keys); err != nil {
		return err
	}

	latest, err := w.readLatestTicks(ctx, tx, keys, blockNumber)
	if err != nil {
		return err
	}

	return w.insertChangedTicks(ctx, tx, ticks, latest, buildID)
}

// sharedBlockNumber returns the block every tick in the write belongs to. One
// SaveBlock is one block, and readLatestTicks bounds itself by that height, so
// a mixed batch would compare a tick against another block's row.
func (w uniswapTickWriter) sharedBlockNumber(ticks []uniswapTickRow) (int64, error) {
	blockNumber := ticks[0].blockNumber
	for _, t := range ticks[1:] {
		if t.blockNumber != blockNumber {
			return 0, fmt.Errorf("%s write spans blocks %d and %d: one SaveBlock is one block", w.table, blockNumber, t.blockNumber)
		}
	}
	return blockNumber, nil
}

// lockTickKeys acquires the per-slot advisory lock for every key in one
// round-trip via unnest(). keys must already be in the canonical sorted order
// (distinctSortedUniswapTickKeys) so concurrent SaveBlock transactions touching
// overlapping ticks acquire overlapping locks in the same order and never
// deadlock.
//
// The "<table>|<pool>|<tick>" lock domain is deliberately distinct from the
// pv-trigger's row-identity key ("uvt|…" / "u4t|…"): this guards the app-level
// read-latest-then-insert decision, the trigger guards processing_version
// assignment. They must not be harmonized (same curve_config precedent, see
// curve_repository.go).
func (w uniswapTickWriter) lockTickKeys(ctx context.Context, tx pgx.Tx, keys []uniswapTickKey) error {
	lockKeys := make([]string, len(keys))
	for i, k := range keys {
		lockKeys[i] = fmt.Sprintf("%s|%d|%d", w.table, k.poolID, k.tick)
	}
	// pg_advisory_xact_lock is acquired left-to-right as unnest() yields rows,
	// preserving the sorted-key lock order.
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended(k, 0))
		 FROM unnest($1::text[]) WITH ORDINALITY AS u(k, ord)
		 ORDER BY ord`,
		lockKeys,
	); err != nil {
		return fmt.Errorf("locking %d %s slots: %w", len(keys), w.table, err)
	}
	return nil
}

// readLatestTicks fetches each key's latest row at or below blockNumber. Keys
// with no such row are absent from the map, which the caller treats as "insert
// unconditionally".
func (w uniswapTickWriter) readLatestTicks(ctx context.Context, tx pgx.Tx, keys []uniswapTickKey, blockNumber int64) (map[uniswapTickKey]uniswapTickValues, error) {
	poolIDs := make([]int64, len(keys))
	tickNums := make([]int32, len(keys))
	for i, k := range keys {
		poolIDs[i] = k.poolID
		tickNums[i] = int32(k.tick)
	}

	rows, err := tx.Query(ctx, w.selectSQL, poolIDs, tickNums, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("querying latest %s rows for %d slots: %w", w.table, len(keys), err)
	}
	defer rows.Close()

	latest := make(map[uniswapTickKey]uniswapTickValues, len(keys))
	for rows.Next() {
		var (
			poolID                int64
			tick                  int
			rowBlockNumber        int64
			blockVersion          int
			liquidityGross        pgtype.Numeric
			liquidityNet          pgtype.Numeric
			feeGrowthOutside0X128 pgtype.Numeric
			feeGrowthOutside1X128 pgtype.Numeric
			initialized           bool
		)
		dest := []any{&poolID, &tick, &rowBlockNumber, &blockVersion,
			&liquidityGross, &liquidityNet, &feeGrowthOutside0X128, &feeGrowthOutside1X128}
		if w.hasInitialized {
			dest = append(dest, &initialized)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("scanning latest %s row: %w", w.table, err)
		}
		values, convErr := toUniswapTickValues(rowBlockNumber, blockVersion, initialized,
			liquidityGross, liquidityNet, feeGrowthOutside0X128, feeGrowthOutside1X128)
		if convErr != nil {
			return nil, fmt.Errorf("reading latest %s row for pool=%d tick=%d: %w", w.table, poolID, tick, convErr)
		}
		latest[uniswapTickKey{poolID: poolID, tick: tick}] = values
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating latest %s rows: %w", w.table, err)
	}
	return latest, nil
}

// insertChangedTicks queues an INSERT for every tick whose latest row is absent
// or does not already reflect it, then sends them in one pgx.Batch. The INSERTs
// are ordinary statements, so the table's BEFORE INSERT ROW trigger assigns
// processing_version exactly as it would for a single-row insert.
func (w uniswapTickWriter) insertChangedTicks(
	ctx context.Context, tx pgx.Tx,
	ticks []uniswapTickRow,
	latest map[uniswapTickKey]uniswapTickValues,
	buildID buildregistry.BuildID,
) (err error) {
	batch := &pgx.Batch{}
	var queued int
	for i, t := range ticks {
		prior, hasPrior := latest[uniswapTickKey{poolID: t.poolID, tick: t.tick}]
		if hasPrior && w.unchanged(prior, t) {
			continue
		}
		args, argsErr := w.insertArgs(t, buildID)
		if argsErr != nil {
			return fmt.Errorf("tick %d: converting %s pool=%d tick=%d: %w", i, w.table, t.poolID, t.tick, argsErr)
		}
		batch.Queue(w.insertSQL, args...)
		queued++
	}

	if queued == 0 {
		return nil
	}

	br := tx.SendBatch(ctx, batch)
	defer func() {
		if closeErr := br.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing %s batch: %w", w.table, closeErr))
		}
	}()
	for i := range queued {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("inserting %s batch entry %d: %w", w.table, i, err)
		}
	}
	return nil
}

// insertArgs converts t's numerics and orders the bind parameters exactly as
// insertUniswapTickSQL names the columns: keys, value columns, build_id.
func (w uniswapTickWriter) insertArgs(t uniswapTickRow, buildID buildregistry.BuildID) ([]any, error) {
	args := []any{t.poolID, t.tick, t.blockNumber, t.blockVersion, t.blockTimestamp}
	for _, numeric := range []struct {
		column string
		value  *big.Int
	}{
		{"liquidity_gross", t.liquidityGross},
		{"liquidity_net", t.liquidityNet},
		{"fee_growth_outside0_x128", t.feeGrowthOutside0X128},
		{"fee_growth_outside1_x128", t.feeGrowthOutside1X128},
	} {
		converted, err := BigIntToNumericRequired(numeric.value, numeric.column)
		if err != nil {
			return nil, err
		}
		args = append(args, converted)
	}
	if w.hasInitialized {
		args = append(args, t.initialized)
	}
	return append(args, int(buildID)), nil
}

// unchanged reports whether the stored row already makes t redundant. Within
// one height a differing block_version is a reorg re-observation and must
// always append, even with identical values; across heights the two versions
// count different blocks' reorgs and comparing them would append a row claiming
// a change the chain never made, so only the values decide.
func (w uniswapTickWriter) unchanged(latest uniswapTickValues, t uniswapTickRow) bool {
	if latest.blockNumber == t.blockNumber && latest.blockVersion != t.blockVersion {
		return false
	}
	if w.hasInitialized && latest.initialized != t.initialized {
		return false
	}
	return bigIntEqual(latest.liquidityGross, t.liquidityGross) &&
		bigIntEqual(latest.liquidityNet, t.liquidityNet) &&
		bigIntEqual(latest.feeGrowthOutside0X128, t.feeGrowthOutside0X128) &&
		bigIntEqual(latest.feeGrowthOutside1X128, t.feeGrowthOutside1X128)
}

type uniswapTickKey struct {
	poolID int64
	tick   int
}

// distinctSortedUniswapTickKeys returns the affected slots in the canonical
// lock order, so concurrent SaveBlock transactions touching overlapping ticks
// never deadlock.
func distinctSortedUniswapTickKeys(ticks []uniswapTickRow) []uniswapTickKey {
	seen := make(map[uniswapTickKey]struct{}, len(ticks))
	for _, t := range ticks {
		seen[uniswapTickKey{poolID: t.poolID, tick: t.tick}] = struct{}{}
	}
	keys := make([]uniswapTickKey, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b uniswapTickKey) int {
		if a.poolID != b.poolID {
			return int(a.poolID - b.poolID)
		}
		return a.tick - b.tick
	})
	return keys
}

// uniswapTickValues holds a stored tick row's fields decoded to *big.Int for
// comparison against a candidate uniswapTickRow.
type uniswapTickValues struct {
	blockNumber           int64
	blockVersion          int
	liquidityGross        *big.Int
	liquidityNet          *big.Int
	feeGrowthOutside0X128 *big.Int
	feeGrowthOutside1X128 *big.Int
	initialized           bool
}

func toUniswapTickValues(
	blockNumber int64, blockVersion int, initialized bool,
	liquidityGross, liquidityNet, feeGrowthOutside0X128, feeGrowthOutside1X128 pgtype.Numeric,
) (uniswapTickValues, error) {
	var v uniswapTickValues
	var err error
	if v.liquidityGross, err = NumericToNullableBigInt(liquidityGross); err != nil {
		return v, fmt.Errorf("liquidity_gross: %w", err)
	}
	if v.liquidityNet, err = NumericToNullableBigInt(liquidityNet); err != nil {
		return v, fmt.Errorf("liquidity_net: %w", err)
	}
	if v.feeGrowthOutside0X128, err = NumericToNullableBigInt(feeGrowthOutside0X128); err != nil {
		return v, fmt.Errorf("fee_growth_outside0_x128: %w", err)
	}
	if v.feeGrowthOutside1X128, err = NumericToNullableBigInt(feeGrowthOutside1X128); err != nil {
		return v, fmt.Errorf("fee_growth_outside1_x128: %w", err)
	}
	v.blockNumber = blockNumber
	v.blockVersion = blockVersion
	v.initialized = initialized
	return v, nil
}
