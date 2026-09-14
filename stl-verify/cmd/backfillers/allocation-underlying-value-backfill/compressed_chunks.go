package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const targetHypertable = "allocation_position"

type compressedChunk struct {
	rangeStart time.Time
	rangeEnd   time.Time
}

// range_end is exclusive and after/before follow candidateQuery's half-open
// convention, so overlap is start < before AND end > after. A NULL after opens
// the window at the beginning of history.
const compressedChunksQuery = `
	SELECT range_start, range_end
	FROM timescaledb_information.chunks
	WHERE hypertable_name = $1
	  AND is_compressed
	  AND range_start < $2
	  AND ($3::timestamptz IS NULL OR range_end > $3)
	ORDER BY range_start
`

// ensureTargetChunksAreDecompressed refuses a writing pass whose target chunks
// are still compressed.
//
// TimescaleDB resolves the key conflict against the columnstore before the
// BEFORE INSERT trigger runs, so the tuple still carries processing_version 0 —
// the version the original row holds — and ON CONFLICT DO NOTHING discards it
// without an error. Clearing that needs decompress_chunk, which demands
// hypertable ownership the app role does not hold, so the run stops here and
// names the operator step (VEC-759).
func ensureTargetChunksAreDecompressed(ctx context.Context, pool *pgxpool.Pool, cfg cliConfig) error {
	chunks, err := compressedChunksInRange(ctx, pool, cfg.after, cfg.before)
	if err != nil {
		return fmt.Errorf("checking %s for compressed chunks: %w", targetHypertable, err)
	}
	if len(chunks) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%d compressed %s chunk(s) cover this run's window %s: a write into a compressed chunk is "+
			"discarded before the processing_version trigger runs, so the run would report success and "+
			"persist nothing. Have an owner of %s decompress them, re-run, then recompress: %s",
		len(chunks), targetHypertable, windowDescription(cfg), targetHypertable,
		decompressRecipe(chunks),
	)
}

func compressedChunksInRange(ctx context.Context, pool *pgxpool.Pool, after, before time.Time) ([]compressedChunk, error) {
	var afterArg any
	if !after.IsZero() {
		afterArg = after
	}

	rows, err := pool.Query(ctx, compressedChunksQuery, targetHypertable, before, afterArg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []compressedChunk
	for rows.Next() {
		var c compressedChunk
		if err := rows.Scan(&c.rangeStart, &c.rangeEnd); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func windowDescription(cfg cliConfig) string {
	from := "the beginning of history"
	if !cfg.after.IsZero() {
		from = cfg.after.UTC().Format(time.RFC3339)
	}
	return fmt.Sprintf("[%s, %s)", from, cfg.before.UTC().Format(time.RFC3339))
}

// decompressRecipe spells out the SQL an operator runs, bounded by the span the
// blocking chunks cover.
func decompressRecipe(chunks []compressedChunk) string {
	oldest, newest := chunks[0].rangeStart, chunks[0].rangeEnd
	for _, c := range chunks[1:] {
		if c.rangeStart.Before(oldest) {
			oldest = c.rangeStart
		}
		if c.rangeEnd.After(newest) {
			newest = c.rangeEnd
		}
	}
	span := fmt.Sprintf("show_chunks('%s', older_than => '%s'::timestamptz, newer_than => '%s'::timestamptz)",
		targetHypertable, newest.UTC().Format(time.RFC3339), oldest.UTC().Format(time.RFC3339))
	// if_compressed/if_not_compressed keep the recipe re-runnable: the span can
	// also cover chunks that are already in the store the operator is asking for.
	return fmt.Sprintf(
		"SELECT decompress_chunk(c, if_compressed => true) FROM %s c;"+
			"  -- afterwards: SELECT compress_chunk(c, if_not_compressed => true) FROM %s c;",
		span, span)
}
