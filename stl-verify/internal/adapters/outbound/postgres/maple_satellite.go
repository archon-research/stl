package postgres

import (
	"context"
	"crypto/md5"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Frozen hashdiff encoding. Changing either constant shifts every hash and
// forces a false "change" on the next sync, so they are fixed forever and the
// SQL backfill in the maple-registry-satellites migration mirrors them
// (md5 over the same canonical string).
const (
	hashdiffFieldSep = "\x1f" // ASCII unit separator, between fields
	hashdiffNullSep  = "\x1e" // ASCII record separator, stands in for SQL NULL
)

// metaHashdiff computes the change-detection hash over a satellite row's
// editorial fields. Each field is rendered to its SQL ::text form by the
// caller (bool as "true"/"false", int as decimal) and passed as *string, with
// nil meaning SQL NULL. md5 is sufficient here: this is change detection over
// tiny registries, not security, and it is callable identically in Postgres
// without a pgcrypto extension.
func metaHashdiff(fields []*string) []byte {
	var sb strings.Builder
	for i, f := range fields {
		if i > 0 {
			sb.WriteString(hashdiffFieldSep)
		}
		if f == nil {
			sb.WriteString(hashdiffNullSep)
		} else {
			sb.WriteString(*f)
		}
	}
	sum := md5.Sum([]byte(sb.String()))
	return sum[:]
}

func boolText(b bool) string { return strconv.FormatBool(b) }

func intText(i int) string { return strconv.Itoa(i) }

// metaRow is one satellite append candidate: a hub id and its editorial column
// values, in the order of the editorialCols passed to appendMeta. The hashdiff
// is derived from these same values (renderHashFields), so the hashed set and
// the stored set cannot drift apart.
type metaRow struct {
	id        int64
	editorial []any
}

// renderHashFields renders editorial values to their canonical hashdiff text
// form: string as-is, bool as "true"/"false", int as decimal, and *string as
// its value or nil (the SQL-NULL sentinel). It mirrors the per-column ::text
// rendering the SQL backfill uses. An unsupported type is a programmer error
// (a new editorial column type added without teaching the encoder).
func renderHashFields(editorial []any) ([]*string, error) {
	fields := make([]*string, len(editorial))
	for i, v := range editorial {
		switch t := v.(type) {
		case string:
			s := t
			fields[i] = &s
		case bool:
			s := boolText(t)
			fields[i] = &s
		case int:
			s := intText(t)
			fields[i] = &s
		case *string:
			fields[i] = t
		default:
			return nil, fmt.Errorf("hashdiff: unsupported editorial field type %T at index %d", v, i)
		}
	}
	return fields, nil
}

// appendMetaSQL builds the per-row append-on-change statement for a satellite.
// The row is inserted only when the hub's latest satellite row carries a
// different hashdiff (or none exists yet); ON CONFLICT keeps a same-synced_at
// retry a no-op. Columns: idCol, synced_at, editorialCols..., hashdiff,
// build_id.
//
// The change check compares against MAX(synced_at), so it assumes monotonic,
// in-order syncs (Maple's cron cadence): an out-of-order synced_at would be
// compared against the newest row rather than its temporal predecessor.
func appendMetaSQL(table, idCol string, editorialCols []string) string {
	cols := append([]string{idCol, "synced_at"}, editorialCols...)
	cols = append(cols, "hashdiff", "build_id")
	placeholders := make([]string, len(cols))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	hashdiffParam := len(editorialCols) + 3 // idCol(1) + synced_at(2) + editorial(k)
	return fmt.Sprintf(
		`INSERT INTO %s (%s)
		 SELECT %s
		 WHERE NOT EXISTS (
		     SELECT 1 FROM %s m
		     WHERE m.%s = $1
		       AND m.synced_at = (SELECT MAX(synced_at) FROM %s WHERE %s = $1)
		       AND m.hashdiff = $%d
		 )
		 ON CONFLICT (%s, synced_at) DO NOTHING`,
		table, strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
		table, idCol, table, idCol, hashdiffParam,
		idCol,
	)
}

// appendMeta appends satellite rows whose editorial values changed since the
// hub's latest satellite row. It is idempotent: an unchanged entity inserts
// nothing (the hashdiff guard), and a same-cycle retry inserts nothing (the
// (id, synced_at) conflict). Unlike the state-snapshot inserts, a no-op here is
// the normal expected outcome, so there is no checkDedupedRows accounting: a
// skipped row means "no editorial change", not a dropped write.
//
// Every appended row is is_present = TRUE; tombstones (is_present = FALSE) are
// not emitted yet — no deletion-detection path exists, so the column is
// forward-looking scaffolding the *_current views already honor.
func (r *MapleGraphQLRepository) appendMeta(ctx context.Context, tx pgx.Tx, table, idCol string, editorialCols []string, syncedAt time.Time, rows []metaRow) (err error) {
	if len(rows) == 0 {
		return nil
	}
	sql := appendMetaSQL(table, idCol, editorialCols)
	batch := &pgx.Batch{}
	for _, row := range rows {
		if len(row.editorial) != len(editorialCols) {
			return fmt.Errorf("appending %s: %d editorial values for %d columns", table, len(row.editorial), len(editorialCols))
		}
		fields, ferr := renderHashFields(row.editorial)
		if ferr != nil {
			return fmt.Errorf("appending %s: %w", table, ferr)
		}
		args := make([]any, 0, len(editorialCols)+4)
		args = append(args, row.id, syncedAt)
		args = append(args, row.editorial...)
		args = append(args, metaHashdiff(fields), int(r.buildID))
		batch.Queue(sql, args...)
	}

	br := tx.SendBatch(ctx, batch)
	defer func() {
		if closeErr := br.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("closing %s meta batch: %w", table, closeErr)
		}
	}()
	for range rows {
		if _, execErr := br.Exec(); execErr != nil {
			return fmt.Errorf("appending %s row: %w", table, execErr)
		}
	}
	return nil
}
