// Package transformgen is the single source behind the transformation-layer
// migration. Given the schema_master register and the live raw schema (column
// lists + primary keys, read from information_schema), it resolves how each
// transformed row is built from its raw row (rename / cast / dimension-fill, the
// derived primary key, the partition column) and emits the DDL for the
// `transformed` schema: per governed table a hypertable, its change queue
// (_pending_<t>), the AFTER INSERT enqueue trigger, the incremental drain function
// _run_<t>(), and the out-of-band bootstrap function _bootstrap_<t>().
//
// It covers bucket 1 (VEC-484): the 13 governed tables with a native or renameable
// observation column and a raw PK. A regen-diff CI test regenerates from the
// register + live schema and compares (normalised: comments and whitespace
// stripped) against the committed migration, so the register and the migration
// cannot silently drift, and the CTAS / _run / _bootstrap projections of one table
// cannot diverge from each other.
//
// Emit status: the projection layer (this file) is complete; the queue-shape
// statement emitters and the static header/tail are built out incrementally, so
// GenerateBucket1 is not yet wired. See emit.go.
package transformgen

import (
	"fmt"

	"github.com/archon-research/stl/stl-verify/data_quality/schemamaster"
)

// RawColumn is a live column of a raw table, as reported by information_schema.
type RawColumn struct {
	Name    string
	Ordinal int
}

// RawSchema is the live shape of one raw public.<table>: its columns (in ordinal
// order) and its primary-key columns (in the order the constraint declares them).
type RawSchema struct {
	Table      string
	Columns    []RawColumn
	PrimaryKey []string
}

// sortedColumns returns the raw columns in ascending ordinal order.
func (s RawSchema) sortedColumns() []RawColumn {
	out := make([]RawColumn, len(s.Columns))
	copy(out, s.Columns)
	// information_schema is queried in ordinal order, but sort defensively.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].Ordinal > out[j].Ordinal; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// hasColumn reports whether name is a raw column of the table.
func (s RawSchema) hasColumn(name string) bool {
	for _, c := range s.Columns {
		if c.Name == name {
			return true
		}
	}
	return false
}

// The bucket-1 tables, in the order they appear in the committed migration.
var bucket1Tables = []string{
	"morpho_market_state",
	"morpho_market_position",
	"morpho_vault_state",
	"morpho_vault_position",
	"fluid_vault_state",
	"token_total_supply",
	"onchain_token_price",
	"maple_loan_state",
	"maple_loan_collateral",
	"maple_pool_state",
	"maple_sky_strategy_state",
	"maple_syrup_global_state",
	"offchain_token_price",
}

// Bucket1Tables returns the fixed set of bucket-1 table names, in migration order.
func Bucket1Tables() []string {
	out := make([]string, len(bucket1Tables))
	copy(out, bucket1Tables)
	return out
}

// projection is the resolved plan for one table: how the transformed row is built
// from the raw row, the join clause, and the derived PK / partition. It carries
// both the transformed names (renames applied) and the raw names, because the
// queue design needs the raw PK (for _pending and the join-back) and the raw
// observation column (for _bootstrap's window predicate).
type projection struct {
	table        string
	selectExprs  []string // full SELECT expression list, in emission order (transformed)
	joinClause   string   // "" or ` LEFT JOIN public."p" p ON ...`
	sourceAlias  string   // "s" when a join exists, "" otherwise
	pkColumns    []string // transformed PK columns (renames applied, processing_version placed)
	rawPK        []string // raw PK columns (raw names) — enqueued and joined back on
	partition    string   // transformed partition column (block_timestamp / snapshot_time)
	rawPartition string   // raw observation column (timestamp / synced_at / block_timestamp)
	columns      []string // all transformed columns, in SELECT emission order (filled, then raw)
	setColumns   []string // non-PK columns (columns minus PK), in the same order
}

// plan resolves the register + raw schema into a projection.
func plan(reg *schemamaster.Register, schema RawSchema) (projection, error) {
	fills := fillsFor(reg, schema.Table)
	joinClause, sourceAlias := joinFor(fills)

	filledExprs, filledCols := filledColumns(fills)

	rawExprs, rawTransObs, err := rawColumns(reg, schema, sourceAlias)
	if err != nil {
		return projection{}, err
	}

	pk := primaryKey(reg, schema)
	if len(pk) == 0 {
		return projection{}, fmt.Errorf("no primary key columns for %q", schema.Table)
	}

	partition, err := partitionColumn(schema, rawTransObs)
	if err != nil {
		return projection{}, err
	}

	rawPart, err := rawObservationColumn(schema)
	if err != nil {
		return projection{}, err
	}

	// columns are the transformed output names in SELECT emission order: filled
	// columns first (chain_id/protocol_id), then raw columns (renames applied).
	columns := append([]string{}, filledCols...)
	for _, c := range schema.sortedColumns() {
		columns = append(columns, canonicalName(reg, schema.Table, c.Name))
	}
	pkSet := make(map[string]bool, len(pk))
	for _, c := range pk {
		pkSet[c] = true
	}
	var set []string
	for _, c := range columns {
		if !pkSet[c] {
			set = append(set, c)
		}
	}

	return projection{
		table:        schema.Table,
		selectExprs:  append(filledExprs, rawExprs...),
		joinClause:   joinClause,
		sourceAlias:  sourceAlias,
		pkColumns:    pk,
		rawPK:        schema.PrimaryKey,
		partition:    partition,
		rawPartition: rawPart,
		columns:      columns,
		setColumns:   set,
	}, nil
}

// fillsFor returns the fills declared for a table, in register order.
func fillsFor(reg *schemamaster.Register, table string) []schemamaster.Fill {
	var out []schemamaster.Fill
	for _, f := range reg.Fills {
		if f.Table == table {
			out = append(out, f)
		}
	}
	return out
}

// joinFor derives the single LEFT JOIN clause and source alias. Every single-hop
// and two-hop fill for a bucket-1 table joins the same parent on the same key, so
// one join serves them all; with no joins the raw columns are unqualified.
func joinFor(fills []schemamaster.Fill) (clause, alias string) {
	for _, f := range fills {
		if f.Parent == "" {
			continue
		}
		clause = fmt.Sprintf(` LEFT JOIN public.%s p ON p.%s=s.%s`, quote(f.Parent), quote(f.Ref), quote(f.Key))
		return clause, "s"
	}
	return "", ""
}

// filledColumns renders the filled-column SELECT expressions (in fill order) and
// returns the canonical column names they produce.
func filledColumns(fills []schemamaster.Fill) (exprs, cols []string) {
	for _, f := range fills {
		exprs = append(exprs, fillExpr(f))
		cols = append(cols, f.Column)
	}
	return exprs, cols
}

// fillExpr renders one filled column. Single-hop reads from the joined parent p; a
// two-hop is a correlated subquery keyed on p's column. Const/BlockTime fills are
// bucket 2/3 and never occur here.
func fillExpr(f schemamaster.Fill) string {
	if f.ThenParent != "" {
		return fmt.Sprintf(`(SELECT l.%s FROM public.%s l WHERE l.%s=p.%s) AS %s`,
			quote(f.Column), quote(f.ThenParent), quote(f.ThenRef), quote(f.ThenKey), quote(f.Column))
	}
	return fmt.Sprintf(`p.%s`, quote(f.Column))
}

// rawColumns renders the raw-column SELECT expressions in ordinal order, applying
// any rename/cast transform, and reports the transformed observation column name.
func rawColumns(reg *schemamaster.Register, schema RawSchema, sourceAlias string) (exprs []string, transObs string, err error) {
	for _, c := range schema.sortedColumns() {
		expr, obs, err := rawColumnExpr(reg, schema.Table, c.Name, sourceAlias)
		if err != nil {
			return nil, "", err
		}
		exprs = append(exprs, expr)
		if obs != "" {
			transObs = obs
		}
	}
	return exprs, transObs, nil
}

// rawColumnExpr renders a single raw column, applying a transform if one exists.
func rawColumnExpr(reg *schemamaster.Register, table, col, sourceAlias string) (expr, obs string, err error) {
	ref := qualify(sourceAlias, col)
	t, ok := transformFor(reg, table, col)
	if !ok {
		if col == "block_timestamp" {
			return ref, col, nil
		}
		return ref, "", nil
	}
	switch t.Action {
	case "rename":
		if isObservationCanonical(t.Canonical) {
			obs = t.Canonical
		}
		return fmt.Sprintf(`%s AS %s`, ref, quote(t.Canonical)), obs, nil
	case "cast":
		e, err := castExpr(reg, t, ref)
		if err != nil {
			return "", "", err
		}
		if isObservationCanonical(t.Canonical) {
			obs = t.Canonical
		}
		return e, obs, nil
	default:
		return "", "", fmt.Errorf("unknown transform action %q for %s.%s", t.Action, table, col)
	}
}

// castExpr renders a cast transform: an epoch (int8 -> timestamptz) cast guarded
// with plausibility bounds, or a plain CAST to the canonical type's Postgres name.
func castExpr(reg *schemamaster.Register, t schemamaster.Transform, ref string) (string, error) {
	if schemamaster.NormalizeType(t.From) == "int8" && isObservationCanonical(t.Canonical) == false && t.GuardMin != nil {
		if t.GuardMax == nil {
			return "", fmt.Errorf("epoch cast %s.%s missing guard bounds", t.Table, t.Column)
		}
		return fmt.Sprintf(`CASE WHEN %s BETWEEN %d AND %d THEN to_timestamp(%s) END AS %s`,
			ref, *t.GuardMin, *t.GuardMax, ref, quote(t.Canonical)), nil
	}
	canon, ok := reg.Canonical[t.Canonical]
	if !ok {
		return "", fmt.Errorf("cast %s.%s references unknown canonical %q", t.Table, t.Column, t.Canonical)
	}
	pgType, err := pgTypeName(canon.Type)
	if err != nil {
		return "", fmt.Errorf("cast %s.%s: %w", t.Table, t.Column, err)
	}
	return fmt.Sprintf(`CAST(%s AS %s) AS %s`, ref, pgType, quote(t.Canonical)), nil
}

// pgTypeName maps a canonical short type token to the Postgres type used in a CAST.
func pgTypeName(canonical string) (string, error) {
	switch canonical {
	case "int8":
		return "bigint", nil
	case "int4":
		return "integer", nil
	default:
		return "", fmt.Errorf("no CAST target for canonical type %q", canonical)
	}
}

// primaryKey derives the transformed PK: the raw PK with renames applied. If
// processing_version is a raw column but absent from the raw PK, it is inserted
// immediately before the observation-time column.
func primaryKey(reg *schemamaster.Register, schema RawSchema) []string {
	var pk []string
	rawHasProc := false
	for _, col := range schema.PrimaryKey {
		if col == "processing_version" {
			rawHasProc = true
		}
	}
	insertProc := schema.hasColumn("processing_version") && !rawHasProc

	for _, col := range schema.PrimaryKey {
		name := canonicalName(reg, schema.Table, col)
		if insertProc && isObservationCanonical(name) {
			pk = append(pk, "processing_version")
		}
		pk = append(pk, name)
	}
	return pk
}

// partitionColumn is the transformed observation time the hypertable partitions on.
func partitionColumn(schema RawSchema, rawTransObs string) (string, error) {
	if rawTransObs != "" {
		return rawTransObs, nil
	}
	if schema.hasColumn("block_timestamp") {
		return "block_timestamp", nil
	}
	return "", fmt.Errorf("no observation column resolves for %q", schema.Table)
}

// rawObservationColumn returns the raw observation column name (synced_at,
// timestamp, or a native block_timestamp), used for the bootstrap window predicate
// and the _pending time column.
func rawObservationColumn(schema RawSchema) (string, error) {
	for _, name := range []string{"synced_at", "timestamp", "block_timestamp"} {
		if schema.hasColumn(name) {
			return name, nil
		}
	}
	return "", fmt.Errorf("no observation column (synced_at / timestamp / block_timestamp) for %q", schema.Table)
}

// transformFor returns the transform for a table+column, if any.
func transformFor(reg *schemamaster.Register, table, col string) (schemamaster.Transform, bool) {
	for _, t := range reg.Transforms {
		if t.Table == table && t.Column == col {
			return t, true
		}
	}
	return schemamaster.Transform{}, false
}

// canonicalName maps a raw column to its transformed name.
func canonicalName(reg *schemamaster.Register, table, col string) string {
	if t, ok := transformFor(reg, table, col); ok {
		return t.Canonical
	}
	return col
}

// isObservationCanonical reports whether a canonical name is a partition observation time.
func isObservationCanonical(name string) bool {
	return name == "block_timestamp" || name == "snapshot_time"
}

// qualify prefixes a column with the source alias when one exists.
func qualify(alias, col string) string {
	if alias == "" {
		return quote(col)
	}
	return alias + "." + quote(col)
}

// quote double-quotes a SQL identifier.
func quote(ident string) string {
	return `"` + ident + `"`
}
