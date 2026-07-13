package transformgen

import (
	"fmt"
	"strings"

	"github.com/archon-research/stl/stl-verify/data_quality/schemamaster"
)

// tableCfg is the per-table storage config that is not derivable from the register
// or the raw schema (operational choices): the hypertable chunk interval and the
// compression segment-by / order-by / age. Kept here alongside the generator; a
// follow-up could move it into the register.
type tableCfg struct {
	chunk string // create_hypertable chunk_time_interval
	seg   string // compress_segmentby
	ord   string // compress_orderby
	ci    string // add_compression_policy interval
}

var tableConfigs = map[string]tableCfg{
	"morpho_market_state":      {"1 day", "morpho_market_id", "block_number DESC, block_version DESC, processing_version DESC", "2 days"},
	"morpho_market_position":   {"1 day", "morpho_market_id, user_id", "block_number DESC, block_version DESC, processing_version DESC", "2 days"},
	"morpho_vault_state":       {"1 day", "morpho_vault_id", "block_number DESC, block_version DESC, processing_version DESC", "2 days"},
	"morpho_vault_position":    {"1 day", "morpho_vault_id, user_id", "block_number DESC, block_version DESC, processing_version DESC", "2 days"},
	"fluid_vault_state":        {"7 days", "fluid_vault_id", "block_timestamp DESC, processing_version DESC", "14 days"},
	"token_total_supply":       {"7 days", "chain_id, token_id", "block_number DESC, block_version DESC, processing_version DESC", "2 days"},
	"onchain_token_price":      {"1 day", "oracle_id, token_id", "block_number DESC, block_version DESC, processing_version DESC", "2 days"},
	"maple_loan_state":         {"7 days", "maple_loan_id", "snapshot_time DESC, processing_version DESC", "2 days"},
	"maple_loan_collateral":    {"7 days", "maple_loan_id", "snapshot_time DESC, processing_version DESC", "2 days"},
	"maple_pool_state":         {"7 days", "maple_pool_id", "snapshot_time DESC, processing_version DESC", "2 days"},
	"maple_sky_strategy_state": {"7 days", "maple_sky_strategy_id", "snapshot_time DESC, processing_version DESC", "2 days"},
	"maple_syrup_global_state": {"7 days", "chain_id", "snapshot_time DESC, processing_version DESC", "2 days"},
	"offchain_token_price":     {"1 day", "token_id", "snapshot_time DESC, processing_version DESC", "2 days"},
}

// GenerateBucket1 renders the full bucket-1 migration from the register and the raw
// schemas of the 13 bucket-1 tables: the static header, one register-driven block
// per table, and the static tail. The regen-diff test compares it (normalised:
// comments and whitespace stripped) against the committed migration.
func GenerateBucket1(reg *schemamaster.Register, raw map[string]RawSchema) (string, error) {
	var b strings.Builder
	b.WriteString(header)
	b.WriteString("\n")
	for _, name := range bucket1Tables {
		schema, ok := raw[name]
		if !ok {
			return "", fmt.Errorf("missing raw schema for %q", name)
		}
		blk, err := tableBlock(reg, schema)
		if err != nil {
			return "", fmt.Errorf("generating %q: %w", name, err)
		}
		b.WriteString(blk)
	}
	b.WriteString(tail)
	return b.String(), nil
}

// tableBlock emits one table's full DDL: the empty transformed table, the guarded
// PK, the hypertable + compression/tiering policies, the change queue + enqueue
// trigger, and the _run / _bootstrap functions. Comments are intentionally not
// emitted (the regen-diff test strips them from both sides).
func tableBlock(reg *schemamaster.Register, schema RawSchema) (string, error) {
	p, err := plan(reg, schema)
	if err != nil {
		return "", err
	}
	cfg, ok := tableConfigs[p.table]
	if !ok {
		return "", fmt.Errorf("no table config for %q", p.table)
	}

	tbl := "transformed." + quote(p.table)
	pend := "transformed." + quote("_pending_"+p.table)
	rawTbl := "public." + quote(p.table)
	from := rawTbl + aliasSuffix(p.sourceAlias) + p.joinClause
	sel := "SELECT " + strings.Join(p.selectExprs, ",\n       ")
	rawPKList := quoteList(p.rawPK)
	rawPKPfx := quoteListPrefixed(p.rawPK, p.sourceAlias)

	var b strings.Builder

	// 1. empty transformed table + guarded PK + hypertable + compression/tiering.
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s AS\n%s\nFROM %s WHERE false;\n", tbl, sel, from)
	fmt.Fprintf(&b, "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='%s'::regclass AND contype='p') THEN ALTER TABLE %s ALTER COLUMN %s SET NOT NULL, ADD PRIMARY KEY (%s); END IF; END $$;\n",
		tbl, tbl, quote(p.partition), quoteList(p.pkColumns))
	fmt.Fprintf(&b, "SELECT create_hypertable('%s','%s',chunk_time_interval=>INTERVAL '%s',if_not_exists=>TRUE);\n", tbl, p.partition, cfg.chunk)
	fmt.Fprintf(&b, "ALTER TABLE %s SET (timescaledb.compress, timescaledb.compress_segmentby = '%s', timescaledb.compress_orderby = '%s');\n", tbl, cfg.seg, cfg.ord)
	fmt.Fprintf(&b, "SELECT add_compression_policy('%s', INTERVAL '%s', if_not_exists => TRUE);\n", tbl, cfg.ci)
	fmt.Fprintf(&b, "DO $$ BEGIN PERFORM add_tiering_policy('%s', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.%s'; END $$;\n", tbl, p.table)

	// 2. change queue (raw PK columns, types copied from raw) + enqueue trigger.
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s AS SELECT %s FROM %s WHERE false;\n", pend, rawPKList, rawTbl)
	fmt.Fprintf(&b, "ALTER TABLE %s ADD COLUMN IF NOT EXISTS enqueued_at timestamptz NOT NULL DEFAULT now();\n", pend)
	fmt.Fprintf(&b, "CREATE INDEX IF NOT EXISTS %s ON %s (enqueued_at);\n", quote("_pending_"+p.table+"_enqueued_at"), pend)
	fmt.Fprintf(&b, "CREATE INDEX IF NOT EXISTS %s ON %s (%s);\n", quote("_pending_"+p.table+"_"+p.rawPartition), pend, quote(p.rawPartition))
	news := make([]string, len(p.rawPK))
	for i, c := range p.rawPK {
		news[i] = "NEW." + quote(c)
	}
	fmt.Fprintf(&b, "CREATE OR REPLACE FUNCTION transformed.%s() RETURNS trigger\n"+
		"  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, transformed, public AS $tg$\n"+
		"BEGIN\n"+
		"  INSERT INTO %s(%s) VALUES (%s);\n"+
		"  RETURN NEW;\n"+
		"END $tg$;\n", quote("_enqueue_"+p.table), pend, rawPKList, strings.Join(news, ", "))
	fmt.Fprintf(&b, "DROP TRIGGER IF EXISTS \"_transform_enqueue\" ON %s;\n", rawTbl)
	fmt.Fprintf(&b, "CREATE TRIGGER \"_transform_enqueue\" AFTER INSERT ON %s FOR EACH ROW EXECUTE FUNCTION transformed.%s();\n", rawTbl, quote("_enqueue_"+p.table))

	// 3. consume (queue drain) function.
	inLeft := "(" + rawPKPfx + ")"
	partPfx := qualify(p.sourceAlias, p.rawPartition)
	fmt.Fprintf(&b, "CREATE OR REPLACE FUNCTION transformed._run_%s() RETURNS TABLE(consumed bigint, upserted bigint) AS $fn$\n"+
		"BEGIN\n"+
		"  PERFORM pg_advisory_xact_lock(hashtextextended('transformed._run_%s', 0));\n"+
		"  RETURN QUERY\n"+
		"  WITH batch AS (\n"+
		"    DELETE FROM %s\n"+
		"    WHERE ctid IN (SELECT ctid FROM %s ORDER BY %s LIMIT 10000)\n"+
		"    RETURNING %s\n"+
		"  ), ins AS (\n"+
		"  INSERT INTO %s AS t (%s)\n%s\n  FROM %s\n"+
		"  WHERE %s IN (SELECT DISTINCT %s FROM batch)\n"+
		"    AND %s >= (SELECT min(%s) FROM batch)\n"+
		"    AND %s <= (SELECT max(%s) FROM batch)\n"+
		"  ON CONFLICT (%s) DO UPDATE SET %s\n"+
		"    WHERE %s IS DISTINCT FROM %s\n"+
		"  RETURNING 1)\n"+
		"  SELECT (SELECT count(*) FROM batch), (SELECT count(*) FROM ins);\n"+
		"END $fn$ LANGUAGE plpgsql;\n",
		p.table, p.table, pend, pend, quote(p.rawPartition), rawPKList,
		tbl, quoteList(p.columns), sel, from,
		inLeft, rawPKList,
		partPfx, quote(p.rawPartition),
		partPfx, quote(p.rawPartition),
		quoteList(p.pkColumns), conflictSet(p.setColumns),
		guardTuple("t", p.setColumns), guardTuple("EXCLUDED", p.setColumns))

	// 4. bootstrap function (out-of-band history copy over a time window).
	fmt.Fprintf(&b, "CREATE OR REPLACE FUNCTION transformed._bootstrap_%s(_from timestamptz, _to timestamptz) RETURNS bigint AS $fn$\n"+
		"DECLARE n bigint;\n"+
		"BEGIN\n"+
		"  WITH ins AS (\n"+
		"  INSERT INTO %s AS t (%s)\n%s\n  FROM %s\n"+
		"  WHERE %s >= _from AND %s < _to\n"+
		"  ON CONFLICT (%s) DO UPDATE SET %s\n"+
		"    WHERE %s IS DISTINCT FROM %s\n"+
		"  RETURNING 1)\n"+
		"  SELECT count(*) INTO n FROM ins;\n"+
		"  RETURN n;\n"+
		"END $fn$ LANGUAGE plpgsql;\n",
		p.table, tbl, quoteList(p.columns), sel, from,
		partPfx, partPfx,
		quoteList(p.pkColumns), conflictSet(p.setColumns),
		guardTuple("t", p.setColumns), guardTuple("EXCLUDED", p.setColumns))

	// 5. register the source.
	fmt.Fprintf(&b, "INSERT INTO transformed._sources(source) VALUES ('%s') ON CONFLICT DO NOTHING;\n", p.table)

	return b.String(), nil
}

// conflictSet renders "col"=EXCLUDED."col" for each SET column.
func conflictSet(cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf(`%s=EXCLUDED.%s`, quote(c), quote(c))
	}
	return strings.Join(parts, ", ")
}

// guardTuple renders a row constructor over the columns, prefixed by alias
// (t / EXCLUDED), for the IS DISTINCT FROM guard.
func guardTuple(alias string, cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = alias + "." + quote(c)
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// quoteList renders a comma-separated list of quoted identifiers.
func quoteList(idents []string) string {
	parts := make([]string, len(idents))
	for i, id := range idents {
		parts[i] = quote(id)
	}
	return strings.Join(parts, ", ")
}

// quoteListPrefixed renders a comma-separated list of quoted identifiers each
// qualified by the source alias (s."col") when one exists.
func quoteListPrefixed(idents []string, alias string) string {
	parts := make([]string, len(idents))
	for i, id := range idents {
		parts[i] = qualify(alias, id)
	}
	return strings.Join(parts, ", ")
}

// aliasSuffix renders the " s" after the FROM table when a join exists.
func aliasSuffix(alias string) string {
	if alias == "" {
		return ""
	}
	return " " + alias
}
