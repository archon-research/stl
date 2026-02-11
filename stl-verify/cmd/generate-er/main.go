package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	output := flag.String("output", "../docs/entity_relation.md", "Output file path for the Mermaid ER diagram")
	flag.Parse()

	connStr := requireEnv("DATABASE_URL")
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatalf("connecting to database: %v", err)
	}
	defer pool.Close()

	if err := run(ctx, pool, *output); err != nil {
		log.Fatalf("generating ER diagram: %v", err)
	}

	log.Printf("ER diagram written to %s", *output)
}

func requireEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("required environment variable not set: %s", key)
	}
	return value
}

func run(ctx context.Context, pool *pgxpool.Pool, outputPath string) error {
	schema, err := fetchSchema(ctx, pool)
	if err != nil {
		return fmt.Errorf("fetching schema: %w", err)
	}
	content := generateMermaid(schema)
	return writeFile(content, outputPath)
}

// Table represents a database table with its columns and optional hypertable metadata.
type Table struct {
	Name       string
	Columns    []Column
	Hypertable *HypertableInfo
}

// Column represents a single column in a table.
type Column struct {
	Name         string
	DataType     string
	IsPrimaryKey bool
	ForeignTable string
	UniqueGroups []string
}

// HypertableInfo holds TimescaleDB-specific metadata for a hypertable.
type HypertableInfo struct {
	PartitionColumn string
	ChunkInterval   string
	HashDimension   string
	CompressAfter   string
	RetainFor       string
	TierAfter       string
}

// Relationship represents a foreign key relationship between two entities.
type Relationship struct {
	Parent     string // PascalCase entity name
	Child      string // PascalCase entity name
	parentSnake string // original snake_case for priority lookup
	childSnake  string // original snake_case for priority lookup
}

// excludedTables are infrastructure tables that should not appear in the ER diagram.
var excludedTables = map[string]bool{
	"migrations":     true,
	"stl_readonly":   true,
	"stl_readwrite":  true,
	"stl_read_only":  true,
	"stl_read_write": true,
}

// tablePriority defines the display order for tables in the diagram.
// Lower values appear first. Tables not in this map sort alphabetically after all prioritized tables.
var tablePriority = map[string]int{
	"chain":                  1,
	"token":                  2,
	"protocol":               3,
	"user":                   4,
	"receipt_token":          10,
	"debt_token":             11,
	"sparklend_reserve_data": 12,
	"borrower":               20,
	"borrower_collateral":    21,
	"protocol_event":         30,
	"oracle":                 40,
	"protocol_oracle":        41,
	"oracle_asset":           42,
	"offchain_price_source":  50,
	"offchain_price_asset":   51,
	"onchain_token_price":    60,
	"offchain_token_price":   61,
	"block_states":           90,
	"reorg_events":           91,
	"backfill_watermark":     92,
}

func fetchSchema(ctx context.Context, pool *pgxpool.Pool) ([]Table, error) {
	tableNames, err := fetchTableNames(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetching table names: %w", err)
	}

	columns, err := fetchColumns(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetching columns: %w", err)
	}

	pks, err := fetchPrimaryKeys(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetching primary keys: %w", err)
	}

	fks, err := fetchForeignKeys(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetching foreign keys: %w", err)
	}

	uqs, err := fetchUniqueConstraints(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetching unique constraints: %w", err)
	}

	hypertables, err := fetchHypertableInfo(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetching hypertable info: %w", err)
	}

	return buildTables(tableNames, columns, pks, fks, uqs, hypertables), nil
}

func fetchTableNames(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
		  AND table_type = 'BASE TABLE'
		ORDER BY table_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if !excludedTables[name] {
			names = append(names, name)
		}
	}
	return names, rows.Err()
}

type columnInfo struct {
	Table    string
	Name     string
	DataType string
	Ordinal  int
}

func fetchColumns(ctx context.Context, pool *pgxpool.Pool) ([]columnInfo, error) {
	rows, err := pool.Query(ctx, `
		SELECT table_name, column_name, data_type, ordinal_position
		FROM information_schema.columns
		WHERE table_schema = 'public'
		ORDER BY table_name, ordinal_position`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []columnInfo
	for rows.Next() {
		var c columnInfo
		if err := rows.Scan(&c.Table, &c.Name, &c.DataType, &c.Ordinal); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

type pkInfo struct {
	Table  string
	Column string
}

func fetchPrimaryKeys(ctx context.Context, pool *pgxpool.Pool) ([]pkInfo, error) {
	rows, err := pool.Query(ctx, `
		SELECT tc.table_name, kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		WHERE tc.table_schema = 'public'
		  AND tc.constraint_type = 'PRIMARY KEY'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pks []pkInfo
	for rows.Next() {
		var pk pkInfo
		if err := rows.Scan(&pk.Table, &pk.Column); err != nil {
			return nil, err
		}
		pks = append(pks, pk)
	}
	return pks, rows.Err()
}

type fkInfo struct {
	Table           string
	Column          string
	ReferencedTable string
}

func fetchForeignKeys(ctx context.Context, pool *pgxpool.Pool) ([]fkInfo, error) {
	rows, err := pool.Query(ctx, `
		SELECT tc.table_name, kcu.column_name, ccu.table_name AS referenced_table
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage ccu
		  ON tc.constraint_name = ccu.constraint_name
		  AND tc.table_schema = ccu.table_schema
		WHERE tc.table_schema = 'public'
		  AND tc.constraint_type = 'FOREIGN KEY'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fks []fkInfo
	for rows.Next() {
		var fk fkInfo
		if err := rows.Scan(&fk.Table, &fk.Column, &fk.ReferencedTable); err != nil {
			return nil, err
		}
		fks = append(fks, fk)
	}
	return fks, rows.Err()
}

type uqInfo struct {
	Table          string
	Column         string
	ConstraintName string
}

func fetchUniqueConstraints(ctx context.Context, pool *pgxpool.Pool) ([]uqInfo, error) {
	rows, err := pool.Query(ctx, `
		SELECT tc.table_name, kcu.column_name, tc.constraint_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		WHERE tc.table_schema = 'public'
		  AND tc.constraint_type = 'UNIQUE'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var uqs []uqInfo
	for rows.Next() {
		var uq uqInfo
		if err := rows.Scan(&uq.Table, &uq.Column, &uq.ConstraintName); err != nil {
			return nil, err
		}
		uqs = append(uqs, uq)
	}
	return uqs, rows.Err()
}

func fetchHypertableInfo(ctx context.Context, pool *pgxpool.Pool) (map[string]*HypertableInfo, error) {
	result := make(map[string]*HypertableInfo)

	// Check if TimescaleDB is available
	var hasTimescale bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
		)`).Scan(&hasTimescale)
	if err != nil || !hasTimescale {
		return result, nil
	}

	// Fetch hypertables and their dimensions
	rows, err := pool.Query(ctx, `
		SELECT h.hypertable_name,
		       d.column_name,
		       d.column_type,
		       d.dimension_type,
		       d.num_partitions,
		       CASE
		           WHEN d.time_interval IS NOT NULL THEN d.time_interval::text
		           WHEN d.integer_interval IS NOT NULL THEN d.integer_interval::text
		           ELSE ''
		       END AS interval_value
		FROM timescaledb_information.hypertables h
		JOIN timescaledb_information.dimensions d
		  ON h.hypertable_schema = d.hypertable_schema
		  AND h.hypertable_name = d.hypertable_name
		WHERE h.hypertable_schema = 'public'
		ORDER BY h.hypertable_name, d.dimension_number`)
	if err != nil {
		return nil, fmt.Errorf("querying hypertables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			tableName     string
			colName       string
			colType       string
			dimType       string
			numPartitions *int
			intervalValue string
		)
		if err := rows.Scan(&tableName, &colName, &colType, &dimType, &numPartitions, &intervalValue); err != nil {
			return nil, err
		}
		info, ok := result[tableName]
		if !ok {
			info = &HypertableInfo{}
			result[tableName] = info
		}

		switch dimType {
		case "Time":
			info.PartitionColumn = colName
			info.ChunkInterval = formatInterval(intervalValue)
		case "Space":
			if numPartitions != nil {
				info.HashDimension = fmt.Sprintf("%s,%d", colName, *numPartitions)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Fetch job policies (compression, retention)
	jobRows, err := pool.Query(ctx, `
		SELECT j.proc_name,
		       j.hypertable_name,
		       j.config
		FROM timescaledb_information.jobs j
		WHERE j.hypertable_schema = 'public'
		  AND j.proc_name IN ('policy_compression', 'policy_retention', 'policy_reorder')
		ORDER BY j.hypertable_name, j.proc_name`)
	if err != nil {
		return nil, fmt.Errorf("querying jobs: %w", err)
	}
	defer jobRows.Close()

	for jobRows.Next() {
		var (
			procName  string
			tableName string
			config    map[string]any
		)
		if err := jobRows.Scan(&procName, &tableName, &config); err != nil {
			return nil, err
		}
		info := result[tableName]
		if info == nil {
			continue
		}
		switch procName {
		case "policy_compression":
			if v, ok := config["compress_after"]; ok {
				info.CompressAfter = formatInterval(fmt.Sprintf("%v", v))
			}
		case "policy_retention":
			if v, ok := config["drop_after"]; ok {
				info.RetainFor = formatInterval(fmt.Sprintf("%v", v))
			}
		}
	}
	if err := jobRows.Err(); err != nil {
		return nil, err
	}

	// Try to fetch tiering policies (only on Timescale Cloud)
	tierRows, err := pool.Query(ctx, `
		SELECT j.proc_name,
		       j.hypertable_name,
		       j.config
		FROM timescaledb_information.jobs j
		WHERE j.hypertable_schema = 'public'
		  AND j.proc_name = 'policy_tiering'
		ORDER BY j.hypertable_name`)
	if err == nil {
		defer tierRows.Close()
		for tierRows.Next() {
			var (
				procName  string
				tableName string
				config    map[string]any
			)
			if err := tierRows.Scan(&procName, &tableName, &config); err != nil {
				break
			}
			info := result[tableName]
			if info == nil {
				continue
			}
			if v, ok := config["move_after"]; ok {
				info.TierAfter = formatInterval(fmt.Sprintf("%v", v))
			}
		}
	}

	return result, nil
}

// formatInterval converts PostgreSQL interval strings to compact display form.
// Examples: "1 day" -> "1d", "2 days" -> "2d", "30 days" -> "30d", "365 days" -> "1y", "100000" -> "100000"
func formatInterval(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	// Integer interval (for block_number partition columns)
	if _, err := fmt.Sscanf(s, "%d", new(int)); err == nil && !strings.Contains(s, " ") {
		return s
	}

	// Time intervals
	replacer := strings.NewReplacer(
		" days", "d",
		" day", "d",
		" years", "y",
		" year", "y",
		" mons", "mo",
		" mon", "mo",
		" hours", "h",
		" hour", "h",
	)

	result := replacer.Replace(s)

	// Handle composite intervals like "365d" -> "1y"
	if result == "365d" {
		return "1y"
	}

	return result
}

func buildTables(
	tableNames []string,
	columns []columnInfo,
	pks []pkInfo,
	fks []fkInfo,
	uqs []uqInfo,
	hypertables map[string]*HypertableInfo,
) []Table {
	// Build lookup maps
	pkSet := make(map[string]bool) // "table.column" -> true
	for _, pk := range pks {
		pkSet[pk.Table+"."+pk.Column] = true
	}

	fkMap := make(map[string]string) // "table.column" -> referenced table
	for _, fk := range fks {
		fkMap[fk.Table+"."+fk.Column] = fk.ReferencedTable
	}

	// Group unique constraints: table -> constraint_name -> []column
	uqGroups := make(map[string]map[string][]string)
	for _, uq := range uqs {
		if uqGroups[uq.Table] == nil {
			uqGroups[uq.Table] = make(map[string][]string)
		}
		uqGroups[uq.Table][uq.ConstraintName] = append(uqGroups[uq.Table][uq.ConstraintName], uq.Column)
	}

	// Assign UK labels per table
	uqLabels := make(map[string]map[string]string)
	for table, constraints := range uqGroups {
		uqLabels[table] = assignUniqueLabels(constraints)
	}

	// Build column-level unique group membership: "table.column" -> []label
	colUniqueGroups := make(map[string][]string)
	for table, constraints := range uqGroups {
		for constraintName, cols := range constraints {
			label := uqLabels[table][constraintName]
			for _, col := range cols {
				key := table + "." + col
				colUniqueGroups[key] = append(colUniqueGroups[key], label)
			}
		}
	}

	// Group columns by table
	tableCols := make(map[string][]columnInfo)
	for _, c := range columns {
		tableCols[c.Table] = append(tableCols[c.Table], c)
	}

	var tables []Table
	for _, name := range tableNames {
		t := Table{
			Name:       name,
			Hypertable: hypertables[name],
		}

		for _, c := range tableCols[name] {
			key := name + "." + c.Name
			col := Column{
				Name:         c.Name,
				DataType:     mapDataType(c.DataType),
				IsPrimaryKey: pkSet[key],
				ForeignTable: fkMap[key],
				UniqueGroups: colUniqueGroups[key],
			}
			t.Columns = append(t.Columns, col)
		}

		tables = append(tables, t)
	}

	sortTables(tables)
	return tables
}

// assignUniqueLabels assigns labels to unique constraints for a table.
// Single-column constraints get "UK". Multi-column or multiple constraints get "UK1", "UK2", etc.
func assignUniqueLabels(constraints map[string][]string) map[string]string {
	labels := make(map[string]string)

	var names []string
	for name := range constraints {
		names = append(names, name)
	}
	sort.Strings(names)

	if len(names) == 1 && len(constraints[names[0]]) == 1 {
		labels[names[0]] = "UK"
		return labels
	}

	for i, name := range names {
		labels[name] = fmt.Sprintf("UK%d", i+1)
	}
	return labels
}

// mapDataType converts PostgreSQL information_schema data types to compact display types.
func mapDataType(pgType string) string {
	switch pgType {
	case "bigint":
		return "bigint"
	case "integer":
		return "int"
	case "smallint":
		return "smallint"
	case "character varying":
		return "varchar"
	case "text":
		return "text"
	case "bytea":
		return "bytea"
	case "numeric":
		return "numeric"
	case "boolean":
		return "boolean"
	case "timestamp with time zone":
		return "timestamptz"
	case "timestamp without time zone":
		return "timestamp"
	case "jsonb":
		return "jsonb"
	case "json":
		return "json"
	case "double precision":
		return "float8"
	case "real":
		return "float4"
	case "uuid":
		return "uuid"
	case "date":
		return "date"
	default:
		return pgType
	}
}

func sortTables(tables []Table) {
	sort.Slice(tables, func(i, j int) bool {
		pi, hasi := tablePriority[tables[i].Name]
		pj, hasj := tablePriority[tables[j].Name]

		switch {
		case hasi && hasj:
			return pi < pj
		case hasi:
			return true
		case hasj:
			return false
		default:
			return tables[i].Name < tables[j].Name
		}
	})
}

// snakeToPascal converts snake_case to PascalCase.
func snakeToPascal(s string) string {
	parts := strings.Split(s, "_")
	var b strings.Builder
	for _, p := range parts {
		if p == "" {
			continue
		}
		b.WriteString(strings.ToUpper(p[:1]))
		b.WriteString(p[1:])
	}
	return b.String()
}

func generateMermaid(tables []Table) string {
	var b strings.Builder

	b.WriteString("```mermaid\nerDiagram\n")

	// Render entities
	for i, t := range tables {
		entityName := snakeToPascal(t.Name)
		b.WriteString(fmt.Sprintf("    %s {\n", entityName))

		for _, col := range t.Columns {
			b.WriteString(renderColumn(col, t.Hypertable))
		}

		b.WriteString("    }\n")
		if i < len(tables)-1 {
			b.WriteString("\n")
		}
	}

	// Collect and render relationships
	rels := collectRelationships(tables)
	if len(rels) > 0 {
		b.WriteString("\n")
		for _, rel := range rels {
			b.WriteString(fmt.Sprintf("    %s ||--o{ %s : \"\"\n", rel.Parent, rel.Child))
		}
	}

	b.WriteString("```\n")
	return b.String()
}

func renderColumn(col Column, ht *HypertableInfo) string {
	var parts []string
	parts = append(parts, "        "+col.DataType)
	parts = append(parts, col.Name)

	// Mermaid ER syntax only supports a single key type (PK, FK, or UK).
	// When a column is both PK and FK, prefer PK since FK is already shown via relationship lines.
	if col.IsPrimaryKey {
		parts = append(parts, "PK")
	} else if col.ForeignTable != "" {
		parts = append(parts, "FK")
	}

	// Build comment from unique groups and hypertable info
	var comments []string

	if len(col.UniqueGroups) > 0 {
		sort.Strings(col.UniqueGroups)
		comments = append(comments, col.UniqueGroups...)
	}

	if ht != nil && col.Name == ht.PartitionColumn {
		comments = append(comments, hypertableComment(ht))
	}

	if len(comments) > 0 {
		parts = append(parts, fmt.Sprintf("%q", strings.Join(comments, ", ")))
	}

	return strings.Join(parts, " ") + "\n"
}

func hypertableComment(ht *HypertableInfo) string {
	var parts []string
	parts = append(parts, fmt.Sprintf("hypertable: %s chunks", ht.ChunkInterval))

	if ht.HashDimension != "" {
		parts = append(parts, fmt.Sprintf("hash(%s)", ht.HashDimension))
	}
	if ht.CompressAfter != "" {
		parts = append(parts, fmt.Sprintf("compress %s", ht.CompressAfter))
	}
	if ht.RetainFor != "" {
		parts = append(parts, fmt.Sprintf("retain %s", ht.RetainFor))
	}
	if ht.TierAfter != "" {
		parts = append(parts, fmt.Sprintf("tier %s", ht.TierAfter))
	}

	return strings.Join(parts, ", ")
}

func collectRelationships(tables []Table) []Relationship {
	seen := make(map[string]bool)
	var rels []Relationship

	for _, t := range tables {
		childName := snakeToPascal(t.Name)
		for _, col := range t.Columns {
			if col.ForeignTable == "" {
				continue
			}
			parentName := snakeToPascal(col.ForeignTable)
			key := parentName + "->" + childName
			if seen[key] {
				continue
			}
			seen[key] = true
			rels = append(rels, Relationship{
				Parent:      parentName,
				Child:       childName,
				parentSnake: col.ForeignTable,
				childSnake:  t.Name,
			})
		}
	}

	slices.SortFunc(rels, func(a, b Relationship) int {
		pa, ha := tablePriority[a.parentSnake]
		pb, hb := tablePriority[b.parentSnake]
		if a.parentSnake != b.parentSnake {
			return compareByPriority(pa, ha, pb, hb, a.Parent, b.Parent)
		}
		ca, hca := tablePriority[a.childSnake]
		cb, hcb := tablePriority[b.childSnake]
		return compareByPriority(ca, hca, cb, hcb, a.Child, b.Child)
	})

	return rels
}

func compareByPriority(pa int, ha bool, pb int, hb bool, nameA, nameB string) int {
	switch {
	case ha && hb:
		return pa - pb
	case ha:
		return -1
	case hb:
		return 1
	default:
		return strings.Compare(nameA, nameB)
	}
}

func writeFile(content, path string) error {
	return os.WriteFile(path, []byte(content), 0644)
}
