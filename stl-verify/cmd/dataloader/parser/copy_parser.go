// Package parser provides parsers for PostgreSQL dump files.
package parser

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// CopyBlock represents a COPY statement block from a PostgreSQL dump.
type CopyBlock struct {
	Schema  string     // Schema name (e.g., "public")
	Table   string     // Table name (e.g., "UserReserveSnapshot")
	Columns []string   // Column names in order
	Rows    [][]string // Each row is a slice of field values
}

// ParseCopyBlocks parses all COPY blocks from a PostgreSQL dump file.
// The dump format expected is:
//
//	COPY schema."TableName" (col1, col2, ...) FROM stdin;
//	value1\tvalue2\t...
//	\.
func ParseCopyBlocks(r io.Reader) ([]*CopyBlock, error) {
	scanner := bufio.NewScanner(r)
	// Increase buffer size for potentially long lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024) // 10MB max line size

	var blocks []*CopyBlock
	var currentBlock *CopyBlock
	inCopyData := false

	for scanner.Scan() {
		line := scanner.Text()

		if inCopyData {
			// Check for end of COPY data marker
			if line == "\\." {
				if currentBlock != nil {
					blocks = append(blocks, currentBlock)
					currentBlock = nil
				}
				inCopyData = false
				continue
			}

			// Parse tab-separated values
			fields := parseCopyRow(line)
			if currentBlock != nil {
				currentBlock.Rows = append(currentBlock.Rows, fields)
			}
			continue
		}

		// Check for COPY statement
		if strings.HasPrefix(line, "COPY ") {
			block, err := parseCopyStatement(line)
			if err != nil {
				return nil, fmt.Errorf("failed to parse COPY statement: %w", err)
			}
			currentBlock = block
			inCopyData = true
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error: %w", err)
	}

	return blocks, nil
}

// parseCopyStatement parses a COPY statement line.
// Expected format: COPY schema."TableName" (col1, col2, ...) FROM stdin;
func parseCopyStatement(line string) (*CopyBlock, error) {
	// Remove "COPY " prefix and " FROM stdin;" suffix
	line = strings.TrimPrefix(line, "COPY ")
	line = strings.TrimSuffix(line, " FROM stdin;")

	// Find the column list (everything inside parentheses)
	parenStart := strings.Index(line, "(")
	parenEnd := strings.LastIndex(line, ")")
	if parenStart == -1 || parenEnd == -1 || parenEnd <= parenStart {
		return nil, fmt.Errorf("invalid COPY statement format: no column list found")
	}

	// Parse table reference (schema.table or schema."table")
	tableRef := strings.TrimSpace(line[:parenStart])
	schema, table := parseTableRef(tableRef)

	// Parse columns
	columnStr := line[parenStart+1 : parenEnd]
	columns := parseColumnList(columnStr)

	return &CopyBlock{
		Schema:  schema,
		Table:   table,
		Columns: columns,
		Rows:    make([][]string, 0),
	}, nil
}

// parseTableRef parses a table reference like schema."TableName" or schema.tablename
func parseTableRef(ref string) (schema, table string) {
	parts := strings.SplitN(ref, ".", 2)
	if len(parts) == 2 {
		schema = parts[0]
		table = strings.Trim(parts[1], "\"")
	} else {
		table = strings.Trim(parts[0], "\"")
	}
	return
}

// parseColumnList parses a comma-separated column list.
func parseColumnList(s string) []string {
	parts := strings.Split(s, ",")
	columns := make([]string, 0, len(parts))
	for _, p := range parts {
		col := strings.TrimSpace(p)
		col = strings.Trim(col, "\"")
		if col != "" {
			columns = append(columns, col)
		}
	}
	return columns
}

// parseCopyRow parses a tab-separated row from COPY data.
// Handles PostgreSQL escape sequences:
//   - \N for NULL
//   - \\ for backslash
//   - \t for tab (within values)
//   - \n for newline
func parseCopyRow(line string) []string {
	if line == "" {
		return nil
	}

	fields := strings.Split(line, "\t")
	result := make([]string, len(fields))

	for i, field := range fields {
		result[i] = unescapeCopyValue(field)
	}

	return result
}

// unescapeCopyValue handles PostgreSQL COPY escape sequences.
func unescapeCopyValue(s string) string {
	if s == "\\N" {
		return "" // NULL value - return empty string; callers should check IsNull
	}

	// Fast path: no escapes
	if !strings.Contains(s, "\\") {
		return s
	}

	// Handle escape sequences
	var sb strings.Builder
	sb.Grow(len(s))

	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case '\\':
				sb.WriteByte('\\')
				i++
			case 'n':
				sb.WriteByte('\n')
				i++
			case 'r':
				sb.WriteByte('\r')
				i++
			case 't':
				sb.WriteByte('\t')
				i++
			default:
				sb.WriteByte(s[i])
			}
		} else {
			sb.WriteByte(s[i])
		}
	}

	return sb.String()
}

// IsNull checks if a COPY field value represents NULL.
func IsNull(field string) bool {
	return field == "\\N"
}

// ColumnIndex returns a map of column name to index for efficient lookup.
func (b *CopyBlock) ColumnIndex() map[string]int {
	idx := make(map[string]int, len(b.Columns))
	for i, col := range b.Columns {
		idx[col] = i
	}
	return idx
}

// GetField returns the field value at the given column name from a row.
// Returns empty string if column not found.
func (b *CopyBlock) GetField(row []string, colIndex map[string]int, colName string) string {
	idx, ok := colIndex[colName]
	if !ok || idx >= len(row) {
		return ""
	}
	return row[idx]
}
