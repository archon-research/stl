package parser

import (
	"strings"
	"testing"
)

func TestParseCopyBlocks(t *testing.T) {
	input := `-- This is a comment
CREATE TABLE test.foo (id int);

COPY test."UserReserveSnapshot" (id, user_address, block_number, amount) FROM stdin;
1	0xabc123	12345	1000000000000000000
2	0xdef456	12346	2000000000000000000
\.

COPY test."ReserveData" (reserve_id, asset) FROM stdin;
100	0xtoken1
101	0xtoken2
\.
`

	blocks, err := ParseCopyBlocks(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCopyBlocks failed: %v", err)
	}

	if len(blocks) != 2 {
		t.Fatalf("expected 2 blocks, got %d", len(blocks))
	}

	// Check first block
	b1 := blocks[0]
	if b1.Schema != "test" {
		t.Errorf("block 1 schema: expected 'test', got '%s'", b1.Schema)
	}
	if b1.Table != "UserReserveSnapshot" {
		t.Errorf("block 1 table: expected 'UserReserveSnapshot', got '%s'", b1.Table)
	}
	if len(b1.Columns) != 4 {
		t.Errorf("block 1 columns: expected 4, got %d", len(b1.Columns))
	}
	expectedCols := []string{"id", "user_address", "block_number", "amount"}
	for i, col := range expectedCols {
		if b1.Columns[i] != col {
			t.Errorf("block 1 column %d: expected '%s', got '%s'", i, col, b1.Columns[i])
		}
	}
	if len(b1.Rows) != 2 {
		t.Errorf("block 1 rows: expected 2, got %d", len(b1.Rows))
	}

	// Check row data
	if b1.Rows[0][0] != "1" {
		t.Errorf("row 0 field 0: expected '1', got '%s'", b1.Rows[0][0])
	}
	if b1.Rows[0][1] != "0xabc123" {
		t.Errorf("row 0 field 1: expected '0xabc123', got '%s'", b1.Rows[0][1])
	}

	// Check second block
	b2 := blocks[1]
	if b2.Table != "ReserveData" {
		t.Errorf("block 2 table: expected 'ReserveData', got '%s'", b2.Table)
	}
	if len(b2.Rows) != 2 {
		t.Errorf("block 2 rows: expected 2, got %d", len(b2.Rows))
	}
}

func TestParseCopyRowWithEscapes(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{
			input:    "simple\tvalue",
			expected: []string{"simple", "value"},
		},
		{
			input:    "has\\ttab",
			expected: []string{"has\ttab"},
		},
		{
			input:    "has\\nnewline",
			expected: []string{"has\nnewline"},
		},
		{
			input:    "has\\\\backslash",
			expected: []string{"has\\backslash"},
		},
		{
			input:    "\\N\tvalue",
			expected: []string{"", "value"}, // NULL becomes empty string
		},
		{
			input:    "a\t\\N\tb",
			expected: []string{"a", "", "b"},
		},
	}

	for _, tc := range tests {
		result := parseCopyRow(tc.input)
		if len(result) != len(tc.expected) {
			t.Errorf("input '%s': expected %d fields, got %d", tc.input, len(tc.expected), len(result))
			continue
		}
		for i := range tc.expected {
			if result[i] != tc.expected[i] {
				t.Errorf("input '%s' field %d: expected '%s', got '%s'", tc.input, i, tc.expected[i], result[i])
			}
		}
	}
}

func TestColumnIndex(t *testing.T) {
	block := &CopyBlock{
		Schema:  "test",
		Table:   "foo",
		Columns: []string{"id", "name", "value"},
	}

	idx := block.ColumnIndex()

	if idx["id"] != 0 {
		t.Errorf("expected id index 0, got %d", idx["id"])
	}
	if idx["name"] != 1 {
		t.Errorf("expected name index 1, got %d", idx["name"])
	}
	if idx["value"] != 2 {
		t.Errorf("expected value index 2, got %d", idx["value"])
	}
}

func TestGetField(t *testing.T) {
	block := &CopyBlock{
		Schema:  "test",
		Table:   "foo",
		Columns: []string{"id", "name", "value"},
	}
	idx := block.ColumnIndex()
	row := []string{"123", "test name", "42"}

	if v := block.GetField(row, idx, "id"); v != "123" {
		t.Errorf("expected '123', got '%s'", v)
	}
	if v := block.GetField(row, idx, "name"); v != "test name" {
		t.Errorf("expected 'test name', got '%s'", v)
	}
	if v := block.GetField(row, idx, "nonexistent"); v != "" {
		t.Errorf("expected empty string for nonexistent column, got '%s'", v)
	}
}

func TestIsNull(t *testing.T) {
	if !IsNull("\\N") {
		t.Error("expected \\N to be null")
	}
	if IsNull("") {
		t.Error("expected empty string to not be null")
	}
	if IsNull("value") {
		t.Error("expected 'value' to not be null")
	}
}

func TestParseTableRef(t *testing.T) {
	tests := []struct {
		input          string
		expectedSchema string
		expectedTable  string
	}{
		{"public.users", "public", "users"},
		{"test.\"UserReserveSnapshot\"", "test", "UserReserveSnapshot"},
		{"\"MyTable\"", "", "MyTable"},
		{"simple", "", "simple"},
	}

	for _, tc := range tests {
		schema, table := parseTableRef(tc.input)
		if schema != tc.expectedSchema {
			t.Errorf("input '%s': expected schema '%s', got '%s'", tc.input, tc.expectedSchema, schema)
		}
		if table != tc.expectedTable {
			t.Errorf("input '%s': expected table '%s', got '%s'", tc.input, tc.expectedTable, table)
		}
	}
}
