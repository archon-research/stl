package s3key

import (
	"testing"
)

func TestBuild(t *testing.T) {
	tests := []struct {
		name        string
		blockNumber int64
		version     int
		dataType    DataType
		want        string
	}{
		{
			name:        "block in first partition",
			blockNumber: 42,
			version:     1,
			dataType:    Block,
			want:        "0-999/42_1_block.json.gz",
		},
		{
			name:        "receipts in second partition",
			blockNumber: 1500,
			version:     2,
			dataType:    Receipts,
			want:        "1000-1999/1500_2_receipts.json.gz",
		},
		{
			name:        "traces at partition boundary",
			blockNumber: 1000,
			version:     1,
			dataType:    Traces,
			want:        "1000-1999/1000_1_traces.json.gz",
		},
		{
			name:        "blobs in high block range",
			blockNumber: 21000042,
			version:     3,
			dataType:    Blobs,
			want:        "21000000-21000999/21000042_3_blobs.json.gz",
		},
		{
			name:        "block zero",
			blockNumber: 0,
			version:     1,
			dataType:    Block,
			want:        "0-999/0_1_block.json.gz",
		},
		{
			name:        "last block in partition",
			blockNumber: 999,
			version:     1,
			dataType:    Receipts,
			want:        "0-999/999_1_receipts.json.gz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Build(tt.blockNumber, tt.version, tt.dataType)
			if got != tt.want {
				t.Errorf("Build(%d, %d, %q) = %q, want %q",
					tt.blockNumber, tt.version, tt.dataType, got, tt.want)
			}
		})
	}
}

func TestBuildWithPartition(t *testing.T) {
	tests := []struct {
		name         string
		partitionStr string
		blockNumber  int64
		version      int
		dataType     DataType
		want         string
	}{
		{
			name:         "pre-computed partition",
			partitionStr: "0-999",
			blockNumber:  42,
			version:      1,
			dataType:     Block,
			want:         "0-999/42_1_block.json.gz",
		},
		{
			name:         "custom partition string",
			partitionStr: "21000000-21000999",
			blockNumber:  21000500,
			version:      2,
			dataType:     Traces,
			want:         "21000000-21000999/21000500_2_traces.json.gz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildWithPartition(tt.partitionStr, tt.blockNumber, tt.version, tt.dataType)
			if got != tt.want {
				t.Errorf("BuildWithPartition(%q, %d, %d, %q) = %q, want %q",
					tt.partitionStr, tt.blockNumber, tt.version, tt.dataType, got, tt.want)
			}
		})
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		name   string
		key    string
		want   Key
		wantOK bool
	}{
		{
			name: "valid block key",
			key:  "0-999/42_1_block.json.gz",
			want: Key{
				Partition:   "0-999",
				BlockNumber: 42,
				Version:     1,
				DataType:    Block,
			},
			wantOK: true,
		},
		{
			name: "valid receipts key",
			key:  "1000-1999/1500_2_receipts.json.gz",
			want: Key{
				Partition:   "1000-1999",
				BlockNumber: 1500,
				Version:     2,
				DataType:    Receipts,
			},
			wantOK: true,
		},
		{
			name: "valid traces key",
			key:  "21000000-21000999/21000042_3_traces.json.gz",
			want: Key{
				Partition:   "21000000-21000999",
				BlockNumber: 21000042,
				Version:     3,
				DataType:    Traces,
			},
			wantOK: true,
		},
		{
			name: "valid blobs key",
			key:  "5000-5999/5555_1_blobs.json.gz",
			want: Key{
				Partition:   "5000-5999",
				BlockNumber: 5555,
				Version:     1,
				DataType:    Blobs,
			},
			wantOK: true,
		},
		{
			name:   "empty string",
			key:    "",
			wantOK: false,
		},
		{
			name:   "no slash",
			key:    "42_1_block.json.gz",
			wantOK: false,
		},
		{
			name:   "missing suffix",
			key:    "0-999/42_1_block",
			wantOK: false,
		},
		{
			name:   "wrong suffix",
			key:    "0-999/42_1_block.json",
			wantOK: false,
		},
		{
			name:   "non-numeric block number",
			key:    "0-999/abc_1_block.json.gz",
			wantOK: false,
		},
		{
			name:   "non-numeric version",
			key:    "0-999/42_xyz_block.json.gz",
			wantOK: false,
		},
		{
			name:   "unknown data type",
			key:    "0-999/42_1_unknown.json.gz",
			wantOK: false,
		},
		{
			name:   "missing version part (only two parts)",
			key:    "0-999/42_block.json.gz",
			wantOK: false,
		},
		{
			name:   "only block number (no underscores)",
			key:    "0-999/42.json.gz",
			wantOK: false,
		},
		{
			name:   "extra underscores in stem",
			key:    "0-999/42_1_block_extra.json.gz",
			wantOK: false,
		},
		{
			name:   "negative block number",
			key:    "0-999/-1_1_block.json.gz",
			wantOK: false,
		},
		{
			name:   "slash only",
			key:    "/",
			wantOK: false,
		},
		{
			name:   "empty filename after slash",
			key:    "0-999/",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := Parse(tt.key)
			if ok != tt.wantOK {
				t.Fatalf("Parse(%q) ok = %v, want %v", tt.key, ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if got != tt.want {
				t.Errorf("Parse(%q) = %+v, want %+v", tt.key, got, tt.want)
			}
		})
	}
}

func TestBuildThenParse(t *testing.T) {
	// Round-trip: Build a key and parse it back, verify we get the same values.
	tests := []struct {
		name        string
		blockNumber int64
		version     int
		dataType    DataType
	}{
		{name: "block partition zero", blockNumber: 0, version: 1, dataType: Block},
		{name: "receipts upper edge partition", blockNumber: 999, version: 1, dataType: Receipts},
		{name: "traces next partition", blockNumber: 1000, version: 2, dataType: Traces},
		{name: "blobs large block", blockNumber: 21000042, version: 3, dataType: Blobs},
		{name: "high block high version", blockNumber: 99999999, version: 100, dataType: Block},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := Build(tt.blockNumber, tt.version, tt.dataType)
			got, ok := Parse(key)
			if !ok {
				t.Errorf("Parse(Build(%d, %d, %q)) failed", tt.blockNumber, tt.version, tt.dataType)
				return
			}
			if got.BlockNumber != tt.blockNumber {
				t.Errorf("round-trip BlockNumber = %d, want %d", got.BlockNumber, tt.blockNumber)
			}
			if got.Version != tt.version {
				t.Errorf("round-trip Version = %d, want %d", got.Version, tt.version)
			}
			if got.DataType != tt.dataType {
				t.Errorf("round-trip DataType = %q, want %q", got.DataType, tt.dataType)
			}
		})
	}
}
