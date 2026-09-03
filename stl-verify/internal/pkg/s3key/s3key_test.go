package s3key

import (
	"errors"
	"strings"
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
		blockNumber int64
		version     int
		dataType    DataType
	}{
		{0, 1, Block},
		{999, 1, Receipts},
		{1000, 2, Traces},
		{21000042, 3, Blobs},
		{99999999, 100, Block},
	}

	for _, tt := range tests {
		key := Build(tt.blockNumber, tt.version, tt.dataType)
		got, ok := Parse(key)
		if !ok {
			t.Errorf("Parse(Build(%d, %d, %q)) failed", tt.blockNumber, tt.version, tt.dataType)
			continue
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
	}
}

func TestHighestVersion(t *testing.T) {
	const height = int64(25395651)
	part := "25395000-25395999"

	tests := []struct {
		name        string
		keys        []string
		wantVersion int
		wantFound   bool
		wantErr     bool
	}{
		{
			name: "an archive holding nothing at the height",
			keys: nil,
		},
		{
			name:      "only the live version",
			keys:      []string{part + "/25395651_0_block.json.gz", part + "/25395651_0_receipts.json.gz"},
			wantFound: true,
		},
		{
			name: "a partial correction still occupies its slot",
			keys: []string{
				part + "/25395651_0_block.json.gz",
				part + "/25395651_0_receipts.json.gz",
				part + "/25395651_1_block.json.gz",
			},
			wantVersion: 1,
			wantFound:   true,
		},
		{
			name: "the highest version wins whatever order the listing arrives in",
			keys: []string{
				part + "/25395651_4_traces.json.gz",
				part + "/25395651_0_block.json.gz",
				part + "/25395651_2_block.json.gz",
			},
			wantVersion: 4,
			wantFound:   true,
		},
		{
			name: "another height in the same partition says nothing about this one",
			keys: []string{part + "/25395650_7_block.json.gz", part + "/25395652_9_block.json.gz"},
		},
		{
			name: "a key filed under another partition is not this height's",
			keys: []string{"0-999/25395651_7_block.json.gz"},
		},
		{
			name: "an object of a type this binary does not know still occupies its version",
			keys: []string{
				part + "/25395651_0_block.json.gz",
				part + "/25395651_1_unknown.json.gz",
			},
			wantVersion: 1,
			wantFound:   true,
		},
		{
			name:        "a data type added after this binary shipped, in any shape",
			keys:        []string{part + "/25395651_2_withdrawals.msgpack"},
			wantVersion: 2,
			wantFound:   true,
		},
		{
			name:    "a key carrying no version at all",
			keys:    []string{part + "/25395651_x_block.json.gz"},
			wantErr: true,
		},
		{
			name:    "an object that is not a block payload",
			keys:    []string{part + "/README.txt"},
			wantErr: true,
		},
		{
			name:    "a malformed key hides the real ones rather than being skipped",
			keys:    []string{part + "/25395651_x_block.json.gz", part + "/25395651_3_block.json.gz"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version, found, err := HighestVersion(tt.keys, height)

			if tt.wantErr {
				if !errors.Is(err, ErrUnrecognisedKey) {
					t.Fatalf("error = %v, want ErrUnrecognisedKey", err)
				}
				if !strings.Contains(err.Error(), tt.keys[0]) {
					t.Errorf("error = %v, want it to name %q", err, tt.keys[0])
				}
				return
			}
			if err != nil {
				t.Fatalf("HighestVersion: %v", err)
			}
			if found != tt.wantFound {
				t.Fatalf("HighestVersion found = %v, want %v", found, tt.wantFound)
			}
			if found && version != tt.wantVersion {
				t.Errorf("HighestVersion = %d, want %d", version, tt.wantVersion)
			}
		})
	}
}

func TestNextVersion(t *testing.T) {
	tests := []struct {
		name    string
		highest int
		found   bool
		want    int
	}{
		{name: "an empty height starts at the first correction slot", want: 1},
		{name: "the live version is corrected at 1", highest: 0, found: true, want: 1},
		{name: "an occupied correction slot moves the next one up", highest: 1, found: true, want: 2},
		{name: "a height corrected many times keeps counting", highest: 9, found: true, want: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NextVersion(tt.highest, tt.found); got != tt.want {
				t.Errorf("NextVersion(%d, %v) = %d, want %d", tt.highest, tt.found, got, tt.want)
			}
		})
	}
}

func TestHeightPrefix(t *testing.T) {
	tests := []struct {
		name        string
		blockNumber int64
		want        string
	}{
		{name: "a height mid-partition", blockNumber: 25395651, want: "25395000-25395999/25395651_"},
		{name: "the first block of a partition", blockNumber: 1000, want: "1000-1999/1000_"},
		{name: "block zero", blockNumber: 0, want: "0-999/0_"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HeightPrefix(tt.blockNumber); got != tt.want {
				t.Errorf("HeightPrefix(%d) = %q, want %q", tt.blockNumber, got, tt.want)
			}
		})
	}
}

// Heights 1 and 10 share partition 0-999, so only the trailing underscore keeps
// one out of the other's listing.
func TestHeightPrefix_ExcludesALongerHeightSharingThePartition(t *testing.T) {
	prefix := HeightPrefix(1)

	if neighbour := Build(10, 1, Block); strings.HasPrefix(neighbour, prefix) {
		t.Errorf("%q starts with %q", neighbour, prefix)
	}
	if own := Build(1, 1, Block); !strings.HasPrefix(own, prefix) {
		t.Errorf("%q does not start with %q", own, prefix)
	}
}

func TestPartitionPrefix(t *testing.T) {
	if got, want := PartitionPrefix("25395000-25395999"), "25395000-25395999/"; got != want {
		t.Errorf("PartitionPrefix = %q, want %q", got, want)
	}
	if key := Build(25395651, 1, Block); !strings.HasPrefix(key, PartitionPrefix("25395000-25395999")) {
		t.Errorf("%q does not start with its own partition prefix", key)
	}
}

func TestOccupancies(t *testing.T) {
	part := "25395000-25395999"

	index, err := Occupancies([]string{
		part + "/25395651_0_block.json.gz",
		part + "/25395651_0_receipts.json.gz",
		part + "/25395651_1_block.json.gz",
		part + "/25395652_0_traces.json.gz",
		"0-999/25395654_9_block.json.gz",
	})
	if err != nil {
		t.Fatalf("Occupancies: %v", err)
	}

	if len(index) != 2 {
		t.Fatalf("index = %v, want only the two heights it can read", index)
	}

	top := index[25395651]
	if top.Version != 1 {
		t.Errorf("top version = %d, want the highest object present", top.Version)
	}
	// The correction only got its block object written, and the lower version's
	// receipts belong to the slot it replaced.
	if !top.DataTypes[Block] || top.DataTypes[Receipts] {
		t.Errorf("data types = %v, want the top version's alone", top.DataTypes)
	}
	if got := index[25395652]; got.Version != 0 || !got.DataTypes[Traces] {
		t.Errorf("index[25395652] = %+v, want version 0 holding traces", got)
	}
	if _, ok := index[25395654]; ok {
		t.Error("indexed a key filed under another partition than its height's")
	}
}

// An object whose type this binary does not know still fills the slot, so a
// correction has to go above it rather than on top of it.
func TestOccupancies_AnUnknownTypeOccupiesWithoutJoiningTheDataTypes(t *testing.T) {
	part := "25395000-25395999"

	index, err := Occupancies([]string{
		part + "/25395651_1_block.json.gz",
		part + "/25395651_2_withdrawals.json.gz",
	})
	if err != nil {
		t.Fatalf("Occupancies: %v", err)
	}

	top := index[25395651]
	if top.Version != 2 {
		t.Errorf("top version = %d, want the unknown object's 2", top.Version)
	}
	if len(top.DataTypes) != 0 {
		t.Errorf("data types = %v, want none this binary can name", top.DataTypes)
	}
}

// Nothing under an archive prefix should be unreadable, and a caller cannot tell
// whether such an object occupies a version — so it fails rather than guesses.
func TestOccupancies_RefusesAKeyItCannotRead(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{name: "no version", key: "25395000-25395999/25395651_x_block.json.gz"},
		{name: "no stem at all", key: "25395000-25395999/manifest.txt"},
		{name: "a directory marker", key: "25395000-25395999/"},
		{name: "a negative height", key: "25395000-25395999/-1_1_block.json.gz"},
		{name: "no partition", key: "25395651_1_block.json.gz"},
		{name: "a stem with nothing after it", key: "25395000-25395999/25395651_1_"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Occupancies([]string{"25395000-25395999/25395651_0_block.json.gz", tc.key})

			if !errors.Is(err, ErrUnrecognisedKey) {
				t.Fatalf("error = %v, want ErrUnrecognisedKey", err)
			}
			if !strings.Contains(err.Error(), tc.key) {
				t.Errorf("error = %v, want it to name %q", err, tc.key)
			}
		})
	}
}
