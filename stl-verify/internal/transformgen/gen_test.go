package transformgen

import (
	"slices"
	"testing"

	"github.com/archon-research/stl/stl-verify/data_quality/schemamaster"
)

// rawSchema is a small fixture builder: ordinal columns + PK.
func rawSchema(table string, cols []string, pk []string) RawSchema {
	rc := make([]RawColumn, len(cols))
	for i, c := range cols {
		rc[i] = RawColumn{Name: c, Ordinal: i + 1}
	}
	return RawSchema{Table: table, Columns: rc, PrimaryKey: pk}
}

// TestPlan_JoinRenameCast covers the register-driven projection for a joined,
// renamed, epoch-cast table (morpho_market_state): the dimension fills lead the
// SELECT, timestamp renames to block_timestamp, and the transformed PK carries the
// rename while the raw PK keeps the raw name.
func TestPlan_JoinRenameCast(t *testing.T) {
	reg, err := schemamaster.Load()
	if err != nil {
		t.Fatalf("load register: %v", err)
	}
	s := rawSchema("morpho_market_state",
		[]string{"morpho_market_id", "block_number", "block_version", "timestamp", "last_update", "processing_version", "build_id"},
		[]string{"morpho_market_id", "block_number", "block_version", "processing_version", "timestamp"})

	p, err := plan(reg, s)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if p.partition != "block_timestamp" {
		t.Errorf("partition = %q, want block_timestamp (renamed from timestamp)", p.partition)
	}
	if p.rawPartition != "timestamp" {
		t.Errorf("rawPartition = %q, want timestamp", p.rawPartition)
	}
	// transformed PK renames timestamp -> block_timestamp; raw PK keeps timestamp.
	if !slices.Contains(p.pkColumns, "block_timestamp") || slices.Contains(p.pkColumns, "timestamp") {
		t.Errorf("pkColumns = %v, want block_timestamp not timestamp", p.pkColumns)
	}
	if !slices.Equal(p.rawPK, s.PrimaryKey) {
		t.Errorf("rawPK = %v, want raw PK %v", p.rawPK, s.PrimaryKey)
	}
	// dimension fills (chain_id, protocol_id) lead the transformed columns.
	if len(p.columns) < 2 || p.columns[0] != "chain_id" || p.columns[1] != "protocol_id" {
		t.Errorf("columns lead = %v, want [chain_id protocol_id ...]", p.columns)
	}
	if p.sourceAlias != "s" {
		t.Errorf("sourceAlias = %q, want s (joined)", p.sourceAlias)
	}
}

// TestPlan_NoJoin covers a native-chain_id, no-join table (maple_syrup_global_state):
// synced_at renames to snapshot_time, no source alias.
func TestPlan_NoJoin(t *testing.T) {
	reg, err := schemamaster.Load()
	if err != nil {
		t.Fatalf("load register: %v", err)
	}
	s := rawSchema("maple_syrup_global_state",
		[]string{"chain_id", "synced_at", "tvl", "processing_version", "build_id"},
		[]string{"chain_id", "synced_at", "processing_version"})

	p, err := plan(reg, s)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if p.sourceAlias != "" {
		t.Errorf("sourceAlias = %q, want empty (no join)", p.sourceAlias)
	}
	if p.partition != "snapshot_time" || p.rawPartition != "synced_at" {
		t.Errorf("partition/rawPartition = %q/%q, want snapshot_time/synced_at", p.partition, p.rawPartition)
	}
	if !slices.Contains(p.pkColumns, "snapshot_time") {
		t.Errorf("pkColumns = %v, want snapshot_time", p.pkColumns)
	}
}

// TestBucket1Tables_MatchConfig guards that every bucket-1 table has storage config.
func TestBucket1Tables_MatchConfig(t *testing.T) {
	for _, name := range Bucket1Tables() {
		if _, ok := tableConfigs[name]; !ok {
			t.Errorf("no tableConfigs entry for bucket-1 table %q", name)
		}
	}
	if len(tableConfigs) != len(Bucket1Tables()) {
		t.Errorf("tableConfigs has %d entries, want %d (one per bucket-1 table)", len(tableConfigs), len(Bucket1Tables()))
	}
}
