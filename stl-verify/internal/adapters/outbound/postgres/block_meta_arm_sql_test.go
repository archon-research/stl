package postgres

import (
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/data_quality/schemamaster"
)

// The chain expression is what decides whose blocks an arm claims, so each shape is pinned by the SQL
// it produces rather than by the arm merely building.
func TestArmSQLTakesChainFromTheFillsShape(t *testing.T) {
	tests := []struct {
		name     string
		fill     schemamaster.Fill
		wantSel  string
		wantFrom string
	}{
		{
			name:     "native column when the table declares no fill",
			fill:     schemamaster.Fill{},
			wantSel:  "SELECT t.chain_id,",
			wantFrom: `FROM "protocol_event" t`,
		},
		{
			name:     "the config parent's column",
			fill:     schemamaster.Fill{Parent: "protocol", Key: "protocol_id", Ref: "id"},
			wantSel:  "SELECT p.chain_id,",
			wantFrom: `FROM "protocol_event" t JOIN "protocol" p ON p."id" = t."protocol_id"`,
		},
		{
			name:     "the fill's constant",
			fill:     schemamaster.Fill{Const: new(1)},
			wantSel:  "SELECT 1,",
			wantFrom: `FROM "protocol_event" t`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := armSQL("protocol_event", tt.fill, tt.fill != schemamaster.Fill{})
			if err != nil {
				t.Fatalf("armSQL: %v", err)
			}
			for _, want := range []string{tt.wantSel, tt.wantFrom} {
				if !strings.Contains(got, want) {
					t.Errorf("arm does not contain %q:\n%s", want, got)
				}
			}
		})
	}
}

func TestArmSQLComparesTheConstantToTheRunsChain(t *testing.T) {
	got, err := armSQL("prime_debt", schemamaster.Fill{Const: new(1)}, true)
	if err != nil {
		t.Fatalf("armSQL: %v", err)
	}
	if !strings.Contains(got, "WHERE 1 = $1") {
		t.Errorf("a constant arm must filter on the run's chain, not label rows with the constant:\n%s", got)
	}
}

// A shape the arm cannot build fails here, not as SQL inside a production run.
func TestArmSQLRefusesShapesItCannotBuild(t *testing.T) {
	tests := []struct {
		name string
		fill schemamaster.Fill
		want string
	}{
		{
			name: "two hops",
			fill: schemamaster.Fill{Parent: "morpho_adapter", Key: "morpho_adapter_id", Ref: "id", ThenParent: "morpho_vault", ThenKey: "morpho_vault_id", ThenRef: "id"},
			want: "two hops",
		},
		{
			name: "a join and a constant at once",
			fill: schemamaster.Fill{Parent: "protocol", Key: "protocol_id", Ref: "id", Const: new(1)},
			want: "both",
		},
		{
			name: "declared with no shape at all",
			fill: schemamaster.Fill{Column: "chain_id"},
			want: "neither a parent nor a constant",
		},
		{
			name: "a constant no chain can have",
			fill: schemamaster.Fill{Const: new(0)},
			want: "no chain has that id",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := armSQL("morpho_adapter_state", tt.fill, true)
			if err == nil {
				t.Fatalf("armSQL built %q for a shape it does not handle; it must fail instead", got)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not say what is unhandled (want it to mention %q)", err, tt.want)
			}
		})
	}
}

// Two chain_id fills for one table would leave the arm built from whichever the register lists last.
func TestChainFillsByTableRefusesTwoFillsForOneTable(t *testing.T) {
	fills := []schemamaster.Fill{
		{Table: "prime_debt", Column: "chain_id", Const: new(1)},
		{Table: "borrower", Column: "chain_id", Parent: "protocol", Key: "protocol_id", Ref: "id"},
		{Table: "prime_debt", Column: "chain_id", Const: new(8453)},
	}
	got, err := chainFillsByTable(fills)
	if err == nil {
		t.Fatalf("two chain_id fills for prime_debt were accepted, keeping %+v", got["prime_debt"])
	}
	if !strings.Contains(err.Error(), "prime_debt") {
		t.Errorf("error %q does not name the table", err)
	}
}

func TestChainFillsByTableIndexesOneFillPerTable(t *testing.T) {
	fills := []schemamaster.Fill{
		{Table: "prime_debt", Column: "chain_id", Const: new(1)},
		{Table: "prime_debt", Column: "block_timestamp", BlockMeta: true},
		{Table: "borrower", Column: "chain_id", Parent: "protocol", Key: "protocol_id", Ref: "id"},
	}
	got, err := chainFillsByTable(fills)
	if err != nil {
		t.Fatalf("chainFillsByTable: %v", err)
	}
	if len(got) != 2 || got["prime_debt"].Const == nil || *got["prime_debt"].Const != 1 || got["borrower"].Parent != "protocol" {
		t.Errorf("got %+v, want prime_debt const 1 and borrower via protocol only", got)
	}
}
