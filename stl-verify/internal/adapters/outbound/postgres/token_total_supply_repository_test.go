package postgres

import (
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

func TestTokenTotalSupply_BuildInsertArgs(t *testing.T) {
	r := &TokenTotalSupplyRepository{buildID: buildregistry.BuildID(7)}

	supply := &entity.TokenTotalSupply{
		ChainID:           1,
		TokenAddress:      common.HexToAddress("0xe7df13b8e3d6740fe17cbe928c7334243d86c92f"),
		TokenSymbol:       "spUSDT",
		TokenDecimals:     6,
		TotalSupply:       big.NewInt(1_000_000_000_000),
		ScaledTotalSupply: big.NewInt(980_000_000_000),
		BlockNumber:       1000,
		BlockVersion:      0,
		BlockTimestamp:    time.Unix(1700000000, 0).UTC(),
		Source:            "sweep",
	}

	_, args := r.buildInsertArgs(supply, 42)

	if got, ok := args[0].(int64); !ok || got != 1 {
		t.Errorf("chain_id arg = %v, want 1", args[0])
	}
	if got, ok := args[1].(int64); !ok || got != 42 {
		t.Errorf("token_id arg = %v, want 42", args[1])
	}
	total, ok := args[2].(pgtype.Numeric)
	if !ok || !total.Valid {
		t.Fatalf("total_supply arg = %v, want valid numeric", args[2])
	}
	if total.Int.Cmp(big.NewInt(1_000_000_000_000)) != 0 {
		t.Errorf("total_supply Int = %s, want 1000000000000", total.Int)
	}
	if total.Exp != -6 {
		t.Errorf("total_supply Exp = %d, want -6", total.Exp)
	}

	scaled, ok := args[3].(pgtype.Numeric)
	if !ok || !scaled.Valid {
		t.Fatalf("scaled_total_supply arg = %v, want valid numeric", args[3])
	}
	if scaled.Int.Cmp(big.NewInt(980_000_000_000)) != 0 {
		t.Errorf("scaled_total_supply Int = %s, want 980000000000", scaled.Int)
	}

	if got, ok := args[7].(string); !ok || got != "sweep" {
		t.Errorf("source arg = %v, want 'sweep'", args[7])
	}
	if got, ok := args[8].(int); !ok || got != 7 {
		t.Errorf("build_id arg = %v, want 7", args[8])
	}
}

func TestTokenTotalSupply_BuildInsertArgs_NilScaledIsNull(t *testing.T) {
	r := &TokenTotalSupplyRepository{buildID: buildregistry.BuildID(1)}

	supply := &entity.TokenTotalSupply{
		ChainID:        1,
		TokenAddress:   common.HexToAddress("0xaaaa"),
		TokenSymbol:    "X",
		TokenDecimals:  18,
		TotalSupply:    big.NewInt(1),
		BlockNumber:    1,
		BlockTimestamp: time.Unix(1, 0).UTC(),
		Source:         "event",
	}

	_, args := r.buildInsertArgs(supply, 1)
	scaled := args[3].(pgtype.Numeric)
	if scaled.Valid {
		t.Error("expected scaled_total_supply to be NULL (invalid) when nil")
	}
}

func TestTokenTotalSupply_Validate(t *testing.T) {
	cases := []struct {
		name    string
		mut     func(*entity.TokenTotalSupply)
		wantErr bool
	}{
		{"ok", func(s *entity.TokenTotalSupply) {}, false},
		{"missing chain", func(s *entity.TokenTotalSupply) { s.ChainID = 0 }, true},
		{"missing address", func(s *entity.TokenTotalSupply) { s.TokenAddress = common.Address{} }, true},
		{"missing supply", func(s *entity.TokenTotalSupply) { s.TotalSupply = nil }, true},
		{"missing block number", func(s *entity.TokenTotalSupply) { s.BlockNumber = 0 }, true},
		{"missing timestamp", func(s *entity.TokenTotalSupply) { s.BlockTimestamp = time.Time{} }, true},
		{"bad source", func(s *entity.TokenTotalSupply) { s.Source = "other" }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &entity.TokenTotalSupply{
				ChainID:        1,
				TokenAddress:   common.HexToAddress("0xaaaa"),
				TotalSupply:    big.NewInt(1),
				BlockNumber:    1,
				BlockTimestamp: time.Unix(1, 0).UTC(),
				Source:         "sweep",
			}
			tc.mut(s)
			err := s.Validate()
			if tc.wantErr {
				if err == nil {
					t.Error("expected error")
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

