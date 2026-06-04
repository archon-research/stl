package rawsckey

import (
	"testing"
	"time"
)

func TestBuild(t *testing.T) {
	ts := time.Date(2026, 6, 4, 10, 30, 45, 0, time.UTC)

	tests := []struct {
		name         string
		chainID      int64
		block        int64
		blockVersion int
		buildID      int
		source       string
		method       string
		ts           time.Time
		want         string
	}{
		{
			name:         "mainnet oracle batch",
			chainID:      1,
			block:        21500042,
			blockVersion: 0,
			buildID:      42,
			source:       "oracle-price",
			method:       "latestRoundData",
			ts:           ts,
			want:         "raw-sc-calls/chain_id=1/block=21500000-21500999/bv=0/build_id=42/oracle-price_latestRoundData_20260604T103045Z.jsonl.zst",
		},
		{
			name:         "reorged block",
			chainID:      1,
			block:        21500042,
			blockVersion: 2,
			buildID:      42,
			source:       "sparklend",
			method:       "getReserveData",
			ts:           ts,
			want:         "raw-sc-calls/chain_id=1/block=21500000-21500999/bv=2/build_id=42/sparklend_getReserveData_20260604T103045Z.jsonl.zst",
		},
		{
			name:         "mixed method batch on L2",
			chainID:      8453,
			block:        17999,
			blockVersion: 0,
			buildID:      7,
			source:       "morpho",
			method:       MixedMethod,
			ts:           ts,
			want:         "raw-sc-calls/chain_id=8453/block=17000-17999/bv=0/build_id=7/morpho_mixed_20260604T103045Z.jsonl.zst",
		},
		{
			name:         "block zero",
			chainID:      1,
			block:        0,
			blockVersion: 0,
			buildID:      0,
			source:       "oracle-price",
			method:       "latestRoundData",
			ts:           ts,
			want:         "raw-sc-calls/chain_id=1/block=0-999/bv=0/build_id=0/oracle-price_latestRoundData_20260604T103045Z.jsonl.zst",
		},
		{
			name:         "non-UTC timestamp is normalised to UTC",
			chainID:      1,
			block:        21500042,
			blockVersion: 0,
			buildID:      42,
			source:       "oracle-price",
			method:       "latestRoundData",
			ts:           time.Date(2026, 6, 4, 12, 30, 45, 0, time.FixedZone("CEST", 2*60*60)),
			want:         "raw-sc-calls/chain_id=1/block=21500000-21500999/bv=0/build_id=42/oracle-price_latestRoundData_20260604T103045Z.jsonl.zst",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Build(tt.chainID, tt.block, tt.blockVersion, tt.buildID, tt.source, tt.method, tt.ts)
			if got != tt.want {
				t.Errorf("Build()\n got = %q\nwant = %q", got, tt.want)
			}
		})
	}
}
