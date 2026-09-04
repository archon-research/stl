package main

import (
	"slices"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
)

const (
	ethereumChainID = int64(1)
	baseChainID     = int64(8453)

	ethereumRawBucket = "stl-sentineltest-ethereum-raw-89d540d0"
	baseRawBucket     = "stl-sentineltest-base-raw-89d540d0"
)

func validRun() Config {
	return Config{
		ChainID:    ethereumChainID,
		StartBlock: 25395000,
		EndBlock:   25395999,
		Bucket:     ethereumRawBucket,
	}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name            string
		mutate          func(*Config)
		wantErrContains string
	}{
		{name: "a complete run", mutate: func(*Config) {}},
		{name: "no start block", mutate: func(c *Config) { c.StartBlock = 0 }, wantErrContains: "--start-block"},
		{name: "no end block", mutate: func(c *Config) { c.EndBlock = 0 }, wantErrContains: "--end-block"},
		{name: "a range that runs backwards", mutate: func(c *Config) { c.EndBlock = c.StartBlock - 1 }, wantErrContains: "--end-block"},
		{name: "no bucket", mutate: func(c *Config) { c.Bucket = "" }, wantErrContains: "--bucket"},
		{
			// Without it the tool would audit an L2 archive against Ethereum's data
			// types and report every height as missing traces.
			name:            "no chain",
			mutate:          func(c *Config) { c.ChainID = 0 },
			wantErrContains: "--chain-id",
		},
		{
			// The bucket decides every height's version and holds another chain's
			// blocks; a mismatch is a run that reports holes that are not there.
			name:            "another chain's archive bucket",
			mutate:          func(c *Config) { c.ChainID = baseChainID },
			wantErrContains: "expected prefix",
		},
		{
			name:            "a bucket that is not an archive at all",
			mutate:          func(c *Config) { c.Bucket = "some-other-bucket" },
			wantErrContains: "stl-sentinel",
		},
		{
			name:            "a chain the repo does not watch",
			mutate:          func(c *Config) { c.ChainID = 999999 },
			wantErrContains: "999999",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validRun()
			tc.mutate(&cfg)

			err := validateConfig(cfg)

			if tc.wantErrContains == "" {
				if err != nil {
					t.Fatalf("validateConfig() error = %v, want none", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
				t.Fatalf("error = %v, want one mentioning %q", err, tc.wantErrContains)
			}
		})
	}
}

// The archive holds what the chain's watcher fetches: only Ethereum's has
// traces. Auditing an L2 against Ethereum's set reports every height as missing
// traces, and archiving one schedules trace fetches the node cannot answer.
func TestChainDataTypes(t *testing.T) {
	tests := []struct {
		name            string
		chainID         int64
		want            []s3key.DataType
		wantErrContains string
	}{
		{name: "ethereum", chainID: ethereumChainID, want: []s3key.DataType{s3key.Block, s3key.Receipts, s3key.Traces}},
		{name: "base", chainID: baseChainID, want: []s3key.DataType{s3key.Block, s3key.Receipts}},
		{name: "arbitrum", chainID: 42161, want: []s3key.DataType{s3key.Block, s3key.Receipts}},
		{name: "avalanche", chainID: 43114, want: []s3key.DataType{s3key.Block, s3key.Receipts}},
		{name: "optimism", chainID: 10, want: []s3key.DataType{s3key.Block, s3key.Receipts}},
		{name: "unichain", chainID: 130, want: []s3key.DataType{s3key.Block, s3key.Receipts}},
		{name: "a chain with no declared block-data shape", chainID: 999999, wantErrContains: "999999"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := chainDataTypes(tc.chainID)

			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("error = %v, want one mentioning %q", err, tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("chainDataTypes(%d) error = %v", tc.chainID, err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("chainDataTypes(%d) = %v, want %v", tc.chainID, got, tc.want)
			}
		})
	}
}
