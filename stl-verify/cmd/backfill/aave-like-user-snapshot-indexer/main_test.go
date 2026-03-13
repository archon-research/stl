package main

import (
	"os"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestParseFlags(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		wantErr bool
		wantCfg *cliConfig
	}{
		{
			name:    "missing --rpc-url is rejected",
			args:    []string{"--csv", "users.csv", "--protocol", "spark_ethereum", "--db", "postgres://x"},
			wantErr: true,
		},
		{
			name:    "missing --csv is rejected",
			args:    []string{"--rpc-url", "http://x", "--protocol", "spark_ethereum", "--db", "postgres://x"},
			wantErr: true,
		},
		{
			name:    "missing --protocol is rejected",
			args:    []string{"--rpc-url", "http://x", "--csv", "users.csv", "--db", "postgres://x"},
			wantErr: true,
		},
		{
			name:    "unknown protocol slug is rejected",
			args:    []string{"--rpc-url", "http://x", "--csv", "users.csv", "--protocol", "unknown_protocol", "--db", "postgres://x"},
			wantErr: true,
		},
		{
			name:    "missing --db with no DATABASE_URL env var is rejected",
			args:    []string{"--rpc-url", "http://x", "--csv", "users.csv", "--protocol", "spark_ethereum"},
			wantErr: true,
		},
		{
			name:    "missing --db falls back to DATABASE_URL env var",
			args:    []string{"--rpc-url", "http://x", "--csv", "users.csv", "--protocol", "spark_ethereum"},
			env:     map[string]string{"DATABASE_URL": "postgres://from-env"},
			wantErr: false,
			wantCfg: &cliConfig{
				rpcURL:          "http://x",
				csvPath:         "users.csv",
				protocolSlug:    "spark_ethereum",
				dbURL:           "postgres://from-env",
				concurrency:     10,
				chainID:         1,
				protocolAddress: common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987"),
			},
		},
		{
			name: "valid full set of flags",
			args: []string{
				"--rpc-url", "http://erigon:8545",
				"--csv", "users.csv",
				"--protocol", "aave_v3_ethereum",
				"--db", "postgres://localhost/stl",
				"--concurrency", "5",
			},
			wantErr: false,
			wantCfg: &cliConfig{
				rpcURL:          "http://erigon:8545",
				csvPath:         "users.csv",
				protocolSlug:    "aave_v3_ethereum",
				dbURL:           "postgres://localhost/stl",
				concurrency:     5,
				chainID:         1,
				protocolAddress: common.HexToAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"),
			},
		},
		{
			name:    "unknown flag is rejected",
			args:    []string{"--unknown-flag", "value"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				os.Setenv(k, v)
			}
			t.Cleanup(func() {
				for k := range tt.env {
					os.Unsetenv(k)
				}
			})

			cfg, err := parseFlags(tt.args)
			if tt.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.wantCfg != nil && err == nil {
				if cfg.rpcURL != tt.wantCfg.rpcURL {
					t.Errorf("rpcURL: got %q, want %q", cfg.rpcURL, tt.wantCfg.rpcURL)
				}
				if cfg.csvPath != tt.wantCfg.csvPath {
					t.Errorf("csvPath: got %q, want %q", cfg.csvPath, tt.wantCfg.csvPath)
				}
				if cfg.protocolSlug != tt.wantCfg.protocolSlug {
					t.Errorf("protocolSlug: got %q, want %q", cfg.protocolSlug, tt.wantCfg.protocolSlug)
				}
				if cfg.dbURL != tt.wantCfg.dbURL {
					t.Errorf("dbURL: got %q, want %q", cfg.dbURL, tt.wantCfg.dbURL)
				}
				if cfg.concurrency != tt.wantCfg.concurrency {
					t.Errorf("concurrency: got %d, want %d", cfg.concurrency, tt.wantCfg.concurrency)
				}
				if cfg.chainID != tt.wantCfg.chainID {
					t.Errorf("chainID: got %d, want %d", cfg.chainID, tt.wantCfg.chainID)
				}
				if cfg.protocolAddress != tt.wantCfg.protocolAddress {
					t.Errorf("protocolAddress: got %s, want %s", cfg.protocolAddress.Hex(), tt.wantCfg.protocolAddress.Hex())
				}
			}
		})
	}
}

func TestParseCSVUsers(t *testing.T) {
	tests := []struct {
		name         string
		csv          string
		protocolSlug string
		wantUsers    []string
		wantErr      bool
	}{
		{
			name: "filters by protocol slug",
			csv: `user_address,protocols
0x0000000000000000000000000000000000000001,[spark_ethereum]
0x0000000000000000000000000000000000000002,[aave_v3_ethereum]
0x0000000000000000000000000000000000000003,[aave_v3_ethereum spark_ethereum]`,
			protocolSlug: "spark_ethereum",
			wantUsers: []string{
				"0x0000000000000000000000000000000000000001",
				"0x0000000000000000000000000000000000000003",
			},
		},
		{
			name: "deduplicates users",
			csv: `user_address,protocols
0x0000000000000000000000000000000000000001,[spark_ethereum]
0x0000000000000000000000000000000000000001,[spark_ethereum]`,
			protocolSlug: "spark_ethereum",
			wantUsers: []string{
				"0x0000000000000000000000000000000000000001",
			},
		},
		{
			name: "returns empty for no matches",
			csv: `user_address,protocols
0x0000000000000000000000000000000000000001,[aave_v3_ethereum]`,
			protocolSlug: "spark_ethereum",
			wantUsers:    []string{},
		},
		{
			name:         "missing columns returns error",
			csv:          "address,chain\n0x1,1\n",
			protocolSlug: "spark_ethereum",
			wantErr:      true,
		},
		{
			name:         "empty CSV returns error",
			csv:          "",
			protocolSlug: "spark_ethereum",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			users, err := parseCSVUsers(strings.NewReader(tt.csv), tt.protocolSlug)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(users) != len(tt.wantUsers) {
				t.Fatalf("got %d users, want %d", len(users), len(tt.wantUsers))
			}
			for i, want := range tt.wantUsers {
				got := users[i].Hex()
				wantAddr := common.HexToAddress(want).Hex()
				if got != wantAddr {
					t.Errorf("user[%d] = %s, want %s", i, got, wantAddr)
				}
			}
		})
	}
}
