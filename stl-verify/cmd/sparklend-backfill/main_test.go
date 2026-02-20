package main

import (
	"os"
	"testing"
)

func TestParseFlags(t *testing.T) {
	// Unset DATABASE_URL before each test so env doesn't bleed across cases.
	t.Cleanup(func() { os.Unsetenv("DATABASE_URL") })

	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		wantErr bool
	}{
		{
			name:    "missing --from defaults to -1 and is rejected",
			args:    []string{"--to", "100", "--bucket", "b", "--rpc-url", "http://x", "--db", "postgres://x"},
			wantErr: true,
		},
		{
			name:    "missing --to defaults to -1 and is rejected",
			args:    []string{"--from", "0", "--bucket", "b", "--rpc-url", "http://x", "--db", "postgres://x"},
			wantErr: true,
		},
		{
			name:    "from 0 to 0 is valid (block zero)",
			args:    []string{"--from", "0", "--to", "0", "--bucket", "b", "--rpc-url", "http://x", "--db", "postgres://x"},
			wantErr: false,
		},
		{
			name:    "--to < --from is rejected",
			args:    []string{"--from", "100", "--to", "50", "--bucket", "b", "--rpc-url", "http://x", "--db", "postgres://x"},
			wantErr: true,
		},
		{
			name:    "missing --bucket is rejected",
			args:    []string{"--from", "0", "--to", "10", "--rpc-url", "http://x", "--db", "postgres://x"},
			wantErr: true,
		},
		{
			name:    "missing --rpc-url is rejected",
			args:    []string{"--from", "0", "--to", "10", "--bucket", "b", "--db", "postgres://x"},
			wantErr: true,
		},
		{
			name:    "missing --db with no DATABASE_URL env var is rejected",
			args:    []string{"--from", "0", "--to", "10", "--bucket", "b", "--rpc-url", "http://x"},
			wantErr: true,
		},
		{
			name:    "missing --db falls back to DATABASE_URL env var",
			args:    []string{"--from", "0", "--to", "10", "--bucket", "b", "--rpc-url", "http://x"},
			env:     map[string]string{"DATABASE_URL": "postgres://from-env"},
			wantErr: false,
		},
		{
			name: "valid full set of flags",
			args: []string{
				"--from", "1000",
				"--to", "2000",
				"--bucket", "my-bucket",
				"--rpc-url", "http://erigon:8545",
				"--db", "postgres://localhost/stl",
				"--chain-id", "1",
				"--concurrency", "4",
				"--aws-region", "eu-west-1",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Apply env vars for this test case.
			for k, v := range tt.env {
				os.Setenv(k, v)
			}
			t.Cleanup(func() {
				for k := range tt.env {
					os.Unsetenv(k)
				}
			})

			_, err := parseFlags(tt.args)
			if tt.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
