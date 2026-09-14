package main

import (
	"strings"
	"testing"
)

const (
	// The Terraform-generated suffix is what makes these names unguessable, and
	// what ValidateS3BucketForChain deliberately ignores.
	ethereumRawBucket = "stl-sentinelstaging-ethereum-raw-89d540d0"
	baseRawBucket     = "stl-sentinelstaging-base-raw-89d540d0"
)

// The task queue is also the Deployment name and the OTel service name, so a
// chain that produced the wrong one would idle on a queue nobody starts runs on.
func TestTaskQueueName(t *testing.T) {
	for _, c := range []struct {
		name    string
		chainID string
		want    string
		wantErr bool
	}{
		{"ethereum is unprefixed", "1", "block-meta-loader", false},
		{"base is prefixed", "8453", "base-block-meta-loader", false},
		{"arbitrum is prefixed", "42161", "arbitrum-block-meta-loader", false},
		{"unset chain is refused", "", "", true},
		{"unknown chain is refused", "999999", "", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("CHAIN_ID", c.chainID)
			got, err := taskQueueName()
			if c.wantErr {
				if err == nil {
					t.Fatalf("want an error for CHAIN_ID %q, got %q", c.chainID, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("taskQueueName: %v", err)
			}
			if got != c.want {
				t.Errorf("task queue = %q, want %q", got, c.want)
			}
		})
	}
}

// loadConfig runs at registration, so every one of these is a worker that does
// not start rather than a run an operator starts before finding out.
func TestLoadConfig(t *testing.T) {
	valid := map[string]string{
		"CHAIN_ID":     "1",
		"DEPLOY_ENV":   "staging",
		"DATABASE_URL": "postgres://user:pw@localhost:5432/stl",
		"S3_BUCKET":    ethereumRawBucket,
		"BATCH_SIZE":   "",
	}
	for _, c := range []struct {
		name     string
		override map[string]string
		wantErr  string
	}{
		{"valid", nil, ""},
		{"missing database url", map[string]string{"DATABASE_URL": ""}, "DATABASE_URL"},
		{"missing bucket", map[string]string{"S3_BUCKET": ""}, "S3_BUCKET"},
		{"missing chain", map[string]string{"CHAIN_ID": ""}, "CHAIN_ID"},
		{"non-numeric batch size", map[string]string{"BATCH_SIZE": "many"}, "BATCH_SIZE"},
		// The guard that matters: chain and bucket arrive independently, so
		// another chain's archive would be read under this chain's id.
		{"bucket belongs to another chain", map[string]string{"S3_BUCKET": baseRawBucket}, "bucket"},
	} {
		t.Run(c.name, func(t *testing.T) {
			for k, v := range valid {
				t.Setenv(k, v)
			}
			for k, v := range c.override {
				t.Setenv(k, v)
			}
			cfg, err := loadConfig()
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("loadConfig: %v", err)
				}
				if cfg.chainID != 1 || cfg.bucket != ethereumRawBucket {
					t.Errorf("config = chain %d bucket %q", cfg.chainID, cfg.bucket)
				}
				return
			}
			if err == nil {
				t.Fatalf("want an error naming %q, got none", c.wantErr)
			}
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(c.wantErr)) {
				t.Errorf("error %q does not name %q", err, c.wantErr)
			}
		})
	}
}
