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

// validEnv is the complete working environment; a case overrides one key, and an empty value
// stands for "unset" (env.Require and os.Getenv both read empty as unset).
func validEnv() map[string]string {
	return map[string]string{
		"CHAIN_ID":     "1",
		"DEPLOY_ENV":   "staging",
		"DATABASE_URL": "postgres://u:p@localhost:5432/stl",
		"S3_BUCKET":    ethereumRawBucket,
		"BATCH_SIZE":   "",
		"CONCURRENCY":  "",
		"HEAD_MARGIN":  "",
	}
}

// configWith applies the valid environment plus overrides and loads.
func configWith(t *testing.T, over map[string]string) (config, error) {
	t.Helper()
	for k, v := range validEnv() {
		t.Setenv(k, v)
	}
	for k, v := range over {
		t.Setenv(k, v)
	}
	return loadConfig()
}

// mustConfig loads and fails the test if the environment was refused.
func mustConfig(t *testing.T, over map[string]string) config {
	t.Helper()
	cfg, err := configWith(t, over)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	return cfg
}

// loadConfig runs at registration, so each of these is a worker that does not start rather than a
// run an operator starts before finding out.
func TestLoadConfigRefusesABadEnvironment(t *testing.T) {
	for _, c := range []struct {
		name    string
		over    map[string]string
		wantErr string
	}{
		{"missing database url", map[string]string{"DATABASE_URL": ""}, "DATABASE_URL"},
		{"missing bucket", map[string]string{"S3_BUCKET": ""}, "S3_BUCKET"},
		{"missing chain", map[string]string{"CHAIN_ID": ""}, "CHAIN_ID"},
		{"missing deploy env", map[string]string{"DEPLOY_ENV": ""}, "DEPLOY_ENV"},
		{"non-numeric batch size", map[string]string{"BATCH_SIZE": "many"}, "BATCH_SIZE"},
		{"non-positive batch size", map[string]string{"BATCH_SIZE": "-5"}, "BATCH_SIZE"},
		{"negative concurrency", map[string]string{"CONCURRENCY": "-1"}, "CONCURRENCY"},
		{"negative head margin", map[string]string{"HEAD_MARGIN": "-1"}, "HEAD_MARGIN"},
		// The guard that matters: chain and bucket arrive independently, so another chain's
		// archive would be read under this chain's id.
		{"bucket belongs to another chain", map[string]string{"S3_BUCKET": baseRawBucket}, "bucket"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := configWith(t, c.over)
			if err == nil {
				t.Fatalf("want an error naming %q, got none", c.wantErr)
			}
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(c.wantErr)) {
				t.Errorf("error %q does not name %q", err, c.wantErr)
			}
		})
	}
}

// A working environment loads the values the deployment set.
func TestLoadConfigAcceptsAValidEnvironment(t *testing.T) {
	cfg := mustConfig(t, nil)
	if cfg.chainID != 1 {
		t.Errorf("chainID = %d, want 1", cfg.chainID)
	}
	if cfg.bucket != ethereumRawBucket {
		t.Errorf("bucket = %q, want %q", cfg.bucket, ethereumRawBucket)
	}
	if cfg.deployEnv != "staging" {
		t.Errorf("deployEnv = %q, want staging", cfg.deployEnv)
	}
}

// The head margin is the reason a repeated run does not report normal archive lag as a missing
// object, so it has to be on by default rather than an available knob nobody sets.
func TestLoadConfigDefaultsAndTunables(t *testing.T) {
	for _, c := range []struct {
		name string
		over map[string]string
		got  func(config) int64
		want int64
	}{
		{"head margin is on by default", nil, func(c config) int64 { return c.headMargin }, defaultHeadMargin},
		{"head margin is tunable", map[string]string{"HEAD_MARGIN": "25"}, func(c config) int64 { return c.headMargin }, 25},
		{"head margin can be disabled explicitly", map[string]string{"HEAD_MARGIN": "0"}, func(c config) int64 { return c.headMargin }, 0},
		{"concurrency is tunable", map[string]string{"CONCURRENCY": "4"}, func(c config) int64 { return int64(c.concurrency) }, 4},
		{"batch size is tunable", map[string]string{"BATCH_SIZE": "50"}, func(c config) int64 { return int64(c.batchSize) }, 50},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := c.got(mustConfig(t, c.over)); got != c.want {
				t.Errorf("got %d, want %d", got, c.want)
			}
		})
	}
}
