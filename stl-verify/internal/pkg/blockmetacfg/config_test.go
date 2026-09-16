package blockmetacfg

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
func configWith(t *testing.T, over map[string]string) (Config, error) {
	t.Helper()
	for k, v := range validEnv() {
		t.Setenv(k, v)
	}
	for k, v := range over {
		t.Setenv(k, v)
	}
	return Load()
}

// mustConfig loads and fails the test if the environment was refused.
func mustConfig(t *testing.T, over map[string]string) Config {
	t.Helper()
	cfg, err := configWith(t, over)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return cfg
}

// Load runs at registration, so each of these is a worker that does not start rather than a
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
	if cfg.ChainID != 1 {
		t.Errorf("chainID = %d, want 1", cfg.ChainID)
	}
	if cfg.Bucket != ethereumRawBucket {
		t.Errorf("bucket = %q, want %q", cfg.Bucket, ethereumRawBucket)
	}
	if cfg.DeployEnv != "staging" {
		t.Errorf("deployEnv = %q, want staging", cfg.DeployEnv)
	}
}

// The head margin is the reason a repeated run does not report normal archive lag as a missing
// object, so it has to be on by default rather than an available knob nobody sets.
func TestLoadConfigDefaultsAndTunables(t *testing.T) {
	for _, c := range []struct {
		name string
		over map[string]string
		got  func(Config) int64
		want int64
	}{
		{"head margin is on by default", nil, func(c Config) int64 { return c.HeadMargin }, defaultHeadMargin},
		{"head margin is tunable", map[string]string{"HEAD_MARGIN": "25"}, func(c Config) int64 { return c.HeadMargin }, 25},
		{"head margin can be disabled explicitly", map[string]string{"HEAD_MARGIN": "0"}, func(c Config) int64 { return c.HeadMargin }, 0},
		{"concurrency is tunable", map[string]string{"CONCURRENCY": "4"}, func(c Config) int64 { return int64(c.Concurrency) }, 4},
		{"batch size is tunable", map[string]string{"BATCH_SIZE": "50"}, func(c Config) int64 { return int64(c.BatchSize) }, 50},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := c.got(mustConfig(t, c.over)); got != c.want {
				t.Errorf("got %d, want %d", got, c.want)
			}
		})
	}
}
