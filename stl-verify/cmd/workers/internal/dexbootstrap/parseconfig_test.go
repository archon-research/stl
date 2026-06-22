package dexbootstrap

import (
	"strings"
	"testing"
)

// envSet wraps the boilerplate of clearing every relevant env var and then
// setting only what the test cares about. Without the explicit clear, a
// previously-set var from a prior test (or the caller's shell) leaks in and
// produces non-deterministic results.
func envSet(t *testing.T, vars map[string]string) {
	t.Helper()
	all := []string{
		"AWS_SQS_QUEUE_URL", "DATABASE_URL", "ALCHEMY_API_KEY", "ALCHEMY_HTTP_URL",
		"REDIS_ADDR", "REDIS_PASSWORD", "SQS_WAIT_TIME", "SQS_VISIBILITY_TIMEOUT",
		"CHAIN_ID", "S3_BUCKET", "DEPLOY_ENV",
	}
	for _, k := range all {
		t.Setenv(k, "")
	}
	for k, v := range vars {
		t.Setenv(k, v)
	}
}

// happy = every required var set to a valid value matching mainnet+staging.
func happyEnv() map[string]string {
	return map[string]string{
		"AWS_SQS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/000/q",
		"DATABASE_URL":      "postgres://localhost:5432/stl",
		"ALCHEMY_API_KEY":   "key",
		"REDIS_ADDR":        "redis:6379",
		"CHAIN_ID":          "1",
		"S3_BUCKET":         "stl-sentinelstaging-ethereum-raw",
		"DEPLOY_ENV":        "staging",
	}
}

func TestParseConfig_HappyPath(t *testing.T) {
	envSet(t, happyEnv())
	cfg, err := ParseConfig("test-worker", nil)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.ChainID != 1 {
		t.Errorf("ChainID = %d, want 1", cfg.ChainID)
	}
	if cfg.S3Bucket != "stl-sentinelstaging-ethereum-raw" {
		t.Errorf("S3Bucket = %q, want stl-sentinelstaging-ethereum-raw", cfg.S3Bucket)
	}
	if cfg.DeployEnv != "staging" {
		t.Errorf("DeployEnv = %q, want staging", cfg.DeployEnv)
	}
	if cfg.AlchemyURL != "https://eth-mainnet.g.alchemy.com/v2/key" {
		t.Errorf("AlchemyURL = %q, default Alchemy URL not applied", cfg.AlchemyURL)
	}
	if cfg.MaxMessages != 10 || cfg.WaitTime != 20 || cfg.VisibilityTimeout != 300 {
		t.Errorf("defaults not applied: max=%d wait=%d vis=%d", cfg.MaxMessages, cfg.WaitTime, cfg.VisibilityTimeout)
	}
}

func TestParseConfig_RequiredEnvVars(t *testing.T) {
	cases := []struct {
		drop      string
		mustMatch string
	}{
		{"AWS_SQS_QUEUE_URL", "queue URL not provided"},
		{"DATABASE_URL", "database URL not provided"},
		{"ALCHEMY_API_KEY", "ALCHEMY_API_KEY"},
		{"REDIS_ADDR", "redis address"},
		{"CHAIN_ID", "CHAIN_ID"},
		{"S3_BUCKET", "S3_BUCKET"},
		{"DEPLOY_ENV", "DEPLOY_ENV"},
	}
	for _, tc := range cases {
		t.Run(tc.drop, func(t *testing.T) {
			vars := happyEnv()
			delete(vars, tc.drop)
			envSet(t, vars)
			_, err := ParseConfig("test", nil)
			if err == nil {
				t.Fatalf("expected error when %s is missing", tc.drop)
			}
			if !strings.Contains(err.Error(), tc.mustMatch) {
				t.Errorf("error %q must reference %q so operator can fix the right var", err, tc.mustMatch)
			}
		})
	}
}

func TestParseConfig_RejectsNonNumericChainID(t *testing.T) {
	vars := happyEnv()
	vars["CHAIN_ID"] = "mainnet"
	envSet(t, vars)
	_, err := ParseConfig("test", nil)
	if err == nil {
		t.Fatal("expected error when CHAIN_ID is not an integer")
	}
	if !strings.Contains(err.Error(), "CHAIN_ID") {
		t.Errorf("error %q does not reference CHAIN_ID", err)
	}
}

// N7-3: ParseConfig must call chainutil.ValidateS3BucketForChain so a
// staging-deploy with a prod bucket name (or vice versa) is caught at boot,
// not five hours later when the watcher fans out events under the wrong
// chain ID.
func TestParseConfig_RejectsBucketChainEnvMismatch(t *testing.T) {
	cases := []struct {
		name      string
		mutate    func(map[string]string)
		mustMatch string
	}{
		{
			name: "staging deploy with prod bucket",
			mutate: func(m map[string]string) {
				m["S3_BUCKET"] = "stl-sentinelprod-ethereum-raw"
				m["DEPLOY_ENV"] = "staging"
			},
			mustMatch: "stl-sentinelstaging-ethereum-raw",
		},
		{
			name: "wrong chain ID for ethereum bucket",
			mutate: func(m map[string]string) {
				m["S3_BUCKET"] = "stl-sentinelstaging-ethereum-raw"
				m["CHAIN_ID"] = "8453" // base, but bucket says ethereum
				m["DEPLOY_ENV"] = "staging"
			},
			mustMatch: "stl-sentinelstaging-base-raw",
		},
		{
			name: "bucket without stl-sentinel prefix",
			mutate: func(m map[string]string) {
				m["S3_BUCKET"] = "raw-data-bucket"
				m["DEPLOY_ENV"] = "staging"
			},
			mustMatch: "raw-data-bucket",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vars := happyEnv()
			tc.mutate(vars)
			envSet(t, vars)
			_, err := ParseConfig("test", nil)
			if err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.mustMatch) {
				t.Errorf("error %q must reference %q", err, tc.mustMatch)
			}
		})
	}
}

func TestParseConfig_AppliesEnvOverridesForTimings(t *testing.T) {
	vars := happyEnv()
	vars["SQS_WAIT_TIME"] = "5"
	vars["SQS_VISIBILITY_TIMEOUT"] = "60"
	envSet(t, vars)
	cfg, err := ParseConfig("test", nil)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.WaitTime != 5 {
		t.Errorf("WaitTime = %d, want 5 (env override)", cfg.WaitTime)
	}
	if cfg.VisibilityTimeout != 60 {
		t.Errorf("VisibilityTimeout = %d, want 60 (env override)", cfg.VisibilityTimeout)
	}
}

func TestParseConfig_RejectsMalformedTimingEnvVars(t *testing.T) {
	cases := []struct {
		envKey string
	}{
		{"SQS_WAIT_TIME"},
		{"SQS_VISIBILITY_TIMEOUT"},
	}
	for _, tc := range cases {
		t.Run(tc.envKey, func(t *testing.T) {
			vars := happyEnv()
			vars[tc.envKey] = "twenty"
			envSet(t, vars)
			_, err := ParseConfig("test", nil)
			if err == nil {
				t.Fatalf("expected error when %s is non-numeric", tc.envKey)
			}
			if !strings.Contains(err.Error(), tc.envKey) {
				t.Errorf("error %q must reference %s", err, tc.envKey)
			}
		})
	}
}

func TestParseConfig_RejectsMaxMessagesOutOfRange(t *testing.T) {
	for _, max := range []string{"0", "11"} {
		t.Run(max, func(t *testing.T) {
			envSet(t, happyEnv())
			_, err := ParseConfig("test", []string{"-max", max})
			if err == nil {
				t.Fatalf("expected error for -max %s (AWS caps ReceiveMessage at 10)", max)
			}
			if !strings.Contains(err.Error(), "max messages") {
				t.Errorf("error %q should reference the max-messages range", err)
			}
		})
	}
}

func TestParseConfig_AlchemyURLHasNoDoubleSlash(t *testing.T) {
	vars := happyEnv()
	vars["ALCHEMY_HTTP_URL"] = "https://eth-mainnet.g.alchemy.com/v2/"
	envSet(t, vars)
	cfg, err := ParseConfig("test", nil)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if want := "https://eth-mainnet.g.alchemy.com/v2/key"; cfg.AlchemyURL != want {
		t.Errorf("AlchemyURL = %q, want %q (trailing slash must not double up)", cfg.AlchemyURL, want)
	}
}

func TestParseConfig_ExplicitTimingFlagsBeatEnv(t *testing.T) {
	vars := happyEnv()
	vars["SQS_WAIT_TIME"] = "10"
	vars["SQS_VISIBILITY_TIMEOUT"] = "200"
	envSet(t, vars)
	cfg, err := ParseConfig("test", []string{"-wait", "5", "-visibility-timeout", "120"})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.WaitTime != 5 {
		t.Errorf("WaitTime = %d, want 5 (explicit flag must beat env)", cfg.WaitTime)
	}
	if cfg.VisibilityTimeout != 120 {
		t.Errorf("VisibilityTimeout = %d, want 120 (explicit flag must beat env)", cfg.VisibilityTimeout)
	}
}

func TestParseConfig_FlagOverridesEnvVar(t *testing.T) {
	vars := happyEnv()
	vars["AWS_SQS_QUEUE_URL"] = "env-queue"
	envSet(t, vars)
	cfg, err := ParseConfig("test", []string{"-queue", "flag-queue"})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.QueueURL != "flag-queue" {
		t.Errorf("QueueURL = %q, want flag-queue (flag wins over env)", cfg.QueueURL)
	}
}
