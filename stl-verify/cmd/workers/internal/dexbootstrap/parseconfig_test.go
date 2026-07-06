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
		"CHAIN_ID", "S3_BUCKET", "DEPLOY_ENV", "DEX",
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
		"DEX":               "curve",
	}
}

// TestParseConfig_SweepBlocksSet: SweepBlocksSet is true only when an operator
// explicitly provided the knob (flag or env), not for the shared default, so a
// no-sweep DEX factory can warn without tripping on every default boot.
func TestParseConfig_SweepBlocksSet(t *testing.T) {
	tests := []struct {
		name    string
		env     string
		args    []string
		wantSet bool
		wantVal int64
	}{
		{name: "default is not marked set", env: "", args: nil, wantSet: false, wantVal: 50},
		{name: "env var marks set", env: "300", args: nil, wantSet: true, wantVal: 300},
		{name: "explicit flag marks set even at zero", env: "", args: []string{"-sweep-blocks", "0"}, wantSet: true, wantVal: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envSet(t, happyEnv())
			t.Setenv("SWEEP_BLOCKS", tt.env)
			cfg, err := ParseConfig("test", tt.args)
			if err != nil {
				t.Fatalf("ParseConfig: %v", err)
			}
			if cfg.SweepBlocksSet != tt.wantSet {
				t.Errorf("SweepBlocksSet = %v, want %v", cfg.SweepBlocksSet, tt.wantSet)
			}
			if cfg.SweepBlocks != tt.wantVal {
				t.Errorf("SweepBlocks = %d, want %d", cfg.SweepBlocks, tt.wantVal)
			}
		})
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
	if cfg.Dex != "curve" {
		t.Errorf("Dex = %q, want curve", cfg.Dex)
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
		{"DEX", "DEX"},
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

func TestParseConfig_RequiresAlchemyURLForNonMainnet(t *testing.T) {
	vars := happyEnv()
	vars["CHAIN_ID"] = "8453"                          // base
	vars["S3_BUCKET"] = "stl-sentinelstaging-base-raw" // consistent, so the S3 check passes and the Alchemy guard fires
	// ALCHEMY_HTTP_URL intentionally unset
	envSet(t, vars)
	_, err := ParseConfig("test", nil)
	if err == nil {
		t.Fatal("expected error: a non-mainnet chain must set ALCHEMY_HTTP_URL (default is mainnet-only)")
	}
	if !strings.Contains(err.Error(), "ALCHEMY_HTTP_URL") {
		t.Errorf("error %q should name ALCHEMY_HTTP_URL, not fall through to a later check", err)
	}
}

func TestParseConfig_AllowsNonMainnetWithExplicitAlchemyURL(t *testing.T) {
	vars := happyEnv()
	vars["CHAIN_ID"] = "8453"
	vars["S3_BUCKET"] = "stl-sentinelstaging-base-raw"
	vars["ALCHEMY_HTTP_URL"] = "https://base-mainnet.g.alchemy.com/v2"
	envSet(t, vars)
	cfg, err := ParseConfig("test", nil)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if want := "https://base-mainnet.g.alchemy.com/v2/key"; cfg.AlchemyURL != want {
		t.Errorf("AlchemyURL = %q, want %q", cfg.AlchemyURL, want)
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

func TestParseConfig_DexFlagOverridesEnvVar(t *testing.T) {
	vars := happyEnv()
	vars["DEX"] = "curve"
	envSet(t, vars)
	cfg, err := ParseConfig("test", []string{"-dex", "uniswap-v3"})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.Dex != "uniswap-v3" {
		t.Errorf("Dex = %q, want uniswap-v3 (flag wins over env)", cfg.Dex)
	}
}

func TestParseConfig_SweepBlocks(t *testing.T) {
	tests := []struct {
		name    string
		flag    []string
		env     map[string]string
		want    int64
		wantErr bool
	}{
		{name: "default when unset", flag: nil, want: 50},
		{name: "flag overrides default", flag: []string{"-sweep-blocks", "10"}, want: 10},
		{name: "env used when flag absent", env: map[string]string{"SWEEP_BLOCKS": "25"}, want: 25},
		{name: "flag overrides env", flag: []string{"-sweep-blocks", "10"}, env: map[string]string{"SWEEP_BLOCKS": "25"}, want: 10},
		{name: "zero disables sweep", flag: []string{"-sweep-blocks", "0"}, want: 0},
		{name: "negative rejected", flag: []string{"-sweep-blocks", "-1"}, wantErr: true},
		{name: "non-numeric env rejected", env: map[string]string{"SWEEP_BLOCKS": "abc"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envSet(t, happyEnv())
			t.Setenv("SWEEP_BLOCKS", "")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			cfg, err := ParseConfig("test", tt.flag)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.SweepBlocks != tt.want {
				t.Fatalf("SweepBlocks = %d, want %d", cfg.SweepBlocks, tt.want)
			}
		})
	}
}
