package main

import (
	"maps"
	"strings"
	"testing"
)

// baseArgs supplies the three flags every case needs so each table row only
// states the timing knobs it is actually exercising.
func baseArgs(extra ...string) []string {
	args := []string{
		"-queue", "https://sqs.eu-west-1.amazonaws.com/123/q",
		"-db", "postgres://localhost/db",
		"-redis", "localhost:6379",
	}
	return append(args, extra...)
}

// baseEnv supplies the env vars parseConfig hard-requires, plus any overrides.
func baseEnv(overrides map[string]string) map[string]string {
	env := map[string]string{
		"ALCHEMY_API_KEY": "test-key",
		"S3_BUCKET":       "stl-sentinelstaging-ethereum-raw",
		"DEPLOY_ENV":      "staging",
	}
	maps.Copy(env, overrides)
	return env
}

func baseCfg() cliConfig {
	return cliConfig{
		queueURL:          "https://sqs.eu-west-1.amazonaws.com/123/q",
		dbURL:             "postgres://localhost/db",
		redisAddr:         "localhost:6379",
		alchemyURL:        "https://eth-mainnet.g.alchemy.com/v2/test-key",
		s3Bucket:          "stl-sentinelstaging-ethereum-raw",
		deployEnv:         "staging",
		maxMessages:       10,
		waitTime:          20,
		visibilityTimeout: 300,
		chainID:           1,
	}
}

func TestParseConfigSQSTimings(t *testing.T) {
	explicitFlags := baseCfg()
	explicitFlags.waitTime = 7
	explicitFlags.visibilityTimeout = 90

	envOverridden := baseCfg()
	envOverridden.waitTime = 5
	envOverridden.visibilityTimeout = 60

	visibilityOnly := baseCfg()
	visibilityOnly.visibilityTimeout = 60

	tests := []struct {
		name    string
		args    []string
		envVars map[string]string
		wantCfg cliConfig
	}{
		{
			name:    "defaults when neither flag nor env is set",
			args:    baseArgs(),
			envVars: baseEnv(nil),
			wantCfg: baseCfg(),
		},
		{
			name:    "SQS_VISIBILITY_TIMEOUT env override",
			args:    baseArgs(),
			envVars: baseEnv(map[string]string{"SQS_VISIBILITY_TIMEOUT": "60"}),
			wantCfg: visibilityOnly,
		},
		{
			name: "both timings from env",
			args: baseArgs(),
			envVars: baseEnv(map[string]string{
				"SQS_WAIT_TIME":          "5",
				"SQS_VISIBILITY_TIMEOUT": "60",
			}),
			wantCfg: envOverridden,
		},
		{
			name: "explicit flags beat their env vars",
			args: baseArgs("-wait", "7", "-visibility-timeout", "90"),
			envVars: baseEnv(map[string]string{
				"SQS_WAIT_TIME":          "5",
				"SQS_VISIBILITY_TIMEOUT": "60",
			}),
			wantCfg: explicitFlags,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t, tt.envVars)

			cfg, err := parseConfig(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg != tt.wantCfg {
				t.Errorf("config mismatch:\n got %+v\nwant %+v", cfg, tt.wantCfg)
			}
		})
	}
}

func TestParseConfigRejectsUnparsableSQSTimings(t *testing.T) {
	tests := []struct {
		name      string
		envVars   map[string]string
		wantError string
	}{
		{
			name:      "non-numeric SQS_WAIT_TIME",
			envVars:   baseEnv(map[string]string{"SQS_WAIT_TIME": "abc"}),
			wantError: "parsing SQS_WAIT_TIME",
		},
		{
			name:      "non-numeric SQS_VISIBILITY_TIMEOUT",
			envVars:   baseEnv(map[string]string{"SQS_VISIBILITY_TIMEOUT": "abc"}),
			wantError: "parsing SQS_VISIBILITY_TIMEOUT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t, tt.envVars)

			_, err := parseConfig(baseArgs())
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantError)
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Errorf("expected error containing %q, got %q", tt.wantError, err.Error())
			}
		})
	}
}

// clearEnv blanks every env var parseConfig reads that the case does not set, so
// a value leaking in from the developer's shell cannot change the outcome.
func clearEnv(t *testing.T, set map[string]string) {
	t.Helper()
	for _, key := range []string{
		"ALCHEMY_API_KEY", "ALCHEMY_HTTP_URL", "AWS_SQS_QUEUE_URL",
		"DATABASE_URL", "REDIS_ADDR", "CHAIN_ID",
		"SQS_WAIT_TIME", "SQS_VISIBILITY_TIMEOUT",
		"S3_BUCKET", "DEPLOY_ENV",
	} {
		if _, has := set[key]; !has {
			t.Setenv(key, "")
		}
	}
	for k, v := range set {
		t.Setenv(k, v)
	}
}
