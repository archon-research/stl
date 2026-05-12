package main

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestLoadBackfillConfig(t *testing.T) {
	logger := slog.Default()

	tests := []struct {
		name             string
		batchSizeEnv     string
		pollIntervalEnv  string
		retryMinAgeEnv   string
		setBatchSize     bool
		setPollInterval  bool
		setRetryMinAge   bool
		wantBatchSize    int
		wantPollInterval time.Duration
		wantRetryMinAge  time.Duration
		wantErrSubstring string
	}{
		{
			name:             "defaults when all unset",
			wantBatchSize:    10,
			wantPollInterval: 30 * time.Second,
			wantRetryMinAge:  30 * time.Second,
		},
		{
			name:             "arbitrum override",
			setBatchSize:     true,
			batchSizeEnv:     "100",
			setPollInterval:  true,
			pollIntervalEnv:  "5s",
			wantBatchSize:    100,
			wantPollInterval: 5 * time.Second,
			wantRetryMinAge:  30 * time.Second,
		},
		{
			name:             "negative batch size rejected",
			setBatchSize:     true,
			batchSizeEnv:     "-1",
			wantErrSubstring: "BACKFILL_BATCH_SIZE must be > 0",
		},
		{
			name:             "zero batch size rejected",
			setBatchSize:     true,
			batchSizeEnv:     "0",
			wantErrSubstring: "BACKFILL_BATCH_SIZE must be > 0",
		},
		{
			name:             "non-numeric batch size rejected",
			setBatchSize:     true,
			batchSizeEnv:     "abc",
			wantErrSubstring: "BACKFILL_BATCH_SIZE",
		},
		{
			name:             "negative poll interval rejected",
			setPollInterval:  true,
			pollIntervalEnv:  "-1s",
			wantErrSubstring: "BACKFILL_POLL_INTERVAL must be > 0",
		},
		{
			name:             "zero poll interval rejected",
			setPollInterval:  true,
			pollIntervalEnv:  "0s",
			wantErrSubstring: "BACKFILL_POLL_INTERVAL must be > 0",
		},
		{
			name:             "unparseable poll interval rejected",
			setPollInterval:  true,
			pollIntervalEnv:  "not-a-duration",
			wantErrSubstring: "BACKFILL_POLL_INTERVAL",
		},
		{
			name:             "negative retry min age rejected",
			setRetryMinAge:   true,
			retryMinAgeEnv:   "-1s",
			wantErrSubstring: "BACKFILL_RETRY_MIN_AGE must be > 0",
		},
		{
			name:             "zero retry min age rejected",
			setRetryMinAge:   true,
			retryMinAgeEnv:   "0s",
			wantErrSubstring: "BACKFILL_RETRY_MIN_AGE must be > 0",
		},
		{
			name:             "unparseable retry min age rejected",
			setRetryMinAge:   true,
			retryMinAgeEnv:   "not-a-duration",
			wantErrSubstring: "BACKFILL_RETRY_MIN_AGE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setBatchSize {
				t.Setenv("BACKFILL_BATCH_SIZE", tc.batchSizeEnv)
			}
			if tc.setPollInterval {
				t.Setenv("BACKFILL_POLL_INTERVAL", tc.pollIntervalEnv)
			}
			if tc.setRetryMinAge {
				t.Setenv("BACKFILL_RETRY_MIN_AGE", tc.retryMinAgeEnv)
			}

			cfg, err := loadBackfillConfig(42161, false, false, logger)
			if tc.wantErrSubstring != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil (cfg=%+v)", tc.wantErrSubstring, cfg)
				}
				if !strings.Contains(err.Error(), tc.wantErrSubstring) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.wantErrSubstring)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.BatchSize != tc.wantBatchSize {
				t.Errorf("BatchSize = %d, want %d", cfg.BatchSize, tc.wantBatchSize)
			}
			if cfg.PollInterval != tc.wantPollInterval {
				t.Errorf("PollInterval = %s, want %s", cfg.PollInterval, tc.wantPollInterval)
			}
			if cfg.RetryMinAge != tc.wantRetryMinAge {
				t.Errorf("RetryMinAge = %s, want %s", cfg.RetryMinAge, tc.wantRetryMinAge)
			}
			if cfg.ChainID != 42161 {
				t.Errorf("ChainID = %d, want 42161", cfg.ChainID)
			}
		})
	}
}
