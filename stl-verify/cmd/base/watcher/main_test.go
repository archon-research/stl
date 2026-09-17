package main

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

func TestResolveServiceName(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{
			name: "defaults when nothing set",
			env:  map[string]string{},
			want: "stl-watcher",
		},
		{
			name: "SERVICE_NAME wins",
			env:  map[string]string{"SERVICE_NAME": "arbitrum-watcher"},
			want: "arbitrum-watcher",
		},
		{
			name: "OTEL_SERVICE_NAME used when SERVICE_NAME unset",
			env:  map[string]string{"OTEL_SERVICE_NAME": "base-watcher"},
			want: "base-watcher",
		},
		{
			name: "SERVICE_NAME takes precedence over OTEL_SERVICE_NAME",
			env: map[string]string{
				"SERVICE_NAME":      "optimism-watcher",
				"OTEL_SERVICE_NAME": "ignored",
			},
			want: "optimism-watcher",
		},
		{
			name: "empty SERVICE_NAME falls through to OTEL_SERVICE_NAME",
			env: map[string]string{
				"SERVICE_NAME":      "",
				"OTEL_SERVICE_NAME": "unichain-watcher",
			},
			want: "unichain-watcher",
		},
		{
			name: "whitespace-only SERVICE_NAME falls through to default",
			env:  map[string]string{"SERVICE_NAME": "   "},
			want: "stl-watcher",
		},
		{
			name: "leading/trailing whitespace is trimmed",
			env:  map[string]string{"SERVICE_NAME": "  avalanche-watcher  "},
			want: "avalanche-watcher",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			getenv := func(key string) string { return tc.env[key] }
			got := resolveServiceName(getenv)
			if got != tc.want {
				t.Errorf("resolveServiceName() = %q, want %q", got, tc.want)
			}
		})
	}
}

// setEnv applies every pair with t.Setenv so the process env is restored
// when the subtest ends.
func setEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for k, v := range env {
		t.Setenv(k, v)
	}
}

func TestLoadBackfillConfig(t *testing.T) {
	tests := []struct {
		name             string
		env              map[string]string
		wantBatchSize    int
		wantPollInterval time.Duration
		wantRetryMinAge  time.Duration
	}{
		{
			name:             "defaults when all unset",
			env:              map[string]string{},
			wantBatchSize:    10,
			wantPollInterval: 30 * time.Second,
			wantRetryMinAge:  30 * time.Second,
		},
		{
			name:             "arbitrum override",
			env:              map[string]string{"BACKFILL_BATCH_SIZE": "100", "BACKFILL_POLL_INTERVAL": "5s"},
			wantBatchSize:    100,
			wantPollInterval: 5 * time.Second,
			wantRetryMinAge:  30 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)

			cfg, err := loadBackfillConfig(42161, false, false, slog.Default(), nil)
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

func TestLoadBackfillConfig_RejectsInvalid(t *testing.T) {
	tests := []struct {
		name             string
		env              map[string]string
		wantErrSubstring string
	}{
		{name: "negative batch size", env: map[string]string{"BACKFILL_BATCH_SIZE": "-1"}, wantErrSubstring: "BACKFILL_BATCH_SIZE must be > 0"},
		{name: "zero batch size", env: map[string]string{"BACKFILL_BATCH_SIZE": "0"}, wantErrSubstring: "BACKFILL_BATCH_SIZE must be > 0"},
		{name: "non-numeric batch size", env: map[string]string{"BACKFILL_BATCH_SIZE": "abc"}, wantErrSubstring: "BACKFILL_BATCH_SIZE"},
		{name: "negative poll interval", env: map[string]string{"BACKFILL_POLL_INTERVAL": "-1s"}, wantErrSubstring: "BACKFILL_POLL_INTERVAL must be > 0"},
		{name: "zero poll interval", env: map[string]string{"BACKFILL_POLL_INTERVAL": "0s"}, wantErrSubstring: "BACKFILL_POLL_INTERVAL must be > 0"},
		{name: "unparseable poll interval", env: map[string]string{"BACKFILL_POLL_INTERVAL": "not-a-duration"}, wantErrSubstring: "BACKFILL_POLL_INTERVAL"},
		{name: "negative retry min age", env: map[string]string{"BACKFILL_RETRY_MIN_AGE": "-1s"}, wantErrSubstring: "BACKFILL_RETRY_MIN_AGE must be > 0"},
		{name: "zero retry min age", env: map[string]string{"BACKFILL_RETRY_MIN_AGE": "0s"}, wantErrSubstring: "BACKFILL_RETRY_MIN_AGE must be > 0"},
		{name: "unparseable retry min age", env: map[string]string{"BACKFILL_RETRY_MIN_AGE": "not-a-duration"}, wantErrSubstring: "BACKFILL_RETRY_MIN_AGE"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)

			cfg, err := loadBackfillConfig(42161, false, false, slog.Default(), nil)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil (cfg=%+v)", tc.wantErrSubstring, cfg)
			}
			if !strings.Contains(err.Error(), tc.wantErrSubstring) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErrSubstring)
			}
		})
	}
}

// TestLoadBackfillConfig_WiresMetricsRecorder is the regression guard for the
// VEC-277 blocker: the BackfillRecorder must be
// threaded into BackfillConfig.Metrics so the post-cycle invariant counter
// (`backfill_gap_fill_no_canonical_total`) is actually emitted.
func TestLoadBackfillConfig_WiresMetricsRecorder(t *testing.T) {
	logger := slog.Default()

	recorder, err := shared.NewServiceTelemetry()
	if err != nil {
		t.Fatalf("NewServiceTelemetry: %v", err)
	}

	cfg, err := loadBackfillConfig(42161, false, false, logger, recorder)
	if err != nil {
		t.Fatalf("loadBackfillConfig: %v", err)
	}
	if cfg.Metrics == nil {
		t.Fatal("BackfillConfig.Metrics is nil — recorder was not threaded through")
	}
	// The concrete type passed in should round-trip unchanged.
	if cfg.Metrics != recorder {
		t.Fatalf("BackfillConfig.Metrics != recorder (got %T, want *shared.ServiceTelemetry)", cfg.Metrics)
	}
}

// TestNewSubscriberConfig_WiresTelemetry guards the ARCT-398 blocker: Telemetry
// is optional on SubscriberConfig, so omitting it silences the subscriber's
// blocks received/dropped counters without failing anything.
func TestNewSubscriberConfig_WiresTelemetry(t *testing.T) {
	telemetry, err := alchemy.NewTelemetry("base")
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}

	cfg := newSubscriberConfig(watcherConfig{}, slog.Default(), telemetry)

	if cfg.Telemetry != telemetry {
		t.Fatalf("SubscriberConfig.Telemetry = %v, want the telemetry passed in", cfg.Telemetry)
	}
}

func TestLoadLiveTuning(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want liveTuning
	}{
		{
			name: "defaults when all unset",
			env:  map[string]string{},
			want: liveTuning{
				rpcTimeout:   alchemy.ClientConfigDefaults().Timeout,
				headerBuffer: alchemy.SubscriberConfigDefaults().ChannelBufferSize,
			},
		},
		{
			name: "arbitrum override",
			env:  map[string]string{"LIVE_RPC_TIMEOUT": "10s", "SUBSCRIBER_BUFFER_SIZE": "1000"},
			want: liveTuning{rpcTimeout: 10 * time.Second, headerBuffer: 1000},
		},
		{
			name: "timeout alone leaves the buffer at its default",
			env:  map[string]string{"LIVE_RPC_TIMEOUT": "10s"},
			want: liveTuning{rpcTimeout: 10 * time.Second, headerBuffer: alchemy.SubscriberConfigDefaults().ChannelBufferSize},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)

			got, err := loadLiveTuning()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("loadLiveTuning() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestLoadLiveTuning_RejectsInvalid(t *testing.T) {
	tests := []struct {
		name             string
		env              map[string]string
		wantErrSubstring string
	}{
		{name: "zero timeout", env: map[string]string{"LIVE_RPC_TIMEOUT": "0s"}, wantErrSubstring: "LIVE_RPC_TIMEOUT must be > 0"},
		{name: "negative timeout", env: map[string]string{"LIVE_RPC_TIMEOUT": "-5s"}, wantErrSubstring: "LIVE_RPC_TIMEOUT must be > 0"},
		{name: "empty timeout", env: map[string]string{"LIVE_RPC_TIMEOUT": ""}, wantErrSubstring: "LIVE_RPC_TIMEOUT"},
		{name: "unparseable timeout", env: map[string]string{"LIVE_RPC_TIMEOUT": "soon"}, wantErrSubstring: "LIVE_RPC_TIMEOUT"},
		{name: "zero buffer", env: map[string]string{"SUBSCRIBER_BUFFER_SIZE": "0"}, wantErrSubstring: "SUBSCRIBER_BUFFER_SIZE must be > 0"},
		{name: "negative buffer", env: map[string]string{"SUBSCRIBER_BUFFER_SIZE": "-1"}, wantErrSubstring: "SUBSCRIBER_BUFFER_SIZE must be > 0"},
		{name: "empty buffer", env: map[string]string{"SUBSCRIBER_BUFFER_SIZE": ""}, wantErrSubstring: "SUBSCRIBER_BUFFER_SIZE"},
		{name: "non-numeric buffer", env: map[string]string{"SUBSCRIBER_BUFFER_SIZE": "lots"}, wantErrSubstring: "SUBSCRIBER_BUFFER_SIZE"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)

			got, err := loadLiveTuning()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil (tuning=%+v)", tc.wantErrSubstring, got)
			}
			if !strings.Contains(err.Error(), tc.wantErrSubstring) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErrSubstring)
			}
		})
	}
}

// TestLoadWatcherConfig_RejectsBadLiveTuning proves the tuning validation runs
// on the startup path, so a bad configmap value fails the pod instead of
// silently running with the adapter defaults.
func TestLoadWatcherConfig_RejectsBadLiveTuning(t *testing.T) {
	setEnv(t, map[string]string{
		"ALCHEMY_API_KEY":        "test-key",
		"CHAIN_ID":               "42161",
		"AWS_SNS_TOPIC_ARN":      "arn:aws:sns:eu-west-1:000000000000:test",
		"SUBSCRIBER_BUFFER_SIZE": "0",
	})

	_, err := loadWatcherConfig()
	if err == nil || !strings.Contains(err.Error(), "SUBSCRIBER_BUFFER_SIZE must be > 0") {
		t.Fatalf("loadWatcherConfig() error = %v, want SUBSCRIBER_BUFFER_SIZE rejection", err)
	}
}

func TestNewSubscriberConfig_UsesConfiguredBufferSize(t *testing.T) {
	cfg := newSubscriberConfig(watcherConfig{live: liveTuning{headerBuffer: 1000}}, slog.Default(), nil)

	if cfg.ChannelBufferSize != 1000 {
		t.Fatalf("SubscriberConfig.ChannelBufferSize = %d, want 1000", cfg.ChannelBufferSize)
	}
}

func TestNewClientConfig_UsesGivenTimeout(t *testing.T) {
	cfg := newClientConfig(watcherConfig{}, cliOptions{}, slog.Default(), nil, 10*time.Second)

	if cfg.Timeout != 10*time.Second {
		t.Errorf("ClientConfig.Timeout = %s, want 10s", cfg.Timeout)
	}
}
