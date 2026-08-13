package httpclient

import (
	"testing"
	"time"
)

func TestBuildRetryConfig_EnablesJitterAndMapsFields(t *testing.T) {
	cfg := Config{
		MaxRetries:     4,
		InitialBackoff: 2 * time.Second,
		MaxBackoff:     20 * time.Second,
		BackoffFactor:  3.0,
	}

	rc := buildRetryConfig(cfg)

	if !rc.Jitter {
		t.Error("Jitter = false, want true (retry backoff must be jittered so callers sharing an upstream rate limit do not re-collide in lockstep after a throttle)")
	}
	if rc.MaxRetries != cfg.MaxRetries ||
		rc.InitialBackoff != cfg.InitialBackoff ||
		rc.MaxBackoff != cfg.MaxBackoff ||
		rc.BackoffFactor != cfg.BackoffFactor {
		t.Errorf("field mapping wrong: got %+v", rc)
	}
}
