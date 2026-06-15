package orderbook

import "testing"

func TestConfigWithDefaults(t *testing.T) {
	c := Config{}.withDefaults()
	d := DefaultConfig()
	if c.Logger == nil {
		t.Error("withDefaults must fill the logger")
	}
	if c.InitialBackoff != d.InitialBackoff || c.MaxBackoff != d.MaxBackoff ||
		c.BackoffFactor != d.BackoffFactor || c.OutputBuffer != d.OutputBuffer {
		t.Errorf("withDefaults left a zero field unfilled: %+v", c)
	}
	// Transport fields are deliberately left zero here: wsclient applies its own
	// defaults at dial time, and a negative PingInterval (disables keepalive)
	// passes through untouched.
	if got := c.PingInterval; got != 0 {
		t.Errorf("PingInterval = %v, want 0 (deferred to wsclient)", got)
	}
}
