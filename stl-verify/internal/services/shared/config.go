package shared

import (
	"log/slog"
	"time"
)

// SQSConsumerConfig holds common configuration for SQS consumer services.
type SQSConsumerConfig struct {
	MaxMessages  int
	PollInterval time.Duration
	Logger       *slog.Logger
}

// SQSConsumerConfigDefaults returns default values for SQS consumer configuration.
func SQSConsumerConfigDefaults() SQSConsumerConfig {
	return SQSConsumerConfig{
		MaxMessages:  10,
		PollInterval: 100 * time.Millisecond,
		Logger:       slog.Default(),
	}
}

// ApplyDefaults fills in zero-value fields with defaults.
func (c *SQSConsumerConfig) ApplyDefaults() {
	defaults := SQSConsumerConfigDefaults()
	if c.MaxMessages == 0 {
		c.MaxMessages = defaults.MaxMessages
	}
	if c.PollInterval == 0 {
		c.PollInterval = defaults.PollInterval
	}
	if c.Logger == nil {
		c.Logger = defaults.Logger
	}
}
