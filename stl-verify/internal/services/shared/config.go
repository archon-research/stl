package shared

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
)

// SQSConsumerConfig holds common configuration for SQS consumer services.
type SQSConsumerConfig struct {
	MaxMessages  int
	PollInterval time.Duration
	Logger       *slog.Logger
	ChainID      int64

	// SupersededBlock is the consume loop's optional supersession check; see
	// sqsutil.Config. A composition root that reads chain state pinned to the
	// event's block hash should supply
	// sqsutil.NewSupersededBlockLookup(blockStateRepo).
	SupersededBlock sqsutil.SupersededBlockLookup
}

// SQSConsumerConfigDefaults returns default values for SQS consumer configuration.
func SQSConsumerConfigDefaults() SQSConsumerConfig {
	return SQSConsumerConfig{
		MaxMessages:  1,
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

// Validate checks that required fields are set.
func (c *SQSConsumerConfig) Validate() error {
	if c.ChainID == 0 {
		return fmt.Errorf("ChainID must be set")
	}
	return nil
}
