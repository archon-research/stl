// CEX feed publisher: publishes raw exchange WebSocket frames to an SNS
// Standard topic. The watcher uses this; horizontally-scaled indexer
// workers consume from an SQS queue subscribed to the topic.
//
// Topic shape: Standard (not FIFO). Orderbook snapshots are point-in-time
// values, not deltas — strict ordering and deduplication don't pull their
// weight here.
//
// Message attributes:
//   - source: exchange identifier ("binance", "bybit", ...)
//
// Body: JSON-encoded entity.RawCEXMessage. The Payload field is base64 in
// the JSON, so the envelope is binary-safe regardless of frame contents.

package sns

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that CEXFeedPublisher implements the port.
var _ outbound.CEXFeedPublisher = (*CEXFeedPublisher)(nil)

// CEXFeedConfig configures the CEX feed publisher.
type CEXFeedConfig struct {
	// TopicARN is the ARN of the SNS Standard topic.
	TopicARN string

	// PublishTimeout caps a single publish attempt. Defaults to 10s.
	PublishTimeout time.Duration

	// Logger is the structured logger.
	Logger *slog.Logger
}

// CEXFeedPublisher publishes raw CEX WebSocket frames to SNS.
type CEXFeedPublisher struct {
	client    SNSPublisher
	config    CEXFeedConfig
	logger    *slog.Logger
	closeOnce sync.Once
	closed    bool
	mu        sync.RWMutex
}

// NewCEXFeedPublisher creates a new publisher.
func NewCEXFeedPublisher(client SNSPublisher, config CEXFeedConfig) (*CEXFeedPublisher, error) {
	if client == nil {
		return nil, errors.New("sns client is required")
	}
	if config.TopicARN == "" {
		return nil, errors.New("topic ARN is required")
	}
	if config.PublishTimeout == 0 {
		config.PublishTimeout = 10 * time.Second
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &CEXFeedPublisher{
		client: client,
		config: config,
		logger: config.Logger.With("component", "sns-cex-feed-publisher"),
	}, nil
}

// Publish sends a single raw CEX message to SNS.
func (p *CEXFeedPublisher) Publish(ctx context.Context, msg entity.RawCEXMessage) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return errors.New("publisher is closed")
	}
	p.mu.RUnlock()

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	input := &sns.PublishInput{
		TopicArn: aws.String(p.config.TopicARN),
		Message:  aws.String(string(body)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"source": {
				DataType:    aws.String("String"),
				StringValue: aws.String(msg.Source),
			},
		},
	}

	publishCtx, cancel := context.WithTimeout(ctx, p.config.PublishTimeout)
	defer cancel()
	if _, err := p.client.Publish(publishCtx, input); err != nil {
		return fmt.Errorf("publish to SNS: %w", err)
	}
	return nil
}

// Close marks the publisher as closed; further Publish calls error.
func (p *CEXFeedPublisher) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		p.logger.Info("CEX feed publisher closed")
	})
	return nil
}
