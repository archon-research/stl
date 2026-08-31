package outbound

import (
	"context"
	"time"
)

// MaxVisibilityBatchSize is the SQS ceiling on one ChangeMessageVisibilityBatch.
const MaxVisibilityBatchSize = 10

// SQSMessage represents a message received from SQS.
type SQSMessage struct {
	// MessageID is the unique ID of the message.
	MessageID string

	// ReceiptHandle is needed to delete the message after processing.
	ReceiptHandle string

	// Body is the raw message body (JSON).
	Body string
}

// DeadLetterPublisher sends failed message bodies to a dead-letter queue so
// that permanent failures are preserved for audit/redrive instead of blocking
// the main queue.
type DeadLetterPublisher interface {
	// Publish sends a failed message body to the dead-letter (FIFO) queue.
	// groupID is the FIFO MessageGroupId.
	Publish(ctx context.Context, body string, groupID string) error
}

// SQSConsumer defines the interface for consuming messages from an SQS queue.
type SQSConsumer interface {
	// ReceiveMessages fetches up to maxMessages from the queue.
	// Returns an empty slice if no messages are available.
	ReceiveMessages(ctx context.Context, maxMessages int) ([]SQSMessage, error)

	// DeleteMessage removes a successfully processed message from the queue.
	DeleteMessage(ctx context.Context, receiptHandle string) error

	// ChangeMessageVisibilityBatch resets how long up to MaxVisibilityBatchSize
	// messages stay hidden, counted from now; zero hands them back immediately.
	// Refusals come back keyed by receipt handle; a non-nil error changed nothing.
	ChangeMessageVisibilityBatch(ctx context.Context, receiptHandles []string, visibility time.Duration) (map[string]error, error)

	// VisibilityTimeout reports the per-receive visibility timeout configured on
	// the consumer. The consume loop validates it exceeds the handler budget so a
	// message is never redelivered while its handler is still running.
	VisibilityTimeout() time.Duration

	// Close closes the consumer and releases resources.
	Close() error
}
