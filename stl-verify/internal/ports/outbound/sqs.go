package outbound

import (
	"context"
	"time"
)

// MaxVisibilityBatchSize is the SQS ceiling on one
// ChangeMessageVisibilityBatch call.
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

	// ChangeMessageVisibility resets how long the received message stays hidden
	// from other consumers, counted from now. Zero hands it back immediately:
	// a worker shutting down uses it to release a message it will not finish,
	// which on a FIFO queue unblocks the whole message group for the successor
	// instead of stalling it until the visibility timeout expires.
	ChangeMessageVisibility(ctx context.Context, receiptHandle string, visibility time.Duration) error

	// ChangeMessageVisibilityBatch does the same for up to
	// MaxVisibilityBatchSize messages in one queue call, so a shutdown handing
	// back a whole batch spends one retryable call rather than one per message:
	// under a single cleanup budget, per-message calls let the first throttled
	// one burn the budget in its own retry chain and strand the rest.
	// Refusals the queue reports per entry come back keyed by receipt handle;
	// a non-nil error means the call itself failed and no handle was changed.
	ChangeMessageVisibilityBatch(ctx context.Context, receiptHandles []string, visibility time.Duration) (map[string]error, error)

	// VisibilityTimeout reports the per-receive visibility timeout configured on
	// the consumer. The consume loop validates it exceeds the handler budget so a
	// message is never redelivered while its handler is still running.
	VisibilityTimeout() time.Duration

	// Close closes the consumer and releases resources.
	Close() error
}
