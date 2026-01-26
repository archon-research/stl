package outbound

import "context"

// SQSMessage represents a message received from SQS.
type SQSMessage struct {
	// MessageID is the unique ID of the message.
	MessageID string

	// ReceiptHandle is needed to delete the message after processing.
	ReceiptHandle string

	// Body is the raw message body (JSON).
	Body string
}

// SQSConsumer defines the interface for consuming messages from an SQS queue.
type SQSConsumer interface {
	// ReceiveMessages fetches up to maxMessages from the queue.
	// Returns an empty slice if no messages are available.
	ReceiveMessages(ctx context.Context, maxMessages int) ([]SQSMessage, error)

	// DeleteMessage removes a successfully processed message from the queue.
	DeleteMessage(ctx context.Context, receiptHandle string) error

	// Close closes the consumer and releases resources.
	Close() error
}
