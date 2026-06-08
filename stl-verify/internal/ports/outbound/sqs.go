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

	// ApproximateReceiveCount is the SQS system attribute counting how many
	// times this message has been delivered. 1 on first delivery; >1 means
	// the previous handler returned an error or didn't DeleteMessage in
	// time. Logged on handler error so a stuck poison-pill is visible in
	// metrics before it reaches the DLQ. Zero if the consumer couldn't read
	// the attribute (e.g. older SDK or unsupported queue type).
	ApproximateReceiveCount int
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

	// Close closes the consumer and releases resources.
	Close() error
}
