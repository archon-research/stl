package sqs

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// mockSendMessageAPI is a mock implementation of the sendMessageAPI interface.
type mockSendMessageAPI struct {
	input   *sqs.SendMessageInput
	output  *sqs.SendMessageOutput
	err     error
	callCnt int
}

func (m *mockSendMessageAPI) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	m.callCnt++
	m.input = params
	if m.err != nil {
		return nil, m.err
	}
	if m.output != nil {
		return m.output, nil
	}
	return &sqs.SendMessageOutput{}, nil
}

// Compile-time check that DeadLetterProducer implements outbound.DeadLetterPublisher.
var _ outbound.DeadLetterPublisher = (*DeadLetterProducer)(nil)

func TestNewDeadLetterPublisher_RequiresQueueURL(t *testing.T) {
	_, err := NewDeadLetterPublisher(aws.Config{}, Config{}, slog.Default())
	if err == nil {
		t.Fatal("expected error for missing queue URL")
	}
	if err.Error() != "queue URL is required" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestNewDeadLetterPublisher_NilLoggerUsesDefault(t *testing.T) {
	p, err := NewDeadLetterPublisher(aws.Config{}, Config{QueueURL: "https://example.com/dlq.fifo"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.logger == nil {
		t.Error("expected logger to be set to default")
	}
}

func TestNewDeadLetterPublisher_BaseEndpointOverride(t *testing.T) {
	// Construction should succeed with a LocalStack-style base endpoint override.
	p, err := NewDeadLetterPublisher(aws.Config{}, Config{
		QueueURL:     "https://example.com/dlq.fifo",
		BaseEndpoint: "http://localhost:4566",
	}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("expected producer to be created")
	}
}

func TestDeadLetterPublisher_Publish_SendsMessage(t *testing.T) {
	mock := &mockSendMessageAPI{}
	p := &DeadLetterProducer{
		client:   mock,
		queueURL: "https://example.com/dlq.fifo",
		logger:   slog.Default(),
	}

	err := p.Publish(context.Background(), `{"block":1}`, "43114")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.callCnt != 1 {
		t.Fatalf("expected 1 SendMessage call, got %d", mock.callCnt)
	}
	if mock.input == nil {
		t.Fatal("expected SendMessage input to be captured")
	}
	if aws.ToString(mock.input.QueueUrl) != "https://example.com/dlq.fifo" {
		t.Errorf("unexpected QueueUrl: %v", aws.ToString(mock.input.QueueUrl))
	}
	if aws.ToString(mock.input.MessageBody) != `{"block":1}` {
		t.Errorf("unexpected MessageBody: %v", aws.ToString(mock.input.MessageBody))
	}
	if aws.ToString(mock.input.MessageGroupId) != "43114" {
		t.Errorf("unexpected MessageGroupId: %v", aws.ToString(mock.input.MessageGroupId))
	}
	// DLQ has ContentBasedDeduplication=true; do NOT set an explicit dedup ID.
	if mock.input.MessageDeduplicationId != nil {
		t.Errorf("expected no MessageDeduplicationId, got %v", aws.ToString(mock.input.MessageDeduplicationId))
	}
}

func TestDeadLetterPublisher_Publish_PropagatesError(t *testing.T) {
	mock := &mockSendMessageAPI{err: errors.New("sqs unavailable")}
	p := &DeadLetterProducer{
		client:   mock,
		queueURL: "https://example.com/dlq.fifo",
		logger:   slog.Default(),
	}

	err := p.Publish(context.Background(), "body", "1")
	if err == nil {
		t.Fatal("expected error to be propagated")
	}
	if !errors.Is(err, mock.err) {
		t.Errorf("expected wrapped sqs error, got: %v", err)
	}
}
