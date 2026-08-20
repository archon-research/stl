package sqs

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// mockSQSAPI records the last ChangeMessageVisibility call; the other sqsAPI
// methods exist only to satisfy the interface.
type mockSQSAPI struct {
	visibilityInput *sqs.ChangeMessageVisibilityInput
	err             error
}

func (m *mockSQSAPI) ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return &sqs.ReceiveMessageOutput{}, nil
}

func (m *mockSQSAPI) DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}

func (m *mockSQSAPI) ChangeMessageVisibility(_ context.Context, params *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	m.visibilityInput = params
	if m.err != nil {
		return nil, m.err
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func newTestConsumer(client sqsAPI) *Consumer {
	return &Consumer{
		client:   client,
		queueURL: "https://sqs.test/queue.fifo",
		config:   ConfigDefaults(),
		logger:   slog.Default(),
	}
}

func TestConsumer_ChangeMessageVisibility_SendsSecondsInRange(t *testing.T) {
	tests := []struct {
		name        string
		visibility  time.Duration
		wantSeconds int32
	}{
		{"zero releases the message", 0, 0},
		{"whole seconds pass through", 30 * time.Second, 30},
		{"sub-second is rounded to the wire unit", 1500 * time.Millisecond, 2},
		{"negative is clamped to an immediate release", -5 * time.Second, 0},
		{"above the SQS ceiling is clamped", 24 * time.Hour, maxVisibilityTimeoutSeconds},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockSQSAPI{}
			consumer := newTestConsumer(client)

			if err := consumer.ChangeMessageVisibility(context.Background(), "handle-1", tt.visibility); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := client.visibilityInput.VisibilityTimeout; got != tt.wantSeconds {
				t.Errorf("expected VisibilityTimeout %d, got %d", tt.wantSeconds, got)
			}
			if got := *client.visibilityInput.ReceiptHandle; got != "handle-1" {
				t.Errorf("expected receipt handle handle-1, got %s", got)
			}
		})
	}
}

func TestConsumer_ChangeMessageVisibility_PropagatesError(t *testing.T) {
	apiErr := errors.New("receipt handle expired")
	consumer := newTestConsumer(&mockSQSAPI{err: apiErr})

	err := consumer.ChangeMessageVisibility(context.Background(), "handle-1", 0)
	if !errors.Is(err, apiErr) {
		t.Fatalf("expected the SQS error wrapped, got %v", err)
	}
}
