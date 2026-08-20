package sqs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
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

// blockingReceiveAPI models the AWS SDK's context handling: a ReceiveMessage
// request aborts the instant its context is done, and otherwise completes when
// the test releases it.
type blockingReceiveAPI struct {
	mockSQSAPI

	entered chan struct{}
	release chan struct{}

	mu          sync.Mutex
	calls       int
	deadline    time.Time
	hasDeadline bool
}

func newBlockingReceiveAPI() *blockingReceiveAPI {
	return &blockingReceiveAPI{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (f *blockingReceiveAPI) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.recordCall(ctx)

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("operation error SQS ReceiveMessage: %w", ctx.Err())
	case <-f.release:
		return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{{
			MessageId:     aws.String("m1"),
			ReceiptHandle: aws.String("h1"),
			Body:          aws.String(`{"chainId":1,"blockNumber":100}`),
		}}}, nil
	}
}

func (f *blockingReceiveAPI) recordCall(ctx context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.deadline, f.hasDeadline = ctx.Deadline()
	select {
	case f.entered <- struct{}{}:
	default:
	}
}

func (f *blockingReceiveAPI) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *blockingReceiveAPI) pollDeadline() (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deadline, f.hasDeadline
}

func awaitPollEntered(t *testing.T, api *blockingReceiveAPI) {
	t.Helper()
	select {
	case <-api.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the poll to reach the SQS client")
	}
}

// receiveResult carries what ReceiveMessages returned on its own goroutine.
type receiveResult struct {
	messages []outbound.SQSMessage
	err      error
}

func awaitReceiveResult(t *testing.T, results <-chan receiveResult) receiveResult {
	t.Helper()
	select {
	case got := <-results:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("ReceiveMessages did not return")
		return receiveResult{}
	}
}

// TestConsumer_ReceiveMessages_CompletesAfterCallerCancellation covers the
// rollout blackout measured in kind: SIGTERM aborted the in-flight long poll,
// SQS still handed the next message to that dead request, and the receipt
// handle never reached the process — so nothing could release it and the FIFO
// group stalled for the whole visibility timeout.
func TestConsumer_ReceiveMessages_CompletesAfterCallerCancellation(t *testing.T) {
	api := newBlockingReceiveAPI()
	consumer := newTestConsumer(api)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan receiveResult, 1)
	go func() {
		messages, err := consumer.ReceiveMessages(ctx, 1)
		results <- receiveResult{messages: messages, err: err}
	}()

	awaitPollEntered(t, api)
	cancel()
	close(api.release)

	got := awaitReceiveResult(t, results)
	if got.err != nil {
		t.Fatalf("expected the poll to complete across cancellation, got %v", got.err)
	}
	if len(got.messages) != 1 || got.messages[0].ReceiptHandle != "h1" {
		t.Fatalf("expected the delivered message to reach the caller, got %+v", got.messages)
	}
}

// TestConsumer_ReceiveMessages_SkipsTheCallWhenAlreadyCancelled is the
// counterpart: a detached poll must not start a fresh 20-second wait after
// shutdown already began.
func TestConsumer_ReceiveMessages_SkipsTheCallWhenAlreadyCancelled(t *testing.T) {
	api := newBlockingReceiveAPI()
	consumer := newTestConsumer(api)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	messages, err := consumer.ReceiveMessages(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected the cancelled caller context returned, got %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("expected no messages, got %+v", messages)
	}
	if calls := api.callCount(); calls != 0 {
		t.Errorf("expected no SQS call after cancellation, got %d", calls)
	}
}

func TestConsumer_ReceiveMessages_BoundsThePollByWaitTimePlusSlack(t *testing.T) {
	api := newBlockingReceiveAPI()
	close(api.release)
	consumer := newTestConsumer(api)

	if _, err := consumer.ReceiveMessages(context.Background(), 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	deadline, ok := api.pollDeadline()
	if !ok {
		t.Fatal("expected the detached poll to carry a deadline, so it cannot hang forever")
	}
	want := time.Duration(ConfigDefaults().WaitTimeSeconds)*time.Second + receiveSlack
	if remaining := time.Until(deadline); remaining > want || remaining < want-time.Second {
		t.Fatalf("expected a deadline of about %s, got %s remaining", want, remaining)
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
