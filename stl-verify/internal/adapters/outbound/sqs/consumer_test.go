package sqs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	smithy "github.com/aws/smithy-go"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

type mockSQSAPI struct {
	visibilityBatchInput *sqs.ChangeMessageVisibilityBatchInput
	batchFailed          []sqstypes.BatchResultErrorEntry
	err                  error

	receiveInput *sqs.ReceiveMessageInput
	receiveErr   error
}

func (m *mockSQSAPI) ReceiveMessage(_ context.Context, params *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	m.receiveInput = params
	if m.receiveErr != nil {
		return nil, m.receiveErr
	}
	return &sqs.ReceiveMessageOutput{}, nil
}

func (m *mockSQSAPI) DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}

func (m *mockSQSAPI) ChangeMessageVisibilityBatch(_ context.Context, params *sqs.ChangeMessageVisibilityBatchInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	m.visibilityBatchInput = params
	if m.err != nil {
		return nil, m.err
	}
	return &sqs.ChangeMessageVisibilityBatchOutput{Failed: m.batchFailed}, nil
}

func newTestConsumer(client sqsAPI) *Consumer {
	return &Consumer{
		client:   client,
		queueURL: "https://sqs.test/queue.fifo",
		config:   ConfigDefaults(),
		logger:   slog.Default(),
	}
}

// A ReceiveMessage request aborts the instant its context is done, the way the
// AWS SDK's does.
type blockingReceiveAPI struct {
	mockSQSAPI

	entered chan struct{}
	release chan struct{}

	mu          sync.Mutex
	calls       int
	deadline    time.Time
	hasDeadline bool
	options     sqs.Options
}

func newBlockingReceiveAPI() *blockingReceiveAPI {
	return &blockingReceiveAPI{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (f *blockingReceiveAPI) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.recordCall(ctx, optFns)

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

func (f *blockingReceiveAPI) recordCall(ctx context.Context, optFns []func(*sqs.Options)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.deadline, f.hasDeadline = ctx.Deadline()
	f.options = sqs.Options{}
	for _, fn := range optFns {
		fn(&f.options)
	}
	select {
	case f.entered <- struct{}{}:
	default:
	}
}

func (f *blockingReceiveAPI) receiveOptions() sqs.Options {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.options
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

func TestConsumer_ReceiveMessages_DisablesSDKRetriesForThePoll(t *testing.T) {
	api := newBlockingReceiveAPI()
	close(api.release)
	consumer := newTestConsumer(api)

	if _, err := consumer.ReceiveMessages(context.Background(), 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	retryer := api.receiveOptions().Retryer
	if retryer == nil {
		t.Fatal("expected the poll to override the SDK retryer")
	}
	if got := retryer.MaxAttempts(); got != 1 {
		t.Errorf("expected one attempt per poll, got %d", got)
	}
	// The SDK's default retryer retries a throttle, each retry costing another
	// full WaitTimeSeconds.
	throttle := &smithy.GenericAPIError{Code: "ThrottlingException", Message: "Rate exceeded"}
	if retryer.IsErrorRetryable(throttle) {
		t.Error("expected the poll retryer to refuse retries, leaving retry to the poll loop")
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

func TestNewConsumer_FallsBackToTheDefaultWaitTime(t *testing.T) {
	consumer, err := NewConsumer(aws.Config{}, Config{QueueURL: "https://sqs.test/queue.fifo"}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := consumer.config.WaitTimeSeconds; got != ConfigDefaults().WaitTimeSeconds {
		t.Errorf("expected the default wait time applied, got %d", got)
	}
}

func TestConsumer_ChangeMessageVisibilityBatch_SendsSecondsInRange(t *testing.T) {
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

			if _, err := consumer.ChangeMessageVisibilityBatch(context.Background(), []string{"handle-1"}, tt.visibility); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := client.visibilityBatchInput.Entries[0].VisibilityTimeout; got != tt.wantSeconds {
				t.Errorf("expected VisibilityTimeout %d, got %d", tt.wantSeconds, got)
			}
		})
	}
}

func TestConsumer_ChangeMessageVisibilityBatch_SendsOneEntryPerHandle(t *testing.T) {
	client := &mockSQSAPI{}
	consumer := newTestConsumer(client)

	refusals, err := consumer.ChangeMessageVisibilityBatch(context.Background(), []string{"handle-1", "handle-2"}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(refusals) != 0 {
		t.Errorf("expected no refusals, got %v", refusals)
	}

	entries := client.visibilityBatchInput.Entries
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	for i, want := range []string{"handle-1", "handle-2"} {
		if got := *entries[i].ReceiptHandle; got != want {
			t.Errorf("entry %d carries handle %s, want %s", i, got, want)
		}
		if got := entries[i].VisibilityTimeout; got != 0 {
			t.Errorf("entry %d asks for %ds, want an immediate release", i, got)
		}
	}
}

// A 200 response can still refuse individual entries.
func TestConsumer_ChangeMessageVisibilityBatch_ReportsPerEntryRefusals(t *testing.T) {
	client := &mockSQSAPI{batchFailed: []sqstypes.BatchResultErrorEntry{{
		Id:      aws.String("1"),
		Code:    aws.String("ReceiptHandleIsInvalid"),
		Message: aws.String("The receipt handle has expired"),
	}}}
	consumer := newTestConsumer(client)

	refusals, err := consumer.ChangeMessageVisibilityBatch(context.Background(), []string{"handle-1", "handle-2"}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if refusals["handle-1"] != nil {
		t.Errorf("expected handle-1 released, got %v", refusals["handle-1"])
	}
	if refusals["handle-2"] == nil {
		t.Fatal("expected the refusal attributed to handle-2")
	}
}

func TestConsumer_ChangeMessageVisibilityBatch_RejectsAnUnattributableRefusal(t *testing.T) {
	client := &mockSQSAPI{batchFailed: []sqstypes.BatchResultErrorEntry{{
		Id:   aws.String("99"),
		Code: aws.String("ReceiptHandleIsInvalid"),
	}}}
	consumer := newTestConsumer(client)

	if _, err := consumer.ChangeMessageVisibilityBatch(context.Background(), []string{"handle-1"}, 0); err == nil {
		t.Fatal("expected an unattributable refusal to fail the call")
	}
}

func TestConsumer_ChangeMessageVisibilityBatch_RejectsMoreHandlesThanSQSAccepts(t *testing.T) {
	client := &mockSQSAPI{}
	consumer := newTestConsumer(client)
	handles := make([]string, outbound.MaxVisibilityBatchSize+1)

	if _, err := consumer.ChangeMessageVisibilityBatch(context.Background(), handles, 0); err == nil {
		t.Fatal("expected a batch above the SQS ceiling rejected before the call")
	}
	if client.visibilityBatchInput != nil {
		t.Error("expected no SQS call for an oversized batch")
	}
}

func TestConsumer_ChangeMessageVisibilityBatch_PropagatesError(t *testing.T) {
	apiErr := errors.New("throttled")
	consumer := newTestConsumer(&mockSQSAPI{err: apiErr})

	_, err := consumer.ChangeMessageVisibilityBatch(context.Background(), []string{"handle-1"}, 0)
	if !errors.Is(err, apiErr) {
		t.Fatalf("expected the SQS error wrapped, got %v", err)
	}
}

func TestNewConsumer_RejectsAWaitTimeOutsideTheSQSLongPollRange(t *testing.T) {
	tests := []struct {
		name            string
		waitTimeSeconds int32
		wantErr         bool
	}{
		{"the SQS long-poll maximum is accepted", 20, false},
		{"one second past the maximum is rejected", 21, true},
		{"well past the maximum is rejected", 25, true},
		{"a negative wait time is rejected", -1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConsumer(aws.Config{}, Config{
				QueueURL:        "https://sqs.test/queue.fifo",
				WaitTimeSeconds: tt.waitTimeSeconds,
			}, slog.Default())
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewConsumer(WaitTimeSeconds=%d) error = %v, wantErr %v", tt.waitTimeSeconds, err, tt.wantErr)
			}
		})
	}
}

// The Id is the only thing making SQS's echoed refusal resolvable back to a
// handle, so drift lands a refusal on the wrong message.
func TestConsumer_ChangeMessageVisibilityBatch_NumbersEachEntryByItsIndex(t *testing.T) {
	client := &mockSQSAPI{}
	consumer := newTestConsumer(client)

	if _, err := consumer.ChangeMessageVisibilityBatch(context.Background(), []string{"handle-1", "handle-2", "handle-3"}, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i, entry := range client.visibilityBatchInput.Entries {
		if got, want := aws.ToString(entry.Id), strconv.Itoa(i); got != want {
			t.Errorf("entry %d carries Id %q, want %q", i, got, want)
		}
	}
}

func TestConsumer_ReceiveMessages_PropagatesAPollFailure(t *testing.T) {
	pollErr := errors.New("InvalidParameterValue")
	consumer := newTestConsumer(&mockSQSAPI{receiveErr: pollErr})

	messages, err := consumer.ReceiveMessages(context.Background(), 1)
	if !errors.Is(err, pollErr) {
		t.Fatalf("expected the poll failure wrapped, got %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("expected no messages alongside the failure, got %+v", messages)
	}
}

// A long poll that outruns its budget returned no message, which is what an
// empty receive returns too. Reporting it as a failure puts an ERROR line on
// every slow idle poll — and low-traffic chains idle-poll constantly — while a
// genuine refusal must stay on the error path the alerts key on.
func TestConsumer_ReceiveMessages_SeparatesAPollBudgetExpiryFromAFailure(t *testing.T) {
	tests := []struct {
		name       string
		receiveErr error
		wantErr    bool
		wantWarns  int
	}{
		{
			name:       "a poll that outran its budget is an empty poll",
			receiveErr: fmt.Errorf("operation error SQS ReceiveMessage: %w", context.DeadlineExceeded),
			wantWarns:  1,
		},
		{
			name:       "a refused poll stays a failure",
			receiveErr: errors.New("InvalidParameterValue"),
			wantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := &testutil.SlogRecorder{}
			consumer := newTestConsumer(&mockSQSAPI{receiveErr: tt.receiveErr})
			consumer.logger = slog.New(recorder)

			messages, err := consumer.ReceiveMessages(context.Background(), 1)

			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("ReceiveMessages error = %v, want error: %v", err, tt.wantErr)
			}
			if len(messages) != 0 {
				t.Errorf("expected no messages, got %+v", messages)
			}
			if got := recorder.CountWarn("outran its budget"); got != tt.wantWarns {
				t.Errorf("budget-expiry warnings = %d, want %d (records: %v)", got, tt.wantWarns, recorder.Records)
			}
		})
	}
}

func TestConsumer_ReceiveMessages_ClampsTheBatchSizeToWhatSQSAccepts(t *testing.T) {
	tests := []struct {
		name        string
		maxMessages int
		want        int32
	}{
		{"zero asks for one message", 0, 1},
		{"negative asks for one message", -5, 1},
		{"one passes through", 1, 1},
		{"the SQS ceiling passes through", 10, 10},
		{"above the ceiling is clamped", 25, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockSQSAPI{}
			consumer := newTestConsumer(client)

			if _, err := consumer.ReceiveMessages(context.Background(), tt.maxMessages); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := client.receiveInput.MaxNumberOfMessages; got != tt.want {
				t.Errorf("maxMessages %d asked SQS for %d, want %d", tt.maxMessages, got, tt.want)
			}
		})
	}
}

func TestConsumer_VisibilityTimeout_ReportsTheConfiguredSeconds(t *testing.T) {
	consumer, err := NewConsumer(aws.Config{}, Config{
		QueueURL:          "https://sqs.test/queue.fifo",
		VisibilityTimeout: 300,
	}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := consumer.VisibilityTimeout(); got != 300*time.Second {
		t.Errorf("VisibilityTimeout() = %s, want %s", got, 300*time.Second)
	}
}

// SQS applied the batch before echoing an Id that matches no handle, so the
// entries it did name are the only ones still hidden.
func TestConsumer_ChangeMessageVisibilityBatch_KeepsTheRefusalsItAttributedAlongsideTheError(t *testing.T) {
	client := &mockSQSAPI{batchFailed: []sqstypes.BatchResultErrorEntry{
		{Id: aws.String("1"), Code: aws.String("ReceiptHandleIsInvalid")},
		{Id: aws.String("99"), Code: aws.String("ReceiptHandleIsInvalid")},
	}}
	consumer := newTestConsumer(client)

	refusals, err := consumer.ChangeMessageVisibilityBatch(context.Background(),
		[]string{"handle-1", "handle-2", "handle-3"}, 0)

	if err == nil {
		t.Fatal("expected the unattributable refusal reported")
	}
	if refusals["handle-2"] == nil {
		t.Error("expected the refusal SQS did attribute kept alongside the error")
	}
	if len(refusals) != 1 {
		t.Errorf("expected only the attributed refusal, got %v", refusals)
	}
}
