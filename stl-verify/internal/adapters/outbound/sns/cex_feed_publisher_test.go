package sns

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

func TestNewCEXFeedPublisher_RequiresClient(t *testing.T) {
	_, err := NewCEXFeedPublisher(nil, CEXFeedConfig{TopicARN: "arn:..."})
	if err == nil {
		t.Error("expected error for nil client")
	}
}

func TestNewCEXFeedPublisher_RequiresTopicARN(t *testing.T) {
	_, err := NewCEXFeedPublisher(&mockSNSClient{}, CEXFeedConfig{})
	if err == nil {
		t.Error("expected error for empty topic ARN")
	}
}

func TestCEXFeedPublisher_PublishSendsEnvelopeAndAttribute(t *testing.T) {
	client := &mockSNSClient{}
	pub, err := NewCEXFeedPublisher(client, CEXFeedConfig{TopicARN: "arn:topic"})
	if err != nil {
		t.Fatal(err)
	}

	msg := entity.RawCEXMessage{
		Source:     "binance",
		CapturedAt: time.Unix(1700000000, 0).UTC(),
		Payload:    []byte(`{"depth":[]}`),
	}
	if err := pub.Publish(context.Background(), msg); err != nil {
		t.Fatal(err)
	}

	if len(client.calls) != 1 {
		t.Fatalf("expected 1 publish call, got %d", len(client.calls))
	}
	call := client.calls[0]

	if call.TopicArn == nil || *call.TopicArn != "arn:topic" {
		t.Errorf("unexpected topic ARN: %v", call.TopicArn)
	}

	srcAttr, ok := call.MessageAttributes["source"]
	if !ok {
		t.Fatal("missing source message attribute")
	}
	if srcAttr.StringValue == nil || *srcAttr.StringValue != "binance" {
		t.Errorf("unexpected source attribute: %v", srcAttr.StringValue)
	}

	if call.Message == nil {
		t.Fatal("missing message body")
	}
	var decoded entity.RawCEXMessage
	if err := json.Unmarshal([]byte(*call.Message), &decoded); err != nil {
		t.Fatalf("envelope unmarshal: %v", err)
	}
	if decoded.Source != "binance" {
		t.Errorf("decoded.Source=%q", decoded.Source)
	}
	if string(decoded.Payload) != `{"depth":[]}` {
		t.Errorf("decoded.Payload=%q", decoded.Payload)
	}
}

func TestCEXFeedPublisher_PublishAfterCloseErrors(t *testing.T) {
	pub, err := NewCEXFeedPublisher(&mockSNSClient{}, CEXFeedConfig{TopicARN: "arn:topic"})
	if err != nil {
		t.Fatal(err)
	}
	if err := pub.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish(context.Background(), entity.RawCEXMessage{Source: "x"}); err == nil {
		t.Error("expected error publishing after close")
	}
}

func TestCEXFeedPublisher_PropagatesSNSError(t *testing.T) {
	wantErr := errors.New("boom")
	client := &mockSNSClient{
		publishFunc: func(_ context.Context, _ *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			return nil, wantErr
		},
	}
	pub, err := NewCEXFeedPublisher(client, CEXFeedConfig{TopicARN: "arn:topic"})
	if err != nil {
		t.Fatal(err)
	}
	err = pub.Publish(context.Background(), entity.RawCEXMessage{Source: "binance", Payload: []byte("x")})
	if err == nil || !errors.Is(err, wantErr) {
		t.Errorf("expected wrapped wantErr, got %v", err)
	}
}
