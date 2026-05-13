package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type mockS3WriterAPI struct {
	putObjectFunc    func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	headObjectFunc   func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	deleteObjectFunc func(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)

	putObjectCalls []*s3.PutObjectInput
}

func (m *mockS3WriterAPI) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.putObjectCalls = append(m.putObjectCalls, params)
	if m.putObjectFunc != nil {
		return m.putObjectFunc(ctx, params, optFns...)
	}
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3WriterAPI) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if m.headObjectFunc != nil {
		return m.headObjectFunc(ctx, params, optFns...)
	}
	return &s3.HeadObjectOutput{}, nil
}

func (m *mockS3WriterAPI) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if m.deleteObjectFunc != nil {
		return m.deleteObjectFunc(ctx, params, optFns...)
	}
	return &s3.DeleteObjectOutput{}, nil
}

func newTestWriter(mock *mockS3WriterAPI) *Writer {
	return &Writer{
		client: mock,
		logger: slog.Default(),
	}
}

func TestWriteFile_Writes(t *testing.T) {
	mock := &mockS3WriterAPI{}
	w := newTestWriter(mock)

	body := []byte(`{"hello":"world"}`)
	err := w.WriteFile(context.Background(), "bucket", "k1", bytes.NewReader(body), false)
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	if len(mock.putObjectCalls) != 1 {
		t.Fatalf("expected 1 PutObject call, got %d", len(mock.putObjectCalls))
	}
	input := mock.putObjectCalls[0]
	if aws.ToString(input.Bucket) != "bucket" {
		t.Errorf("Bucket = %q, want %q", aws.ToString(input.Bucket), "bucket")
	}
	if aws.ToString(input.Key) != "k1" {
		t.Errorf("Key = %q, want %q", aws.ToString(input.Key), "k1")
	}
	if input.IfNoneMatch != nil {
		t.Errorf("IfNoneMatch = %q, want nil", aws.ToString(input.IfNoneMatch))
	}
	got, err := io.ReadAll(input.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("body = %q, want %q", string(got), string(body))
	}
}

func TestWriteFile_OverwritesExisting(t *testing.T) {
	mock := &mockS3WriterAPI{}
	w := newTestWriter(mock)

	if err := w.WriteFile(context.Background(), "bucket", "k1", bytes.NewReader([]byte("first")), false); err != nil {
		t.Fatalf("first WriteFile: %v", err)
	}
	if err := w.WriteFile(context.Background(), "bucket", "k1", bytes.NewReader([]byte("second")), false); err != nil {
		t.Fatalf("second WriteFile: %v", err)
	}

	if len(mock.putObjectCalls) != 2 {
		t.Fatalf("expected 2 PutObject calls, got %d", len(mock.putObjectCalls))
	}
	for i, in := range mock.putObjectCalls {
		if in.IfNoneMatch != nil {
			t.Errorf("call %d: IfNoneMatch = %q, want nil", i, aws.ToString(in.IfNoneMatch))
		}
	}
}

func TestWriteFile_Gzip(t *testing.T) {
	mock := &mockS3WriterAPI{}
	w := newTestWriter(mock)

	original := []byte(`{"block":"data"}`)
	err := w.WriteFile(context.Background(), "bucket", "k1", bytes.NewReader(original), true)
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if len(mock.putObjectCalls) != 1 {
		t.Fatalf("expected 1 PutObject call, got %d", len(mock.putObjectCalls))
	}
	input := mock.putObjectCalls[0]
	if aws.ToString(input.ContentEncoding) != "gzip" {
		t.Errorf("ContentEncoding = %q, want %q", aws.ToString(input.ContentEncoding), "gzip")
	}

	gzReader, err := gzip.NewReader(input.Body)
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	roundtrip, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("read gzipped body: %v", err)
	}
	if !bytes.Equal(roundtrip, original) {
		t.Errorf("gzip roundtrip mismatch: got %q, want %q", string(roundtrip), string(original))
	}
}

func TestWriteFile_NoGzip(t *testing.T) {
	mock := &mockS3WriterAPI{}
	w := newTestWriter(mock)

	err := w.WriteFile(context.Background(), "bucket", "k1", bytes.NewReader([]byte("payload")), false)
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if len(mock.putObjectCalls) != 1 {
		t.Fatalf("expected 1 PutObject call, got %d", len(mock.putObjectCalls))
	}
	if mock.putObjectCalls[0].ContentEncoding != nil {
		t.Errorf("ContentEncoding = %q, want nil", aws.ToString(mock.putObjectCalls[0].ContentEncoding))
	}
}

func TestWriteFile_PropagatesPutError(t *testing.T) {
	wantErr := errors.New("boom")
	mock := &mockS3WriterAPI{
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return nil, wantErr
		},
	}
	w := newTestWriter(mock)

	err := w.WriteFile(context.Background(), "bucket", "k1", bytes.NewReader([]byte("x")), false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("error chain missing wantErr: %v", err)
	}
	if !strings.Contains(err.Error(), "failed to write to S3") {
		t.Errorf("error message should be wrapped, got: %v", err)
	}
}
