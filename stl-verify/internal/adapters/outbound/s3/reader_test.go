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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type mockS3API struct {
	listObjectsV2Func func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	getObjectFunc     func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	headBucketFunc    func(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
}

func (m *mockS3API) HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	if m.headBucketFunc != nil {
		return m.headBucketFunc(ctx, params, optFns...)
	}
	return &s3.HeadBucketOutput{}, nil
}

func (m *mockS3API) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.listObjectsV2Func != nil {
		return m.listObjectsV2Func(ctx, params, optFns...)
	}
	return &s3.ListObjectsV2Output{}, nil
}

func (m *mockS3API) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.getObjectFunc != nil {
		return m.getObjectFunc(ctx, params, optFns...)
	}
	return &s3.GetObjectOutput{}, nil
}

func TestNewReader(t *testing.T) {
	cfg := aws.Config{}
	logger := slog.Default()

	reader := NewReader(cfg, logger)

	if reader == nil {
		t.Fatal("expected non-nil reader")
	}
	if reader.client == nil {
		t.Error("expected non-nil client")
	}
	if reader.logger == nil {
		t.Error("expected non-nil logger")
	}
}

func TestNewReader_NilLogger(t *testing.T) {
	cfg := aws.Config{}

	reader := NewReader(cfg, nil)

	if reader == nil {
		t.Fatal("expected non-nil reader")
	}
	if reader.logger == nil {
		t.Error("expected default logger when nil is passed")
	}
}

func TestListFiles(t *testing.T) {
	ctx := context.Background()
	testTime := time.Date(2026, 1, 18, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		bucket     string
		prefix     string
		mockOutput *s3.ListObjectsV2Output
		wantCount  int
		wantErr    bool
	}{
		{
			name:   "list files successfully",
			bucket: "test-bucket",
			prefix: "test-prefix/",
			mockOutput: &s3.ListObjectsV2Output{
				Contents: []types.Object{
					{
						Key:          aws.String("test-prefix/file1.txt"),
						Size:         aws.Int64(100),
						LastModified: &testTime,
					},
					{
						Key:          aws.String("test-prefix/file2.txt"),
						Size:         aws.Int64(200),
						LastModified: &testTime,
					},
				},
			},
			wantCount: 2,
			wantErr:   false,
		},
		{
			name:   "skip directory entries",
			bucket: "test-bucket",
			prefix: "test-prefix/",
			mockOutput: &s3.ListObjectsV2Output{
				Contents: []types.Object{
					{
						Key:          aws.String("test-prefix/"),
						Size:         aws.Int64(0),
						LastModified: &testTime,
					},
					{
						Key:          aws.String("test-prefix/file1.txt"),
						Size:         aws.Int64(100),
						LastModified: &testTime,
					},
				},
			},
			wantCount: 1,
			wantErr:   false,
		},
		{
			name:   "skip incomplete objects",
			bucket: "test-bucket",
			prefix: "test-prefix/",
			mockOutput: &s3.ListObjectsV2Output{
				Contents: []types.Object{
					{
						Key:          nil,
						Size:         aws.Int64(100),
						LastModified: &testTime,
					},
					{
						Key:          aws.String("test-prefix/file1.txt"),
						Size:         aws.Int64(100),
						LastModified: &testTime,
					},
				},
			},
			wantCount: 1,
			wantErr:   false,
		},
		{
			name:       "empty bucket",
			bucket:     "test-bucket",
			prefix:     "test-prefix/",
			mockOutput: &s3.ListObjectsV2Output{},
			wantCount:  0,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockS3API{
				listObjectsV2Func: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
					return tt.mockOutput, nil
				},
			}

			reader := &Reader{
				client: mock,
				logger: slog.Default(),
			}

			files, err := reader.ListFiles(ctx, tt.bucket, tt.prefix)

			if (err != nil) != tt.wantErr {
				t.Errorf("ListFiles() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(files) != tt.wantCount {
				t.Errorf("ListFiles() got %d files, want %d", len(files), tt.wantCount)
			}
		})
	}
}

func TestStreamFile(t *testing.T) {
	ctx := context.Background()
	testContent := "test file content"

	tests := []struct {
		name        string
		bucket      string
		key         string
		mockContent string
		wantContent string
		wantErr     bool
	}{
		{
			name:        "read plain file",
			bucket:      "test-bucket",
			key:         "test.txt",
			mockContent: testContent,
			wantContent: testContent,
			wantErr:     false,
		},
		{
			name:        "read gzipped file",
			bucket:      "test-bucket",
			key:         "test.txt.gz",
			mockContent: testContent,
			wantContent: testContent,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var bodyContent []byte
			if strings.HasSuffix(tt.key, ".gz") {
				var buf bytes.Buffer
				gzWriter := gzip.NewWriter(&buf)
				_, err := gzWriter.Write([]byte(tt.mockContent))
				if err != nil {
					t.Fatalf("failed to write gzip content: %v", err)
				}
				gzWriter.Close()
				bodyContent = buf.Bytes()
			} else {
				bodyContent = []byte(tt.mockContent)
			}

			mock := &mockS3API{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(bodyContent)),
					}, nil
				},
			}

			reader := &Reader{
				client: mock,
				logger: slog.Default(),
			}

			rc, err := reader.StreamFile(ctx, tt.bucket, tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("StreamFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				defer rc.Close()

				content, err := io.ReadAll(rc)
				if err != nil {
					t.Errorf("failed to read content: %v", err)
					return
				}

				if string(content) != tt.wantContent {
					t.Errorf("StreamFile() content = %q, want %q", string(content), tt.wantContent)
				}
			}
		})
	}
}

func TestGzipReadCloser(t *testing.T) {
	testContent := "test content"
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	_, err := gzWriter.Write([]byte(testContent))
	if err != nil {
		t.Fatalf("failed to write gzip content: %v", err)
	}
	gzWriter.Close()

	gzReader, err := gzip.NewReader(&buf)
	if err != nil {
		t.Fatalf("failed to create gzip reader: %v", err)
	}

	closer := &gzipReadCloser{
		gzReader: gzReader,
		body:     io.NopCloser(&buf),
	}

	content, err := io.ReadAll(closer)
	if err != nil {
		t.Errorf("Read() error = %v", err)
	}

	if string(content) != testContent {
		t.Errorf("Read() content = %q, want %q", string(content), testContent)
	}

	err = closer.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestReadRange_SendsByteRangeAndReturnsObjectAsStored(t *testing.T) {
	ctx := context.Background()

	var full bytes.Buffer
	gzWriter := gzip.NewWriter(&full)
	if _, err := gzWriter.Write([]byte(`{"hash":"0xabc","transactions":[]}`)); err != nil {
		t.Fatalf("write gzip content: %v", err)
	}
	if err := gzWriter.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	stored := full.Bytes()

	var gotRange string
	reader := &Reader{
		client: &mockS3API{
			getObjectFunc: func(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
				if params.Range != nil {
					gotRange = *params.Range
				}
				return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(stored[:10]))}, nil
			},
		},
		logger: slog.Default(),
	}

	got, err := reader.ReadRange(ctx, "bucket", "0-999/42_1_block.json.gz", 0, 9)
	if err != nil {
		t.Fatalf("ReadRange() error = %v", err)
	}
	if gotRange != "bytes=0-9" {
		t.Errorf("Range header = %q, want %q", gotRange, "bytes=0-9")
	}
	if !bytes.Equal(got, stored[:10]) {
		t.Errorf("ReadRange() = %x, want the stored bytes undecompressed %x", got, stored[:10])
	}
}

// HeadBucket is the startup probe a caller runs before a long job: it answers
// "does this bucket exist and may I list it" in one call.
func TestHeadBucket(t *testing.T) {
	tests := []struct {
		name    string
		headErr error
		wantErr bool
	}{
		{name: "a bucket the caller may list"},
		{name: "a bucket that is not there", headErr: errors.New("NotFound"), wantErr: true},
		{name: "a bucket the caller may not list", headErr: errors.New("AccessDenied"), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var seen s3.HeadBucketInput
			mock := &mockS3API{headBucketFunc: func(_ context.Context, params *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
				seen = *params
				return &s3.HeadBucketOutput{}, tc.headErr
			}}
			reader := &Reader{client: mock, logger: slog.Default()}

			err := reader.HeadBucket(context.Background(), "stl-sentinelstaging-ethereum-raw")

			if (err != nil) != tc.wantErr {
				t.Fatalf("HeadBucket error = %v, wantErr %v", err, tc.wantErr)
			}
			if got := aws.ToString(seen.Bucket); got != "stl-sentinelstaging-ethereum-raw" {
				t.Errorf("headed bucket %q, want the one asked for", got)
			}
			if tc.wantErr && !strings.Contains(err.Error(), "stl-sentinelstaging-ethereum-raw") {
				t.Errorf("error = %v, want it to name the bucket", err)
			}
		})
	}
}
