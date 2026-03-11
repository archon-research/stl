package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestRun_Validation verifies that run returns descriptive errors for each invalid input.
func TestRun_Validation(t *testing.T) {
	ctx := context.Background()
	logger := testLogger()

	tests := []struct {
		name       string
		c          cfg
		wantErrMsg string
	}{
		{
			name:       "wrong env",
			c:          cfg{env: "production", srcBucket: "src", destBucket: "dest", blockCount: 10, region: "eu-west-1"},
			wantErrMsg: `--env must be "staging"`,
		},
		{
			name:       "empty src-bucket",
			c:          cfg{env: "staging", srcBucket: "", destBucket: "dest", blockCount: 10, region: "eu-west-1"},
			wantErrMsg: "--src-bucket is required",
		},
		{
			name:       "empty dest-bucket",
			c:          cfg{env: "staging", srcBucket: "src", destBucket: "", blockCount: 10, region: "eu-west-1"},
			wantErrMsg: "--dest-bucket is required",
		},
		{
			name:       "zero block-count",
			c:          cfg{env: "staging", srcBucket: "src", destBucket: "dest", blockCount: 0, region: "eu-west-1"},
			wantErrMsg: "--block-count must be positive",
		},
		{
			name:       "negative block-count",
			c:          cfg{env: "staging", srcBucket: "src", destBucket: "dest", blockCount: -1, region: "eu-west-1"},
			wantErrMsg: "--block-count must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(ctx, logger, tt.c)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErrMsg) {
				t.Errorf("error = %q; want it to contain %q", err.Error(), tt.wantErrMsg)
			}
		})
	}
}

// TestBuildDestConfig_LocalStack verifies that a non-empty destEndpoint produces a config
// with exactly one option function that enables path-style S3 addressing.
func TestBuildDestConfig_LocalStack(t *testing.T) {
	ctx := context.Background()
	c := cfg{
		region:       "us-east-1",
		destEndpoint: "http://localhost:4566",
	}
	_, optFns, err := buildDestConfig(ctx, c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(optFns) != 1 {
		t.Fatalf("expected 1 option function for LocalStack, got %d", len(optFns))
	}
	var opts awss3.Options
	optFns[0](&opts)
	if !opts.UsePathStyle {
		t.Error("expected UsePathStyle=true for LocalStack config")
	}
}

// TestBuildDestConfig_RealAWS verifies that an empty destEndpoint produces no option functions.
func TestBuildDestConfig_RealAWS(t *testing.T) {
	ctx := context.Background()
	c := cfg{
		region:       "us-east-1",
		destEndpoint: "",
	}
	_, optFns, err := buildDestConfig(ctx, c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(optFns) != 0 {
		t.Errorf("expected no option functions for real AWS, got %d", len(optFns))
	}
}
