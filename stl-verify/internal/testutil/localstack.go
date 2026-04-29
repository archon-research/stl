package testutil

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// LocalStackConfig contains connection details for a test LocalStack container.
type LocalStackConfig struct {
	Endpoint string
	Region   string
}

// StartLocalStack creates a LocalStack container with the given AWS services
// and returns the container and connection config.
// Services is a comma-separated list (e.g. "sns,sqs" or "sns,sqs,s3").
//
// It also ensures the container host is added to NO_PROXY / no_proxy so that
// HTTP proxy settings present in CI environments do not intercept requests to
// the LocalStack endpoint.
func StartLocalStack(t *testing.T, ctx context.Context, services string) (testcontainers.Container, LocalStackConfig) {
	t.Helper()

	config := LocalStackConfig{
		Region: "us-east-1",
	}

	req := testcontainers.ContainerRequest{
		Image:        ImageLocalStack,
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"SERVICES":               services,
			"DEBUG":                  "0",
			"DISABLE_EVENTS":         "1",
			"SKIP_SSL_CERT_DOWNLOAD": "1",
		},
		WaitingFor: wait.ForLog("Ready."),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		HandleContainerRuntimeError(t, err, "failed to start localstack")
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get localstack host: %v", err)
	}
	port, err := container.MappedPort(ctx, "4566")
	if err != nil {
		t.Fatalf("failed to get localstack port: %v", err)
	}
	config.Endpoint = fmt.Sprintf("http://%s:%s", host, port.Port())

	// Ensure the container host bypasses the HTTP proxy. In containerised CI
	// environments testcontainers resolves the Docker bridge gateway IP (e.g.
	// 172.17.0.1) which is typically NOT in NO_PROXY.
	if noProxy := os.Getenv("NO_PROXY"); !strings.Contains(noProxy, host) {
		t.Setenv("NO_PROXY", noProxy+","+host)
	}
	if noProxy := os.Getenv("no_proxy"); !strings.Contains(noProxy, host) {
		t.Setenv("no_proxy", noProxy+","+host)
	}

	return container, config
}

// NewS3Client constructs an S3 client pointed at the given LocalStack endpoint.
// UsePathStyle is enabled as required by LocalStack.
func NewS3Client(t *testing.T, ctx context.Context, cfg LocalStackConfig) *s3.Client {
	t.Helper()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true // Required for LocalStack
	})
}

// StartLocalStackForMain starts a LocalStack container for use in TestMain.
// On error it calls log.Fatal instead of t.Fatal.
func StartLocalStackForMain(services string) (cfg LocalStackConfig, cleanup func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cfg.Region = "us-east-1"

	req := testcontainers.ContainerRequest{
		Image:        ImageLocalStack,
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"SERVICES":               services,
			"DEBUG":                  "0",
			"DISABLE_EVENTS":         "1",
			"SKIP_SSL_CERT_DOWNLOAD": "1",
		},
		WaitingFor: wait.ForLog("Ready."),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		if IsContainerRuntimeUnavailable(err) {
			log.Fatalf("container runtime unavailable: %v", err)
		}
		log.Fatalf("start LocalStack container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		log.Fatalf("get LocalStack host: %v", err)
	}
	port, err := container.MappedPort(ctx, "4566")
	if err != nil {
		log.Fatalf("get LocalStack port: %v", err)
	}
	cfg.Endpoint = fmt.Sprintf("http://%s:%s", host, port.Port())

	// Ensure the container host bypasses the HTTP proxy.
	if noProxy := os.Getenv("NO_PROXY"); !strings.Contains(noProxy, host) {
		os.Setenv("NO_PROXY", noProxy+","+host)
	}
	if noProxy := os.Getenv("no_proxy"); !strings.Contains(noProxy, host) {
		os.Setenv("no_proxy", noProxy+","+host)
	}

	cleanup = func() { _ = container.Terminate(context.Background()) }
	return cfg, cleanup
}
