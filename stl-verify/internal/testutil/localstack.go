package testutil

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// LocalStackConfig contains connection details for a test LocalStack container.
type LocalStackConfig struct {
	Endpoint string
	Region   string
}

const localStackRegion = "us-east-1"

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
//
// When STL_TEST_LOCALSTACK_ENDPOINT is set it returns that endpoint instead, so
// CI can own one LocalStack per shard rather than one per package. That instance
// has to enable the union of every services string passed here, which
// ci/check-ci-services.sh checks against the workflow.
func StartLocalStackForMain(services string) (cfg LocalStackConfig, cleanup func()) {
	if endpoint, ok := sharedService(EnvLocalStackEndpoint); ok {
		cfg = LocalStackConfig{Endpoint: endpoint, Region: localStackRegion}
		allowDirectConnection(endpointHost(endpoint))
		return cfg, noopCleanup
	}

	cfg, cleanup, err := startLocalStackContainer(services)
	if err != nil {
		log.Fatalf("%v", err)
	}
	return cfg, cleanup
}

func startLocalStackContainer(services string) (cfg LocalStackConfig, cleanup func(), err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg.Region = localStackRegion

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
			return cfg, nil, fmt.Errorf("container runtime unavailable: %w", err)
		}
		return cfg, nil, fmt.Errorf("start LocalStack container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return cfg, nil, fmt.Errorf("get LocalStack host: %w", err)
	}
	port, err := container.MappedPort(ctx, "4566")
	if err != nil {
		return cfg, nil, fmt.Errorf("get LocalStack port: %w", err)
	}
	cfg.Endpoint = fmt.Sprintf("http://%s:%s", host, port.Port())
	allowDirectConnection(host)

	return cfg, func() { _ = container.Terminate(context.Background()) }, nil
}

// allowDirectConnection adds host to both spellings of the no-proxy list, so an
// ambient HTTP proxy does not swallow requests to a local LocalStack.
func allowDirectConnection(host string) {
	if host == "" {
		return
	}
	for _, envVar := range []string{"NO_PROXY", "no_proxy"} {
		noProxy := os.Getenv(envVar)
		if noProxy == "" {
			os.Setenv(envVar, host)
			continue
		}
		// Split, not strings.Contains: "127.0.0.1" is a substring of "127.0.0.10".
		if slices.Contains(strings.Split(noProxy, ","), host) {
			continue
		}
		os.Setenv(envVar, noProxy+","+host)
	}
}

// endpointHost extracts the hostname from a LocalStack endpoint URL, returning
// "" when it cannot be parsed — the no-proxy entry is an optimization, not a
// precondition, so an unparseable endpoint must not fail the suite.
func endpointHost(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// EnsureBucket creates bucket unless it is already there, for a name shared by
// more than one test in a package.
//
// Existing is tolerated because it is expected: an archive bucket is named for the
// worker, not the test, so every test in the package asks for the same one. A name
// that has to be unique per test comes from S3TestBucketName, which needs no
// tolerance and so can be counted against.
func EnsureBucket(t *testing.T, ctx context.Context, client *s3.Client, bucket string) {
	t.Helper()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return
	}

	var owned *s3types.BucketAlreadyOwnedByYou
	var exists *s3types.BucketAlreadyExists
	if errors.As(err, &owned) || errors.As(err, &exists) {
		return
	}
	t.Fatalf("create bucket %s: %v", bucket, err)
}
