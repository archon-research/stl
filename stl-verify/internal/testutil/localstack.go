package testutil

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// S3TestBucketName builds a bucket name unique to the calling test, for suites
// that share one LocalStack container and so cannot share a bucket.
func S3TestBucketName(t *testing.T, prefix string) string {
	t.Helper()

	name := prefix + strings.ReplaceAll(SanitizeTestName(t.Name()), "_", "-")
	if len(name) > 63 {
		// Plain truncation would put two sibling subtests sharing a 63-character
		// prefix on one bucket, so spend the tail on a digest of the full name.
		sum := sha256.Sum256([]byte(name))
		name = name[:55] + hex.EncodeToString(sum[:4])
	}
	// TrimRight, not TrimSuffix: truncation can land mid-run of separators, and
	// S3 rejects a name that does not end in a letter or digit.
	return strings.TrimRight(name, "-.")
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
		if noProxy == "" {
			os.Setenv("NO_PROXY", host)
		} else {
			os.Setenv("NO_PROXY", noProxy+","+host)
		}
	}
	if noProxy := os.Getenv("no_proxy"); !strings.Contains(noProxy, host) {
		if noProxy == "" {
			os.Setenv("no_proxy", host)
		} else {
			os.Setenv("no_proxy", noProxy+","+host)
		}
	}

	cleanup = func() { _ = container.Terminate(context.Background()) }
	return cfg, cleanup
}
