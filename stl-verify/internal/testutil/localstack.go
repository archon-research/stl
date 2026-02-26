package testutil

import (
	"context"
	"fmt"
	"testing"

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
func StartLocalStack(t *testing.T, ctx context.Context, services string) (testcontainers.Container, LocalStackConfig) {
	t.Helper()

	config := LocalStackConfig{
		Region: "us-east-1",
	}

	req := testcontainers.ContainerRequest{
		Image:        "localstack/localstack:4.3",
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
		t.Fatalf("failed to start localstack: %v", err)
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

	return container, config
}
