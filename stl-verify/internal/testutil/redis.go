package testutil

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// StartRedisForMain starts a Redis container for use in TestMain.
// On error it calls log.Fatal instead of t.Fatal.
func StartRedisForMain() (addr string, cleanup func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        ImageRedis,
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		if IsContainerRuntimeUnavailable(err) {
			log.Fatalf("container runtime unavailable: %v", err)
		}
		log.Fatalf("start Redis container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		log.Fatalf("get Redis host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6379")
	if err != nil {
		log.Fatalf("get Redis port: %v", err)
	}

	addr = fmt.Sprintf("%s:%s", host, port.Port())
	cleanup = func() { _ = container.Terminate(context.Background()) }
	return addr, cleanup
}
