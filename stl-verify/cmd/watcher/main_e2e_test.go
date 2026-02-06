//go:build e2e

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	rediscache "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	snsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sns"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/live_data"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// newTestBlockchainClient returns a MockBlockchainClient configured for
// watcher e2e tests. It sets UseBlockErrForMissing so that GetBlocksBatch
// returns BlockErr on blocks not previously added.
func newTestBlockchainClient() *testutil.MockBlockchainClient {
	c := testutil.NewMockBlockchainClient()
	c.UseBlockErrForMissing = true
	return c
}

// =============================================================================
// Test Cases
// =============================================================================

// TestEndToEnd_LiveService_ProcessesNewBlock tests the full flow of processing a new block.
func TestEndToEnd_LiveService_ProcessesNewBlock(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Set up infrastructure
	infra := setupTestInfrastructure(t, ctx)
	t.Cleanup(infra.Cleanup)

	// Create mock subscriber that will emit blocks
	mockSub := testutil.NewMockSubscriber()

	// Create mock blockchain client
	mockClient := newTestBlockchainClient()

	// Create live service
	config := live_data.LiveConfig{
		ChainID:            1,
		FinalityBlockCount: 2, // Use small finality for faster tests
		EnableBlobs:        false,
		Logger:             slog.Default(),
	}

	liveService, err := live_data.NewLiveService(
		config,
		mockSub,
		mockClient,
		infra.BlockStateRepo,
		infra.Cache,
		infra.EventSink,
	)
	if err != nil {
		t.Fatalf("failed to create live service: %v", err)
	}

	// Start the service
	if err := liveService.Start(ctx); err != nil {
		t.Fatalf("failed to start live service: %v", err)
	}
	defer liveService.Stop()

	// Set up block data that mock client will return
	mockClient.AddBlock(100, "0x"+fmt.Sprintf("%064x", 99))

	// Emit block notification via header
	header := mockClient.GetHeader(100)
	mockSub.SendHeader(header)

	// Wait for messages on all queues
	const messageTimeout = 10 * time.Second

	t.Run("block_published", func(t *testing.T) {
		msg := infra.WaitForMessage(t, infra.BlocksQueueURL, messageTimeout)
		if msg == "" {
			t.Fatal("expected block message, got none")
		}
		t.Logf("Received block message: %s", truncate(msg, 200))

		// Verify the message contains expected data
		var blockMsg map[string]interface{}
		if err := json.Unmarshal([]byte(msg), &blockMsg); err != nil {
			t.Fatalf("failed to parse block message: %v", err)
		}
	})

	// Note: With the new unified topic architecture, we only publish a single block event
	// that includes references to block/receipts/traces/blobs data in cache.
	// The separate receipts/traces topics are no longer used.

	t.Run("block_state_saved", func(t *testing.T) {
		// Poll for block state with timeout instead of static sleep
		var state *outbound.BlockState
		var err error
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			state, err = infra.BlockStateRepo.GetBlockByNumber(ctx, 100)
			if err != nil {
				t.Fatalf("failed to get block state: %v", err)
			}
			if state != nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if state == nil {
			t.Fatal("expected block state to be saved")
		}
		expectedHash := fmt.Sprintf("0x%064x", 100)
		if state.Hash != expectedHash {
			t.Errorf("expected hash %s, got %s", expectedHash, state.Hash)
		}
	})
}

// TestEndToEnd_MultipleBlocksInSequence tests processing multiple blocks in sequence.
func TestEndToEnd_MultipleBlocksInSequence(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupTestInfrastructure(t, ctx)
	t.Cleanup(infra.Cleanup)

	mockSub := testutil.NewMockSubscriber()
	mockClient := newTestBlockchainClient()

	config := live_data.LiveConfig{
		ChainID:            1,
		FinalityBlockCount: 3,
		EnableBlobs:        false,
		Logger:             slog.Default(),
	}

	liveService, err := live_data.NewLiveService(
		config,
		mockSub,
		mockClient,
		infra.BlockStateRepo,
		infra.Cache,
		infra.EventSink,
	)
	if err != nil {
		t.Fatalf("failed to create live service: %v", err)
	}

	if err := liveService.Start(ctx); err != nil {
		t.Fatalf("failed to start live service: %v", err)
	}
	defer liveService.Stop()

	// Process 10 blocks
	const numBlocks = 10
	for i := int64(100); i < 100+numBlocks; i++ {
		parentHash := fmt.Sprintf("0x%064x", i-1)
		mockClient.AddBlock(i, parentHash)
		header := mockClient.GetHeader(i)
		mockSub.SendHeader(header)
		time.Sleep(50 * time.Millisecond)
	}

	// Poll until all blocks are saved (or timeout)
	deadline := time.Now().Add(10 * time.Second)
	allSaved := false
	for time.Now().Before(deadline) {
		allSaved = true
		for i := int64(100); i < 100+numBlocks; i++ {
			state, _ := infra.BlockStateRepo.GetBlockByNumber(ctx, i)
			if state == nil {
				allSaved = false
				break
			}
		}
		if allSaved {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify all blocks are saved
	for i := int64(100); i < 100+numBlocks; i++ {
		state, err := infra.BlockStateRepo.GetBlockByNumber(ctx, i)
		if err != nil {
			t.Errorf("failed to get block %d: %v", i, err)
			continue
		}
		if state == nil {
			t.Errorf("block %d not found in database", i)
			continue
		}
		expectedHash := fmt.Sprintf("0x%064x", i)
		if state.Hash != expectedHash {
			t.Errorf("block %d: expected hash %s, got %s", i, expectedHash, state.Hash)
		}
	}

	// Verify last block
	lastBlock, err := infra.BlockStateRepo.GetLastBlock(ctx)
	if err != nil {
		t.Fatalf("failed to get last block: %v", err)
	}
	if lastBlock == nil {
		t.Fatal("expected last block to exist")
	}
	if lastBlock.Number != 109 {
		t.Errorf("expected last block number 109, got %d", lastBlock.Number)
	}
}

// =============================================================================
// Test Infrastructure
// =============================================================================

// TestInfrastructure holds all test dependencies.
type TestInfrastructure struct {
	Pool           *pgxpool.Pool
	BlockStateRepo *postgres.BlockStateRepository
	Cache          *rediscache.BlockCache
	EventSink      *snsadapter.EventSink
	SNSClient      *sns.Client
	SQSClient      *sqs.Client

	// Queue URLs for reading messages
	BlocksQueueURL   string
	ReceiptsQueueURL string
	TracesQueueURL   string
	BlobsQueueURL    string

	containers []testcontainers.Container
	Cleanup    func()
}

func setupTestInfrastructure(t *testing.T, ctx context.Context) *TestInfrastructure {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	infra := &TestInfrastructure{}
	var cleanupFuncs []func()

	// Start PostgreSQL
	postgresContainer, postgresCfg := startPostgres(t, ctx)
	infra.containers = append(infra.containers, postgresContainer)
	cleanupFuncs = append(cleanupFuncs, func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			logger.Error("failed to terminate postgres container", "error", err)
		}
	})

	pool, err := pgxpool.New(ctx, postgresCfg.ConnectionString())
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	cleanupFuncs = append(cleanupFuncs, func() {
		pool.Close()
	})
	infra.Pool = pool

	// Run migrations
	_, currentFile, _, _ := runtime.Caller(0)
	migrationsDir := filepath.Join(filepath.Dir(currentFile), "../../db/migrations")
	m := migrator.New(pool, migrationsDir)
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	blockStateRepo := postgres.NewBlockStateRepository(pool, logger)
	infra.BlockStateRepo = blockStateRepo

	// Start Redis
	redisContainer, redisCfg := startRedis(t, ctx)
	infra.containers = append(infra.containers, redisContainer)
	cleanupFuncs = append(cleanupFuncs, func() {
		if err := redisContainer.Terminate(ctx); err != nil {
			logger.Error("failed to terminate redis container", "error", err)
		}
	})

	cache, err := rediscache.NewBlockCache(rediscache.Config{
		Addr:      redisCfg.Addr,
		Password:  redisCfg.Password,
		DB:        redisCfg.DB,
		TTL:       1 * time.Hour,
		KeyPrefix: "test",
	}, logger)
	if err != nil {
		t.Fatalf("failed to create redis cache: %v", err)
	}
	cleanupFuncs = append(cleanupFuncs, func() {
		if err := cache.Close(); err != nil {
			logger.Error("failed to close redis cache", "error", err)
		}
	})
	infra.Cache = cache

	// Start LocalStack
	localstackContainer, localstackCfg := startLocalStack(t, ctx)
	infra.containers = append(infra.containers, localstackContainer)
	cleanupFuncs = append(cleanupFuncs, func() {
		if err := localstackContainer.Terminate(ctx); err != nil {
			logger.Error("failed to terminate localstack container", "error", err)
		}
	})

	// Create AWS clients
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(localstackCfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	snsClient := sns.NewFromConfig(awsCfg, func(o *sns.Options) {
		o.BaseEndpoint = aws.String(localstackCfg.Endpoint)
	})
	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(localstackCfg.Endpoint)
	})
	infra.SNSClient = snsClient
	infra.SQSClient = sqsClient

	// Create SNS topics and SQS queues
	topics := createSNSTopics(t, ctx, snsClient)
	queues := createSQSQueues(t, ctx, sqsClient)
	subscribeQueuesToTopics(t, ctx, snsClient, topics, queues)

	infra.BlocksQueueURL = queues["blocks"]
	infra.ReceiptsQueueURL = queues["receipts"]
	infra.TracesQueueURL = queues["traces"]
	infra.BlobsQueueURL = queues["blobs"]

	// Create event sink
	eventSink, err := snsadapter.NewEventSink(snsClient, snsadapter.Config{
		TopicARN: topics["blocks"],
		Logger:   logger,
	})
	if err != nil {
		t.Fatalf("failed to create event sink: %v", err)
	}
	cleanupFuncs = append(cleanupFuncs, func() {
		if err := eventSink.Close(); err != nil {
			logger.Error("failed to close event sink", "error", err)
		}
	})
	infra.EventSink = eventSink

	infra.Cleanup = func() {
		for i := len(cleanupFuncs) - 1; i >= 0; i-- {
			cleanupFuncs[i]()
		}
	}

	return infra
}

// WaitForMessage waits for a message on the given SQS queue.
func (infra *TestInfrastructure) WaitForMessage(t *testing.T, queueURL string, timeout time.Duration) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return ""
		default:
			result, err := infra.SQSClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(queueURL),
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     1,
			})
			if err != nil {
				t.Logf("error receiving message: %v", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			if len(result.Messages) > 0 {
				// Delete the message
				infra.SQSClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      aws.String(queueURL),
					ReceiptHandle: result.Messages[0].ReceiptHandle,
				})
				return aws.ToString(result.Messages[0].Body)
			}
		}
	}
}

// =============================================================================
// Container Setup
// =============================================================================

// PostgresTestConfig contains all configuration needed to connect to test Postgres.
type PostgresTestConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
	SSLMode  string
}

// ConnectionString returns the full postgres connection URL.
func (c PostgresTestConfig) ConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Database, c.SSLMode)
}

func startPostgres(t *testing.T, ctx context.Context) (testcontainers.Container, PostgresTestConfig) {
	t.Helper()

	config := PostgresTestConfig{
		User:     "test",
		Password: "test",
		Database: "testdb",
		SSLMode:  "disable",
	}

	req := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb:latest-pg17",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     config.User,
			"POSTGRES_PASSWORD": config.Password,
			"POSTGRES_DB":       config.Database,
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start postgres: %v", err)
	}

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "5432")
	config.Host = host
	config.Port = port.Port()

	return container, config
}

// RedisTestConfig contains all configuration needed to connect to test Redis.
type RedisTestConfig struct {
	Addr     string
	Password string
	DB       int
}

func startRedis(t *testing.T, ctx context.Context) (testcontainers.Container, RedisTestConfig) {
	t.Helper()

	config := RedisTestConfig{
		Password: "",
		DB:       0,
	}

	req := testcontainers.ContainerRequest{
		Image:        "redis:8.0-M04-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start redis: %v", err)
	}

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "6379")
	config.Addr = fmt.Sprintf("%s:%s", host, port.Port())

	return container, config
}

// LocalStackTestConfig contains all configuration needed to connect to test LocalStack.
type LocalStackTestConfig struct {
	Endpoint string
	Region   string
}

func startLocalStack(t *testing.T, ctx context.Context) (testcontainers.Container, LocalStackTestConfig) {
	t.Helper()

	config := LocalStackTestConfig{
		Region: "us-east-1",
	}

	req := testcontainers.ContainerRequest{
		Image:        "localstack/localstack:latest",
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"SERVICES": "sns,sqs",
			"DEBUG":    "0",
		},
		WaitingFor: wait.ForHTTP("/_localstack/health").
			WithPort("4566/tcp").
			WithStartupTimeout(120 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start localstack: %v", err)
	}

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "4566")
	config.Endpoint = fmt.Sprintf("http://%s:%s", host, port.Port())

	return container, config
}

func createSNSTopics(t *testing.T, ctx context.Context, client *sns.Client) map[string]string {
	t.Helper()
	topics := make(map[string]string)

	for _, name := range []string{"blocks", "receipts", "traces", "blobs"} {
		result, err := client.CreateTopic(ctx, &sns.CreateTopicInput{
			Name: aws.String(fmt.Sprintf("test-%s.fifo", name)),
			Attributes: map[string]string{
				"FifoTopic":                 "true",
				"ContentBasedDeduplication": "false",
			},
		})
		if err != nil {
			t.Fatalf("failed to create topic %s: %v", name, err)
		}
		topics[name] = *result.TopicArn
	}

	return topics
}

func createSQSQueues(t *testing.T, ctx context.Context, client *sqs.Client) map[string]string {
	t.Helper()
	queues := make(map[string]string)

	for _, name := range []string{"blocks", "receipts", "traces", "blobs"} {
		result, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName: aws.String(fmt.Sprintf("test-%s-queue.fifo", name)),
			Attributes: map[string]string{
				string(sqstypes.QueueAttributeNameVisibilityTimeout): "30",
				string(sqstypes.QueueAttributeNameFifoQueue):         "true",
			},
		})
		if err != nil {
			t.Fatalf("failed to create queue %s: %v", name, err)
		}
		queues[name] = *result.QueueUrl
	}

	return queues
}

func subscribeQueuesToTopics(t *testing.T, ctx context.Context, snsClient *sns.Client, topics, queues map[string]string) {
	t.Helper()

	for name, topicARN := range topics {
		queueURL := queues[name]
		// Get queue ARN from URL (LocalStack pattern for FIFO queues)
		queueARN := fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:test-%s-queue.fifo", name)

		_, err := snsClient.Subscribe(ctx, &sns.SubscribeInput{
			TopicArn: aws.String(topicARN),
			Protocol: aws.String("sqs"),
			Endpoint: aws.String(queueARN),
			Attributes: map[string]string{
				"RawMessageDelivery": "true",
			},
		})
		if err != nil {
			t.Fatalf("failed to subscribe queue to topic %s: %v", name, err)
		}
		t.Logf("Subscribed queue %s to topic %s", queueURL, topicARN)
	}
}

// =============================================================================
// Helpers
// =============================================================================

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
