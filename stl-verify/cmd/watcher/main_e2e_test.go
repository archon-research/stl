//go:build e2e

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	rediscache "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	snsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sns"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/live_data"
)

// =============================================================================
// Mock Implementations (must be declared before tests to avoid compilation issues)
// =============================================================================

// mockSubscriber simulates block notifications from Alchemy WebSocket.
type mockSubscriber struct {
	headers chan outbound.BlockHeader
	closed  bool
	mu      sync.Mutex
}

func newMockSubscriber() *mockSubscriber {
	return &mockSubscriber{
		headers: make(chan outbound.BlockHeader, 100),
	}
}

func (m *mockSubscriber) Subscribe(ctx context.Context) (<-chan outbound.BlockHeader, error) {
	return m.headers, nil
}

func (m *mockSubscriber) Unsubscribe() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.headers)
	}
	return nil
}

func (m *mockSubscriber) HealthCheck(ctx context.Context) error {
	return nil
}

func (m *mockSubscriber) sendHeader(header outbound.BlockHeader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.headers <- header
	}
}

// mockBlockchainClient provides mock block data for tests.
type mockBlockchainClient struct {
	blocks map[int64]blockTestData
	mu     sync.RWMutex
}

type blockTestData struct {
	header   outbound.BlockHeader
	receipts json.RawMessage
	traces   json.RawMessage
	blobs    json.RawMessage
}

func newMockBlockchainClient() *mockBlockchainClient {
	return &mockBlockchainClient{
		blocks: make(map[int64]blockTestData),
	}
}

func (m *mockBlockchainClient) addBlock(num int64, parentHash string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	hash := fmt.Sprintf("0x%064x", num)
	if parentHash == "" && num > 0 {
		parentHash = fmt.Sprintf("0x%064x", num-1)
	}

	header := outbound.BlockHeader{
		Number:     fmt.Sprintf("0x%x", num),
		Hash:       hash,
		ParentHash: parentHash,
		Timestamp:  fmt.Sprintf("0x%x", time.Now().Unix()),
	}

	m.blocks[num] = blockTestData{
		header:   header,
		receipts: json.RawMessage(`[]`),
		traces:   json.RawMessage(`[]`),
		blobs:    json.RawMessage(`[]`),
	}
}

func (m *mockBlockchainClient) getHeader(num int64) outbound.BlockHeader {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.blocks[num].header
}

func (m *mockBlockchainClient) GetBlockByNumber(ctx context.Context, blockNum int64, fullTx bool) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		data, _ := json.Marshal(bd.header)
		return data, nil
	}
	return nil, fmt.Errorf("block %d not found", blockNum)
}

func (m *mockBlockchainClient) GetBlockByHash(ctx context.Context, hash string, fullTx bool) (*outbound.BlockHeader, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if bd.header.Hash == hash {
			h := bd.header
			return &h, nil
		}
	}
	return nil, fmt.Errorf("block %s not found", hash)
}

func (m *mockBlockchainClient) GetFullBlockByHash(ctx context.Context, hash string, fullTx bool) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if bd.header.Hash == hash {
			data, _ := json.Marshal(bd.header)
			return data, nil
		}
	}
	return nil, fmt.Errorf("block %s not found", hash)
}

func (m *mockBlockchainClient) GetBlockReceipts(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.receipts, nil
	}
	return nil, fmt.Errorf("receipts for block %d not found", blockNum)
}

func (m *mockBlockchainClient) GetBlockReceiptsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if bd.header.Hash == hash {
			return bd.receipts, nil
		}
	}
	return nil, fmt.Errorf("receipts for block %s not found", hash)
}

func (m *mockBlockchainClient) GetBlockTraces(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.traces, nil
	}
	return nil, fmt.Errorf("traces for block %d not found", blockNum)
}

func (m *mockBlockchainClient) GetBlockTracesByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if bd.header.Hash == hash {
			return bd.traces, nil
		}
	}
	return nil, fmt.Errorf("traces for block %s not found", hash)
}

func (m *mockBlockchainClient) GetBlobSidecars(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.blobs, nil
	}
	return nil, fmt.Errorf("blobs for block %d not found", blockNum)
}

func (m *mockBlockchainClient) GetBlobSidecarsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if bd.header.Hash == hash {
			return bd.blobs, nil
		}
	}
	return nil, fmt.Errorf("blobs for block %s not found", hash)
}

func (m *mockBlockchainClient) GetCurrentBlockNumber(ctx context.Context) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var max int64
	for num := range m.blocks {
		if num > max {
			max = num
		}
	}
	return max, nil
}

func (m *mockBlockchainClient) GetBlocksBatch(ctx context.Context, blockNums []int64, fullTx bool) ([]outbound.BlockData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	results := make([]outbound.BlockData, len(blockNums))
	for i, num := range blockNums {
		if bd, ok := m.blocks[num]; ok {
			data, _ := json.Marshal(bd.header)
			results[i] = outbound.BlockData{
				BlockNumber: num,
				Block:       data,
				Receipts:    bd.receipts,
				Traces:      bd.traces,
				Blobs:       bd.blobs,
			}
		} else {
			results[i] = outbound.BlockData{
				BlockNumber: num,
				BlockErr:    fmt.Errorf("block %d not found", num),
			}
		}
	}
	return results, nil
}

func (m *mockBlockchainClient) GetBlockDataByHash(ctx context.Context, blockNum int64, hash string, fullTx bool) (outbound.BlockData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if bd.header.Hash == hash {
			data, _ := json.Marshal(bd.header)
			return outbound.BlockData{
				BlockNumber: blockNum,
				Block:       data,
				Receipts:    bd.receipts,
				Traces:      bd.traces,
				Blobs:       bd.blobs,
			}, nil
		}
	}
	return outbound.BlockData{}, fmt.Errorf("block %s not found", hash)
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
	mockSub := newMockSubscriber()

	// Create mock blockchain client
	mockClient := newMockBlockchainClient()

	// Create live service
	config := live_data.LiveConfig{
		ChainID:              1,
		FinalityBlockCount:   2, // Use small finality for faster tests
		MaxUnfinalizedBlocks: 10,
		EnableBlobs:          false,
		Logger:               slog.Default(),
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
	mockClient.addBlock(100, "0x"+fmt.Sprintf("%064x", 99))

	// Emit block notification via header
	header := mockClient.getHeader(100)
	mockSub.sendHeader(header)

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

	mockSub := newMockSubscriber()
	mockClient := newMockBlockchainClient()

	config := live_data.LiveConfig{
		ChainID:              1,
		FinalityBlockCount:   3,
		MaxUnfinalizedBlocks: 10,
		EnableBlobs:          false,
		Logger:               slog.Default(),
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
		mockClient.addBlock(i, parentHash)
		header := mockClient.getHeader(i)
		mockSub.sendHeader(header)
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
	DB             *sql.DB
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

	db, err := sql.Open("pgx", postgresCfg.ConnectionString())
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	cleanupFuncs = append(cleanupFuncs, func() {
		if err := db.Close(); err != nil {
			logger.Error("failed to close database connection", "error", err)
		}
	})
	infra.DB = db

	blockStateRepo := postgres.NewBlockStateRepository(db, logger)
	if err := blockStateRepo.Migrate(ctx); err != nil {
		t.Fatalf("failed to migrate: %v", err)
	}
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
		Image:        "postgres:18",
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
