//go:build integration

package rawdatabackup

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	rediscache "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	sqsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// =============================================================================
// Test Infrastructure
// =============================================================================

// IntegrationTestInfra holds all the infrastructure for integration tests.
type IntegrationTestInfra struct {
	// AWS clients
	SNSClient *sns.Client
	SQSClient *sqs.Client
	S3Client  *s3.Client

	// Queue URLs and ARNs
	BackupQueueURL string
	TopicARN       string

	// S3 bucket
	BucketName string

	// Redis cache
	Cache outbound.BlockCache

	// Service dependencies
	Consumer outbound.SQSConsumer
	Writer   outbound.S3Writer

	// Logger
	Logger *slog.Logger

	// Containers
	containers []testcontainers.Container
	Cleanup    func()
}

func setupIntegrationInfra(t *testing.T, ctx context.Context) *IntegrationTestInfra {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	infra := &IntegrationTestInfra{
		Logger: logger,
	}
	var cleanupFuncs []func()

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
		KeyPrefix: "integration-test",
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

	// Start LocalStack (with S3, SNS, SQS)
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
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(localstackCfg.Endpoint)
		o.UsePathStyle = true // Required for LocalStack
	})

	infra.SNSClient = snsClient
	infra.SQSClient = sqsClient
	infra.S3Client = s3Client

	// Create S3 bucket
	bucketName := "test-raw-data-backup"
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("failed to create S3 bucket: %v", err)
	}
	infra.BucketName = bucketName
	t.Logf("Created S3 bucket: %s", bucketName)

	// Create SNS topic
	topicResult, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("test-blocks"),
	})
	if err != nil {
		t.Fatalf("failed to create SNS topic: %v", err)
	}
	infra.TopicARN = *topicResult.TopicArn
	t.Logf("Created SNS topic: %s", infra.TopicARN)

	// Create SQS queue for backup
	queueResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("test-backup-queue"),
		Attributes: map[string]string{
			string(sqstypes.QueueAttributeNameVisibilityTimeout): "30",
		},
	})
	if err != nil {
		t.Fatalf("failed to create SQS queue: %v", err)
	}
	infra.BackupQueueURL = *queueResult.QueueUrl
	t.Logf("Created SQS queue: %s", infra.BackupQueueURL)

	// Get queue ARN for subscription
	queueAttrs, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(infra.BackupQueueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("failed to get queue ARN: %v", err)
	}
	queueARN := queueAttrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]

	// Subscribe queue to topic
	_, err = snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: aws.String(infra.TopicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
		Attributes: map[string]string{
			"RawMessageDelivery": "true",
		},
	})
	if err != nil {
		t.Fatalf("failed to subscribe queue to topic: %v", err)
	}
	t.Logf("Subscribed queue to topic")

	// Create SQS consumer adapter using the real adapter with LocalStack endpoint
	consumer, err := sqsadapter.NewConsumerWithOptions(awsCfg, sqsadapter.Config{
		QueueURL:          infra.BackupQueueURL,
		WaitTimeSeconds:   1, // Short for tests
		VisibilityTimeout: 30,
	}, logger, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(localstackCfg.Endpoint)
	})
	if err != nil {
		t.Fatalf("failed to create SQS consumer: %v", err)
	}
	infra.Consumer = consumer

	// Create S3 writer adapter using the real adapter with LocalStack endpoint
	infra.Writer = s3adapter.NewWriterWithOptions(awsCfg, logger, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(localstackCfg.Endpoint)
		o.UsePathStyle = true // Required for LocalStack
	})

	infra.Cleanup = func() {
		for i := len(cleanupFuncs) - 1; i >= 0; i-- {
			cleanupFuncs[i]()
		}
	}

	return infra
}

// =============================================================================
// Container Setup
// =============================================================================

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
			"SERVICES": "sns,sqs,s3",
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

// =============================================================================
// Helper Functions
// =============================================================================

func publishBlockEvent(t *testing.T, ctx context.Context, infra *IntegrationTestInfra, event outbound.BlockEvent) {
	t.Helper()

	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	_, err = infra.SNSClient.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(infra.TopicARN),
		Message:  aws.String(string(body)),
	})
	if err != nil {
		t.Fatalf("failed to publish to SNS: %v", err)
	}
	t.Logf("Published block event: block=%d, version=%d", event.BlockNumber, event.Version)
}

func getS3Object(t *testing.T, ctx context.Context, infra *IntegrationTestInfra, key string) []byte {
	t.Helper()

	result, err := infra.S3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(infra.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("failed to get S3 object %s: %v", key, err)
	}
	defer result.Body.Close()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("failed to read S3 object body: %v", err)
	}

	// Decompress if gzipped
	if result.ContentEncoding != nil && *result.ContentEncoding == "gzip" {
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			t.Fatalf("failed to create gzip reader: %v", err)
		}
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("failed to decompress: %v", err)
		}
		return decompressed
	}

	return body
}

func listS3Objects(t *testing.T, ctx context.Context, infra *IntegrationTestInfra, prefix string) []string {
	t.Helper()

	result, err := infra.S3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(infra.BucketName),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		t.Fatalf("failed to list S3 objects: %v", err)
	}

	keys := make([]string, 0, len(result.Contents))
	for _, obj := range result.Contents {
		keys = append(keys, *obj.Key)
	}
	return keys
}

func waitForQueueEmpty(t *testing.T, ctx context.Context, infra *IntegrationTestInfra, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		attrs, err := infra.SQSClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(infra.BackupQueueURL),
			AttributeNames: []sqstypes.QueueAttributeName{
				sqstypes.QueueAttributeNameApproximateNumberOfMessages,
				sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})
		if err != nil {
			t.Logf("failed to get queue attributes: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		visible := attrs.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessages)]
		notVisible := attrs.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible)]

		if visible == "0" && notVisible == "0" {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// =============================================================================
// Integration Tests
// =============================================================================

// TestIntegration_SingleBlockBackup tests backing up a single block's data.
func TestIntegration_SingleBlockBackup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	// Set up block data in Redis cache
	blockNumber := int64(12345)
	version := 0
	chainID := int64(1)

	blockData := json.RawMessage(`{"number":"0x3039","hash":"0xabcd","parentHash":"0x1234"}`)
	receiptsData := json.RawMessage(`[{"transactionHash":"0xdead","status":"0x1"}]`)
	tracesData := json.RawMessage(`[{"action":{"callType":"call","from":"0x1","to":"0x2"}}]`)
	blobsData := json.RawMessage(`[{"commitment":"0xbeef"}]`)

	err := infra.Cache.SetBlock(ctx, chainID, blockNumber, version, blockData)
	if err != nil {
		t.Fatalf("failed to set block in cache: %v", err)
	}
	err = infra.Cache.SetReceipts(ctx, chainID, blockNumber, version, receiptsData)
	if err != nil {
		t.Fatalf("failed to set receipts in cache: %v", err)
	}
	err = infra.Cache.SetTraces(ctx, chainID, blockNumber, version, tracesData)
	if err != nil {
		t.Fatalf("failed to set traces in cache: %v", err)
	}
	err = infra.Cache.SetBlobs(ctx, chainID, blockNumber, version, blobsData)
	if err != nil {
		t.Fatalf("failed to set blobs in cache: %v", err)
	}

	// Create and start the backup service
	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   2,
		BatchSize: 10,
		Logger:    infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Start service in background
	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	// Publish the block event
	event := outbound.BlockEvent{
		ChainID:        chainID,
		BlockNumber:    blockNumber,
		Version:        version,
		BlockHash:      "0xabcd",
		ParentHash:     "0x1234",
		BlockTimestamp: time.Now().Unix(),
		ReceivedAt:     time.Now(),
	}
	publishBlockEvent(t, ctx, infra, event)

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Stop service
	svc.Stop()
	svcCancel()
	<-done

	// Verify S3 objects were created
	partition := "12001-13000" // Block 12345 falls in this partition
	prefix := fmt.Sprintf("%d/%s/", chainID, partition)
	objects := listS3Objects(t, ctx, infra, prefix)

	t.Logf("S3 objects created: %v", objects)

	expectedFiles := []string{
		fmt.Sprintf("%d/%s/%d_%d_block.json.gz", chainID, partition, blockNumber, version),
		fmt.Sprintf("%d/%s/%d_%d_receipts.json.gz", chainID, partition, blockNumber, version),
		fmt.Sprintf("%d/%s/%d_%d_traces.json.gz", chainID, partition, blockNumber, version),
		fmt.Sprintf("%d/%s/%d_%d_blobs.json.gz", chainID, partition, blockNumber, version),
	}

	for _, expected := range expectedFiles {
		found := false
		for _, obj := range objects {
			if obj == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected file %s not found in S3", expected)
		}
	}

	// Verify content of block file
	blockKey := fmt.Sprintf("%d/%s/%d_%d_block.json.gz", chainID, partition, blockNumber, version)
	content := getS3Object(t, ctx, infra, blockKey)
	t.Logf("Block content: %s", string(content))

	var savedBlock map[string]interface{}
	if err := json.Unmarshal(content, &savedBlock); err != nil {
		t.Fatalf("failed to parse saved block: %v", err)
	}
	if savedBlock["hash"] != "0xabcd" {
		t.Errorf("expected hash 0xabcd, got %v", savedBlock["hash"])
	}
}

// TestIntegration_MultipleBlocksProcessedConcurrently tests processing multiple blocks.
func TestIntegration_MultipleBlocksProcessedConcurrently(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	chainID := int64(1)
	numBlocks := 10

	// Set up block data for multiple blocks
	for i := 0; i < numBlocks; i++ {
		blockNumber := int64(100 + i)
		blockData := json.RawMessage(fmt.Sprintf(`{"number":"0x%x","hash":"0x%064x"}`, blockNumber, blockNumber))
		err := infra.Cache.SetBlock(ctx, chainID, blockNumber, 0, blockData)
		if err != nil {
			t.Fatalf("failed to set block %d in cache: %v", blockNumber, err)
		}
	}

	// Create service with multiple workers
	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   4,
		BatchSize: 10,
		Logger:    infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	// Publish all block events
	for i := 0; i < numBlocks; i++ {
		blockNumber := int64(100 + i)
		event := outbound.BlockEvent{
			ChainID:        chainID,
			BlockNumber:    blockNumber,
			Version:        0,
			BlockHash:      fmt.Sprintf("0x%064x", blockNumber),
			BlockTimestamp: time.Now().Unix(),
			ReceivedAt:     time.Now(),
		}
		publishBlockEvent(t, ctx, infra, event)
	}

	// Wait for processing
	time.Sleep(5 * time.Second)

	svc.Stop()
	svcCancel()
	<-done

	// Verify all blocks were backed up
	// Blocks 100-109 all fall in partition 0-1000
	prefix := fmt.Sprintf("%d/0-1000/", chainID)
	objects := listS3Objects(t, ctx, infra, prefix)

	t.Logf("Created %d S3 objects", len(objects))

	if len(objects) != numBlocks {
		t.Errorf("expected %d S3 objects, got %d", numBlocks, len(objects))
	}
}

// TestIntegration_IdempotentWrites tests that duplicate messages don't create duplicate files.
func TestIntegration_IdempotentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	chainID := int64(1)
	blockNumber := int64(500)

	// Set up block data
	blockData := json.RawMessage(`{"number":"0x1f4","hash":"0xfirst"}`)
	err := infra.Cache.SetBlock(ctx, chainID, blockNumber, 0, blockData)
	if err != nil {
		t.Fatalf("failed to set block in cache: %v", err)
	}

	// Create service
	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   1,
		BatchSize: 10,
		Logger:    infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	// Publish same event twice
	event := outbound.BlockEvent{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Version:     0,
		BlockHash:   "0xfirst",
	}
	publishBlockEvent(t, ctx, infra, event)
	time.Sleep(2 * time.Second)

	// Update cache with different data (simulating reprocessing scenario)
	blockData2 := json.RawMessage(`{"number":"0x1f4","hash":"0xsecond"}`)
	_ = infra.Cache.SetBlock(ctx, chainID, blockNumber, 0, blockData2)

	// Publish again
	publishBlockEvent(t, ctx, infra, event)
	time.Sleep(2 * time.Second)

	svc.Stop()
	svcCancel()
	<-done

	// Verify only one file exists and has original content
	key := fmt.Sprintf("%d/0-1000/%d_0_block.json.gz", chainID, blockNumber)
	content := getS3Object(t, ctx, infra, key)

	var savedBlock map[string]interface{}
	if err := json.Unmarshal(content, &savedBlock); err != nil {
		t.Fatalf("failed to parse saved block: %v", err)
	}

	// Should have original content since idempotency check prevented overwrite
	if savedBlock["hash"] != "0xfirst" {
		t.Errorf("expected original hash 0xfirst, got %v (idempotency failed)", savedBlock["hash"])
	}
}

// TestIntegration_DifferentVersionsStored tests that reorg versions are stored separately.
func TestIntegration_DifferentVersionsStored(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	chainID := int64(1)
	blockNumber := int64(1500) // Partition 1001-2000

	// Set up version 0
	blockData0 := json.RawMessage(`{"number":"0x5dc","version":0}`)
	err := infra.Cache.SetBlock(ctx, chainID, blockNumber, 0, blockData0)
	if err != nil {
		t.Fatalf("failed to set block v0 in cache: %v", err)
	}

	// Set up version 1 (reorg)
	blockData1 := json.RawMessage(`{"number":"0x5dc","version":1}`)
	err = infra.Cache.SetBlock(ctx, chainID, blockNumber, 1, blockData1)
	if err != nil {
		t.Fatalf("failed to set block v1 in cache: %v", err)
	}

	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   2,
		BatchSize: 10,
		Logger:    infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	// Publish version 0
	event0 := outbound.BlockEvent{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Version:     0,
		BlockHash:   "0xv0",
	}
	publishBlockEvent(t, ctx, infra, event0)
	time.Sleep(2 * time.Second)

	// Publish version 1
	event1 := outbound.BlockEvent{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Version:     1,
		BlockHash:   "0xv1",
	}
	publishBlockEvent(t, ctx, infra, event1)
	time.Sleep(2 * time.Second)

	svc.Stop()
	svcCancel()
	<-done

	// Verify both versions exist
	prefix := fmt.Sprintf("%d/1001-2000/", chainID)
	objects := listS3Objects(t, ctx, infra, prefix)
	t.Logf("S3 objects: %v", objects)

	expectedV0 := fmt.Sprintf("%d/1001-2000/%d_0_block.json.gz", chainID, blockNumber)
	expectedV1 := fmt.Sprintf("%d/1001-2000/%d_1_block.json.gz", chainID, blockNumber)

	foundV0, foundV1 := false, false
	for _, obj := range objects {
		if obj == expectedV0 {
			foundV0 = true
		}
		if obj == expectedV1 {
			foundV1 = true
		}
	}

	if !foundV0 {
		t.Error("version 0 file not found")
	}
	if !foundV1 {
		t.Error("version 1 file not found")
	}

	// Verify content
	content0 := getS3Object(t, ctx, infra, expectedV0)
	content1 := getS3Object(t, ctx, infra, expectedV1)

	var block0, block1 map[string]interface{}
	json.Unmarshal(content0, &block0)
	json.Unmarshal(content1, &block1)

	if block0["version"] != float64(0) {
		t.Errorf("v0 content wrong: %v", block0)
	}
	if block1["version"] != float64(1) {
		t.Errorf("v1 content wrong: %v", block1)
	}
}

// TestIntegration_LargeBlockData tests handling of large block data.
func TestIntegration_LargeBlockData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	chainID := int64(1)
	blockNumber := int64(2001) // Partition 2001-3000

	// Create large block data (~1MB)
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = 'x'
	}
	blockData := json.RawMessage(fmt.Sprintf(`{"number":"0x7d1","data":"%s"}`, string(largeData)))

	err := infra.Cache.SetBlock(ctx, chainID, blockNumber, 0, blockData)
	if err != nil {
		t.Fatalf("failed to set large block in cache: %v", err)
	}

	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   1,
		BatchSize: 10,
		Logger:    infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	event := outbound.BlockEvent{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Version:     0,
		BlockHash:   "0xlarge",
	}
	publishBlockEvent(t, ctx, infra, event)
	time.Sleep(5 * time.Second)

	svc.Stop()
	svcCancel()
	<-done

	// Verify file exists and content is correct
	key := fmt.Sprintf("%d/2001-3000/%d_0_block.json.gz", chainID, blockNumber)
	content := getS3Object(t, ctx, infra, key)

	if len(content) < 1024*1024 {
		t.Errorf("expected large content (>1MB), got %d bytes", len(content))
	}
}

// TestIntegration_GracefulShutdown tests that the service shuts down gracefully.
func TestIntegration_GracefulShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	chainID := int64(1)

	// Set up some blocks
	for i := 0; i < 5; i++ {
		blockNumber := int64(3001 + i)
		blockData := json.RawMessage(fmt.Sprintf(`{"number":"0x%x"}`, blockNumber))
		_ = infra.Cache.SetBlock(ctx, chainID, blockNumber, 0, blockData)
	}

	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   2,
		BatchSize: 10,
		Logger:    infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	// Publish events
	for i := 0; i < 5; i++ {
		blockNumber := int64(3001 + i)
		event := outbound.BlockEvent{
			ChainID:     chainID,
			BlockNumber: blockNumber,
			Version:     0,
		}
		publishBlockEvent(t, ctx, infra, event)
	}

	// Wait a bit for processing to start
	time.Sleep(1 * time.Second)

	// Stop gracefully using the Stop() method (not context cancellation)
	svc.Stop()

	select {
	case err := <-done:
		// Either nil or context.Canceled is acceptable for graceful shutdown
		if err != nil && err != context.Canceled {
			t.Errorf("unexpected error on shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("service did not shut down within timeout")
	}

	// Clean up context
	svcCancel()

	t.Log("Service shut down gracefully")
}
