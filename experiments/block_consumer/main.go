// Package main provides a dummy consumer that reads block events from SQS
// and fetches the corresponding block data from Redis.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/redis/go-redis/v9"
)

// BlockEvent represents the event published by the watcher.
type BlockEvent struct {
	ChainID        int64  `json:"chainId"`
	BlockNumber    int64  `json:"blockNumber"`
	Version        int    `json:"version"`
	BlockHash      string `json:"blockHash"`
	ParentHash     string `json:"parentHash"`
	BlockTimestamp int64  `json:"blockTimestamp"`
	ReceivedAt     string `json:"receivedAt"`
	CacheKey       string `json:"cacheKey"`
	IsBackfill     bool   `json:"isBackfill"`
	IsReorg        bool   `json:"isReorg"`
}

// SNSMessage wraps the actual message from SNS->SQS.
type SNSMessage struct {
	Message string `json:"Message"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Configuration
	sqsEndpoint := getEnv("AWS_SQS_ENDPOINT", "http://172.19.0.5:4566")
	sqsQueueURL := getEnv("SQS_QUEUE_URL", "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/stl-ethereum-transformer.fifo")
	redisAddr := getEnv("REDIS_ADDR", "172.19.0.4:6379")

	// Set up AWS SDK for LocalStack
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		logger.Error("failed to load AWS config", "error", err)
		os.Exit(1)
	}

	// Create SQS client
	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(sqsEndpoint)
	})

	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer redisClient.Close()

	// Test Redis connection
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		logger.Error("failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to Redis", "addr", redisAddr)

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("shutting down...")
		cancel()
	}()

	logger.Info("starting block consumer", "queue", sqsQueueURL)

	// Poll for messages
	for {
		select {
		case <-ctx.Done():
			logger.Info("consumer stopped")
			return
		default:
		}

		// Receive messages from SQS
		result, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(sqsQueueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20, // Long polling
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Error("failed to receive messages", "error", err)
			time.Sleep(time.Second)
			continue
		}

		for _, msg := range result.Messages {
			// With RawMessageDelivery=true, the message body is the raw content
			// Try parsing as raw BlockEvent first, fall back to SNS wrapper
			var event BlockEvent
			if err := json.Unmarshal([]byte(*msg.Body), &event); err != nil {
				// Try SNS wrapper format
				var snsMsg SNSMessage
				if err := json.Unmarshal([]byte(*msg.Body), &snsMsg); err != nil {
					logger.Error("failed to parse message", "error", err, "body", *msg.Body)
					continue
				}
				if err := json.Unmarshal([]byte(snsMsg.Message), &event); err != nil {
					logger.Error("failed to parse block event from SNS wrapper", "error", err)
					continue
				}
			}

			logger.Info("received block event",
				"block", event.BlockNumber,
				"hash", truncate(event.BlockHash, 16),
				"version", event.Version,
				"cacheKey", event.CacheKey,
				"isBackfill", event.IsBackfill,
				"isReorg", event.IsReorg,
			)

			// Fetch block data from Redis using the cache key
			blockData, err := redisClient.Get(ctx, event.CacheKey).Result()
			if err == redis.Nil {
				logger.Warn("block not found in cache", "cacheKey", event.CacheKey)
			} else if err != nil {
				logger.Error("failed to fetch from Redis", "error", err, "cacheKey", event.CacheKey)
			} else {
				// Parse and display some block info
				var block map[string]interface{}
				if err := json.Unmarshal([]byte(blockData), &block); err != nil {
					logger.Error("failed to parse block data", "error", err)
				} else {
					txCount := 0
					if txs, ok := block["transactions"].([]interface{}); ok {
						txCount = len(txs)
					}
					logger.Info("fetched block from Redis",
						"block", event.BlockNumber,
						"transactions", txCount,
						"dataSize", fmt.Sprintf("%d bytes", len(blockData)),
					)
				}
			}

			// Delete message from queue
			_, err = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(sqsQueueURL),
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				logger.Error("failed to delete message", "error", err)
			}
		}
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
