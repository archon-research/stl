// Package main provides the cex-feed-watcher entry point.
//
// One process subscribes to one CEX's WebSocket feed (selected by CEX_NAME)
// and publishes raw frames to an SNS Standard topic. Indexer workers consume
// from an SQS queue subscribed to the topic, parse, and persist.
//
// Deployment model: one pod per exchange. Same binary, different CEX_NAME.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cex"
	snsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sns"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/services/cex_feed_watcher"
)

var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

func main() {
	showVersion := flag.Bool("version", false, "Show version information and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("stl-cex-feed-watcher\n")
		fmt.Printf("  Commit:     %s\n", GitCommit)
		fmt.Printf("  Branch:     %s\n", GitBranch)
		fmt.Printf("  Build Time: %s\n", BuildTime)
		os.Exit(0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	cexName := requireEnv("CEX_NAME")
	logger = logger.With("source", cexName)

	logger.Info("starting stl-cex-feed-watcher",
		"commit", GitCommit,
		"branch", GitBranch,
		"buildTime", BuildTime,
	)

	shutdownOTEL, err := telemetry.InitOTEL(ctx, telemetry.OTELConfig{
		ServiceName:    "stl-cex-feed-watcher",
		ServiceVersion: GitCommit,
		BuildTime:      BuildTime,
		Logger:         logger,
	})
	if err != nil {
		logger.Error("failed to initialize telemetry", "error", err)
		os.Exit(1)
	}
	defer shutdownOTEL(context.Background())

	wsConn, err := buildWSConnection(cexName, logger)
	if err != nil {
		logger.Error("failed to build WS connection", "error", err)
		os.Exit(1)
	}

	publisher, err := buildPublisher(ctx, logger)
	if err != nil {
		logger.Error("failed to build publisher", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			logger.Warn("publisher close error", "error", err)
		}
	}()

	service, err := cex_feed_watcher.NewService(
		cex_feed_watcher.Config{Source: cexName, Logger: logger},
		wsConn,
		publisher,
	)
	if err != nil {
		logger.Error("failed to create service", "error", err)
		os.Exit(1)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if err := service.Start(ctx); err != nil {
		logger.Error("failed to start service", "error", err)
		os.Exit(1)
	}
	logger.Info("service started, forwarding frames")

	sig := <-sigChan
	logger.Info("received signal, shutting down", "signal", sig)

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		if err := service.Stop(); err != nil {
			logger.Error("service stop error", "error", err)
		}
	}()
	select {
	case <-shutdownDone:
		logger.Info("shutdown complete")
	case <-shutdownCtx.Done():
		logger.Error("shutdown timed out, forcing exit")
		os.Exit(1)
	}
}

func buildWSConnection(cexName string, logger *slog.Logger) (*cex.WSConnection, error) {
	wsCfg, err := cex.BuildWSConnectionConfig(cexName)
	if err != nil {
		return nil, fmt.Errorf("build WS config: %w", err)
	}
	return cex.NewWSConnection(wsCfg, logger), nil
}

func buildPublisher(ctx context.Context, logger *slog.Logger) (*snsadapter.CEXFeedPublisher, error) {
	topicARN := requireEnv("AWS_SNS_CEX_FEED_TOPIC_ARN")
	snsEndpoint := env.Get("AWS_SNS_ENDPOINT", "")
	awsRegion := env.Get("AWS_REGION", "us-east-1")

	awsCfg, err := loadAWSConfig(ctx, awsRegion, logger)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	snsClient := sns.NewFromConfig(awsCfg, func(o *sns.Options) {
		if snsEndpoint != "" {
			o.BaseEndpoint = aws.String(snsEndpoint)
		}
	})
	return snsadapter.NewCEXFeedPublisher(snsClient, snsadapter.CEXFeedConfig{
		TopicARN: topicARN,
		Logger:   logger,
	})
}

// loadAWSConfig builds an AWS config that uses static credentials when
// AWS_ACCESS_KEY_ID is set (for LocalStack) and the default credential
// chain otherwise (for IAM role in EKS).
func loadAWSConfig(ctx context.Context, region string, logger *slog.Logger) (aws.Config, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if accessKey := os.Getenv("AWS_ACCESS_KEY_ID"); accessKey != "" {
		secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secret, ""),
		))
		logger.Debug("using static AWS credentials from environment")
	} else {
		logger.Debug("using default AWS credential chain")
	}
	return awsconfig.LoadDefaultConfig(ctx, opts...)
}

func requireEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		slog.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return value
}
