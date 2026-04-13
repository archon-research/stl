// Command mock-blockchain-server starts an in-memory mock Ethereum JSON-RPC server for stress testing.
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

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/testutil/mockchain"
)

func main() {
	addr := flag.String("addr", ":8546", "listen address (e.g. :8546)")
	adminAddr := flag.String("admin-addr", "localhost:8547", "admin API listen address (empty to disable; use :8547 to bind all interfaces in k8s)")
	interval := flag.Duration("interval", 12*time.Second, "block emission interval (e.g. 1s, 500ms)")
	s3Bucket := flag.String("s3-bucket", "", "S3 bucket containing block data (empty = use fixture data)")
	s3Prefix := flag.String("s3-prefix", "blocks/chain-1", "S3 key prefix for block data")
	s3Endpoint := flag.String("s3-endpoint", "", "S3 endpoint override for LocalStack (e.g. http://localstack:4566)")
	region := flag.String("region", "eu-west-1", "AWS region")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serverCfg := serverConfig{
		addr:       *addr,
		adminAddr:  *adminAddr,
		interval:   *interval,
		s3Bucket:   *s3Bucket,
		s3Prefix:   *s3Prefix,
		s3Endpoint: *s3Endpoint,
		region:     *region,
	}

	if err := run(ctx, logger, serverCfg); err != nil {
		logger.Error("mock-blockchain-server failed", "error", err)
		os.Exit(1)
	}
}

// serverConfig holds configuration parsed from CLI flags.
type serverConfig struct {
	addr       string
	adminAddr  string
	interval   time.Duration
	s3Bucket   string
	s3Prefix   string
	s3Endpoint string
	region     string
}

// serverAdapter adapts mockchain.Server to the lifecycle.Service interface.
type serverAdapter struct {
	srv       *mockchain.Server
	store     *mockchain.DataStore
	adminAddr string
	addr      string
	logger    *slog.Logger
}

func (a *serverAdapter) Start(_ context.Context) error {
	if err := a.srv.Start(a.addr); err != nil {
		return fmt.Errorf("starting server: %w", err)
	}

	a.logger.Info("mock blockchain server ready",
		"addr", a.srv.Addr(),
		"ws", "ws://"+a.srv.Addr().String(),
		"http", "http://"+a.srv.Addr().String(),
		"blocks", a.store.Len(),
	)

	if a.adminAddr != "" {
		if err := a.srv.StartAdmin(a.adminAddr); err != nil {
			return fmt.Errorf("starting admin server: %w", err)
		}
		a.logger.Info("admin API ready", "addr", a.adminAddr)
	}

	return nil
}

func (a *serverAdapter) Stop() error {
	return a.srv.Stop()
}

func run(ctx context.Context, logger *slog.Logger, c serverConfig) error {
	if c.interval <= 0 {
		return fmt.Errorf("interval must be positive, got %v", c.interval)
	}

	store, err := loadStore(ctx, logger, c)
	if err != nil {
		return fmt.Errorf("loading block data: %w", err)
	}

	srv := mockchain.NewServer(store, c.interval)

	return lifecycle.Run(ctx, logger, &serverAdapter{
		srv:       srv,
		store:     store,
		adminAddr: c.adminAddr,
		addr:      c.addr,
		logger:    logger,
	})
}

// loadStore returns a DataStore populated from S3 when --s3-bucket is set,
// or from synthetic fixture data otherwise.
func loadStore(ctx context.Context, logger *slog.Logger, c serverConfig) (*mockchain.DataStore, error) {
	if c.s3Bucket == "" {
		logger.Info("using fixture block data (no --s3-bucket set)")
		return mockchain.NewFixtureDataStore(), nil
	}

	logger.Info("loading block data from S3",
		"bucket", c.s3Bucket,
		"prefix", c.s3Prefix,
		"localstack", c.s3Endpoint != "",
	)

	var optFns []func(*awss3.Options)
	awsCfgOpts := []func(*config.LoadOptions) error{
		config.WithRegion(c.region),
	}
	if c.s3Endpoint != "" {
		awsCfgOpts = append(awsCfgOpts,
			config.WithBaseEndpoint(c.s3Endpoint),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		)
		optFns = append(optFns, func(o *awss3.Options) { o.UsePathStyle = true })
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, awsCfgOpts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	reader := s3adapter.NewReaderWithOptions(awsCfg, logger, optFns...)

	store := mockchain.NewDataStore()
	if err := store.LoadFromS3(ctx, reader, c.s3Bucket, c.s3Prefix); err != nil {
		logger.Warn("failed to load from S3, falling back to fixture data", "bucket", c.s3Bucket, "error", err)
		return mockchain.NewFixtureDataStore(), nil
	}

	if store.Len() == 0 {
		logger.Warn("S3 bucket is empty, falling back to fixture data", "bucket", c.s3Bucket)
		return mockchain.NewFixtureDataStore(), nil
	}

	logger.Info("block data loaded from S3", "blocks", store.Len())
	return store, nil
}
