package main

import (
	"flag"
	"fmt"

	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
)

// Config holds the CLI configuration.
type Config struct {
	ChainID    int64
	RPCURL     string
	StartBlock int64
	EndBlock   int64
	Bucket     string
	Region     string
	DryRun     bool
	ReportPath string

	AllowUnfinalized bool

	BlockReceiptWorkers int
	TraceWorkers        int
	UploadWorkers       int
	BlockBatchSize      int
	TraceBatchSize      int
}

func parseFlags() Config {
	var cfg Config

	flag.Int64Var(&cfg.ChainID, "chain-id", 0, "Chain the archive belongs to (required)")
	flag.StringVar(&cfg.RPCURL, "rpc-url", "http://localhost:8545", "RPC endpoint URL")
	flag.Int64Var(&cfg.StartBlock, "start-block", 0, "Starting block number (required)")
	flag.Int64Var(&cfg.EndBlock, "end-block", 0, "Ending block number (required)")
	flag.StringVar(&cfg.Bucket, "bucket", "", "S3 bucket name (required)")
	flag.StringVar(&cfg.Region, "region", "", "AWS region (e.g., eu-west-1)")
	flag.BoolVar(&cfg.DryRun, "dry-run", false, "Log every block's decision and upload nothing")
	flag.StringVar(&cfg.ReportPath, "report", "", "Write every non-skip decision to this file, one JSON object per line")
	flag.BoolVar(&cfg.AllowUnfinalized, "allow-unfinalized", false, "Archive past the node's finalized head, accepting that a height that loses its fork stays wrong")
	flag.IntVar(&cfg.BlockReceiptWorkers, "block-workers", DefaultBlockReceiptWorkers, "Block+receipt worker count")
	flag.IntVar(&cfg.TraceWorkers, "trace-workers", DefaultTraceWorkers, "Trace worker count")
	flag.IntVar(&cfg.UploadWorkers, "upload-workers", DefaultUploadWorkers, "S3 upload worker count")
	flag.IntVar(&cfg.BlockBatchSize, "block-batch-size", DefaultBlockBatchSize, "Blocks per batch for block+receipt fetching")
	flag.IntVar(&cfg.TraceBatchSize, "trace-batch-size", DefaultTraceBatchSize, "Blocks per batch for trace fetching")
	flag.Parse()

	return cfg
}

func validateConfig(cfg Config) error {
	if cfg.StartBlock == 0 {
		return fmt.Errorf("--start-block is required")
	}
	if cfg.EndBlock == 0 {
		return fmt.Errorf("--end-block is required")
	}
	if cfg.EndBlock < cfg.StartBlock {
		return fmt.Errorf("--end-block must be >= --start-block")
	}
	if cfg.Bucket == "" {
		return fmt.Errorf("--bucket is required")
	}
	if cfg.ChainID == 0 {
		return fmt.Errorf("--chain-id is required")
	}
	if _, err := chainDataTypes(cfg.ChainID); err != nil {
		return err
	}
	return validateBucketChain(cfg.ChainID, cfg.Bucket)
}

// chainDataTypes are the data types a chain's archive holds, in upload order:
// the block and its receipts everywhere, traces and blob sidecars only where
// that chain's watcher fetches them. Auditing a chain against another's set
// reports every height as incomplete.
func chainDataTypes(chainID int64) ([]s3key.DataType, error) {
	expectation, known := chainutil.DefaultChainExpectations()[chainID]
	if !known {
		return nil, fmt.Errorf(
			"chain %d has no declared block-data shape: add it to chainutil.DefaultChainExpectations, "+
				"or this run would archive a different data set than its watcher publishes", chainID)
	}

	return archivableTypes(chainID, expectation)
}

// archivableTypes refuses a shape this binary cannot fetch: nothing here asks a
// node for blob sidecars — createRPCClient never enables them and payloadFor
// answers nil — so a chain declaring them would fail every height instead.
func archivableTypes(chainID int64, expectation chainutil.BlockDataExpectation) ([]s3key.DataType, error) {
	if expectation.ExpectBlobs {
		return nil, fmt.Errorf(
			"chain %d declares %s, which this tool has no fetch path for: give createRPCClient and payloadFor one "+
				"before archiving that chain", chainID, s3key.Blobs)
	}

	types := []s3key.DataType{s3key.Block, s3key.Receipts}
	if expectation.ExpectTraces {
		types = append(types, s3key.Traces)
	}
	return types, nil
}

// validateBucketChain refuses another chain's archive: the bucket answers what
// each height already holds, so the wrong one reports holes that are not there
// — and, outside a dry run, writes this chain's blocks into it.
func validateBucketChain(chainID int64, bucket string) error {
	environment, err := chainutil.EnvironmentFromBucket(bucket)
	if err != nil {
		return err
	}
	return chainutil.ValidateS3BucketForChain(chainID, bucket, environment)
}
