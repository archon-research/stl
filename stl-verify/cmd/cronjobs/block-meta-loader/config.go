package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

// config is the deployment's static configuration. One deployment serves one
// chain: the chain is not workflow input, because the S3 bucket, the Pod
// Identity grant and the task queue are all per-chain and a run that took the
// chain as a parameter could address none of them.
type config struct {
	chainID   int64
	bucket    string
	deployEnv string
	dsn       string
	batchSize int
}

const (
	// ethereumQueueName is what an Ethereum deployment polls; every other chain
	// prefixes it with its own name, the way its Deployment is named.
	ethereumQueueName = "block-meta-loader"
	ethereumChain     = "ethereum"
)

// taskQueueName is the Temporal task queue this deployment polls, which is also
// its OTel service name and its Deployment name — the alerts and the runbook
// select on all three. A run is per chain, so each chain has its own queue.
func taskQueueName() (string, error) {
	chainID, err := chainutil.RequireChainID()
	if err != nil {
		return "", err
	}
	chain, err := chainutil.ChainSlug(int64(chainID))
	if err != nil {
		return "", err
	}
	if chain == ethereumChain {
		return ethereumQueueName, nil
	}
	return chain + "-" + ethereumQueueName, nil
}

// loadConfig reads the deployment's environment. It runs at registration rather
// than per run, so a misconfigured deployment is a worker that will not start
// instead of a run an operator has to start before finding out.
func loadConfig() (config, error) {
	var cfg config

	chainID, err := chainutil.RequireChainID()
	if err != nil {
		return cfg, err
	}
	cfg.chainID = int64(chainID)

	cfg.dsn = os.Getenv("DATABASE_URL")
	if cfg.dsn == "" {
		return cfg, fmt.Errorf("DATABASE_URL is required")
	}
	cfg.bucket = os.Getenv("S3_BUCKET")
	if cfg.bucket == "" {
		return cfg, fmt.Errorf("S3_BUCKET is required")
	}
	// Required, not defaulted: it selects which environment's bucket names the guard
	// below accepts, and empty surfaces from chainutil as a error naming neither.
	if cfg.deployEnv, err = env.Require("DEPLOY_ENV"); err != nil {
		return cfg, err
	}

	if v := os.Getenv("BATCH_SIZE"); v != "" {
		if cfg.batchSize, err = strconv.Atoi(v); err != nil {
			return cfg, fmt.Errorf("BATCH_SIZE: %w", err)
		}
		// A negative parses fine and then reads as "unset" downstream, running at the
		// default while the operator believes they set something.
		if cfg.batchSize <= 0 {
			return cfg, fmt.Errorf("BATCH_SIZE must be positive, got %d", cfg.batchSize)
		}
	}

	// The same guard raw-data-backup takes at startup: the chain and the bucket
	// arrive as independent variables, and reading the wrong chain's archive
	// would write that chain's header times under this chain's id.
	if err := chainutil.ValidateS3BucketForChain(cfg.chainID, cfg.bucket, cfg.deployEnv); err != nil {
		return cfg, err
	}
	return cfg, nil
}
