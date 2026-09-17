// Package blockmetacfg is the deployment configuration both block_meta jobs read: the on-demand
// loader that fills a chain's history, and the scheduled top-up that keeps it current. One chain per
// deployment and the same environment shape for both, so the guards cannot drift between them.
//
// The task queue is not here: chainutil.TaskQueueName derives it from each binary's own base name.
package blockmetacfg

import (
	"fmt"
	"os"
	"strconv"

	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

// Config is the deployment's static configuration. One deployment serves one
// chain: the chain is not workflow input, because the S3 bucket, the Pod
// Identity grant and the task queue are all per-chain and a run that took the
// chain as a parameter could address none of them.
type Config struct {
	ChainID     int64
	Bucket      string
	DeployEnv   string
	DSN         string
	BatchSize   int
	Concurrency int
	HeadMargin  int64
}

// defaultHeadMargin keeps the newest blocks out of a run, because the archive trails the indexers at the
// head. HEAD_MARGIN tunes it per chain and 0 disables it.
const defaultHeadMargin = int64(300)

// Load reads the deployment's environment. It runs at registration rather
// than per run, so a misconfigured deployment is a worker that will not start
// instead of a run an operator has to start before finding out.
func Load() (Config, error) {
	var cfg Config

	chainID, err := chainutil.RequireChainID()
	if err != nil {
		return cfg, err
	}
	cfg.ChainID = int64(chainID)

	cfg.DSN = os.Getenv("DATABASE_URL")
	if cfg.DSN == "" {
		return cfg, fmt.Errorf("DATABASE_URL is required")
	}
	cfg.Bucket = os.Getenv("S3_BUCKET")
	if cfg.Bucket == "" {
		return cfg, fmt.Errorf("S3_BUCKET is required")
	}
	// Required, not defaulted: it selects which environment's bucket names the guard
	// below accepts, and empty surfaces from chainutil as a error naming neither.
	if cfg.DeployEnv, err = env.Require("DEPLOY_ENV"); err != nil {
		return cfg, err
	}

	if v := os.Getenv("BATCH_SIZE"); v != "" {
		if cfg.BatchSize, err = strconv.Atoi(v); err != nil {
			return cfg, fmt.Errorf("BATCH_SIZE: %w", err)
		}
		// A negative parses fine and then reads as "unset" downstream, running at the
		// default while the operator believes they set something.
		if cfg.BatchSize <= 0 {
			return cfg, fmt.Errorf("BATCH_SIZE must be positive, got %d", cfg.BatchSize)
		}
	}

	if cfg.Concurrency, err = PositiveEnv("CONCURRENCY", 0); err != nil {
		return cfg, err
	}
	margin, err := PositiveEnv("HEAD_MARGIN", int(defaultHeadMargin))
	if err != nil {
		return cfg, err
	}
	cfg.HeadMargin = int64(margin)

	// The same guard raw-data-backup takes at startup: the chain and the bucket
	// arrive as independent variables, and reading the wrong chain's archive
	// would write that chain's header times under this chain's id.
	if err := chainutil.ValidateS3BucketForChain(cfg.ChainID, cfg.Bucket, cfg.DeployEnv); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// PositiveEnv reads an optional non-negative integer, falling back to def when unset.
// A negative parses fine and then reads as "unset" downstream, running at a default
// the operator believes they overrode.
func PositiveEnv(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("%s must not be negative, got %d", key, n)
	}
	return n, nil
}
