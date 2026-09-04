package main

import (
	"fmt"

	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

// config is the deployment's static configuration. The block range is
// deliberately absent: it is per-run workflow input (see BackfillParams).
type config struct {
	bucket     string
	rpcURL     string
	chainID    int64
	goroutines int
	probeBatch int
}

const (
	defaultGoroutines = 16
	defaultProbeBatch = 50
)

func loadConfig() (config, error) {
	chainID, err := chainutil.RequireChainID()
	if err != nil {
		return config{}, err
	}
	// RequireChainID parses but does not range-check, and a non-positive id would
	// otherwise surface far downstream as an RPC chain-ID mismatch.
	if chainID <= 0 {
		return config{}, fmt.Errorf("CHAIN_ID must be a positive chain ID, got %d", chainID)
	}

	bucket, err := env.Require("S3_BUCKET")
	if err != nil {
		return config{}, err
	}

	rpcURL, err := chainutil.AlchemyRPCURL(int64(chainID))
	if err != nil {
		return config{}, err
	}

	goroutines, err := positiveIntEnv("BACKFILL_GOROUTINES", defaultGoroutines)
	if err != nil {
		return config{}, err
	}
	probeBatch, err := positiveIntEnv("BACKFILL_PROBE_BATCH", defaultProbeBatch)
	if err != nil {
		return config{}, err
	}

	return config{
		bucket:     bucket,
		rpcURL:     rpcURL,
		chainID:    int64(chainID),
		goroutines: goroutines,
		probeBatch: probeBatch,
	}, nil
}

// positiveIntEnv rejects a non-positive override rather than letting it through:
// zero workers stalls the scan forever and a zero probe batch loops without
// progress, both of which look like a hung run rather than a bad setting.
func positiveIntEnv(name string, fallback int) (int, error) {
	value, err := env.GetInt(name, fallback)
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %d", name, value)
	}
	return value, nil
}
