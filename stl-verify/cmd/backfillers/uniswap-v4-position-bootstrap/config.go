package main

import (
	"errors"
	"fmt"

	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4bootstrap"
)

// config is the deployment's static configuration, shared by both of this
// worker's workflow types. A run derives its pin and scan start for itself (head
// minus the finality depth, and the lowest registered deploy block, or the
// PositionManager's), and a resumed attempt takes the pin from its own record.
type config struct {
	rpcURL    string
	bootstrap uniswapv4bootstrap.Config
}

// loadConfig reads the scan knobs from the environment; an unset knob is the
// service's default (zero means "use the default" all the way down).
func loadConfig() (config, error) {
	chainID, err := chainutil.RequireChainID()
	if err != nil {
		return config{}, err
	}
	rpcURL, err := chainutil.AlchemyRPCURL(int64(chainID))
	if err != nil {
		return config{}, err
	}

	cfg := config{rpcURL: rpcURL, bootstrap: uniswapv4bootstrap.Config{ChainID: int64(chainID)}}
	positionBatch, transferBatch := int64(0), int64(0)
	if err := errors.Join(
		fillInt64FromEnv(&cfg.bootstrap.FinalityDepth, "FINALITY_DEPTH"),
		fillInt64FromEnv(&cfg.bootstrap.InitialWindow, "INITIAL_WINDOW"),
		fillInt64FromEnv(&cfg.bootstrap.MinWindow, "MIN_WINDOW"),
		fillInt64FromEnv(&cfg.bootstrap.MaxWindow, "MAX_WINDOW"),
		fillInt64FromEnv(&positionBatch, "POSITION_BATCH"),
		fillInt64FromEnv(&transferBatch, "TRANSFER_BATCH"),
	); err != nil {
		return config{}, err
	}
	cfg.bootstrap.PositionBatch = int(positionBatch)
	cfg.bootstrap.TransferBatch = int(transferBatch)

	if err := cfg.bootstrap.Validate(); err != nil {
		return config{}, fmt.Errorf("validating the scan knobs: %w", err)
	}
	return cfg, nil
}

func fillInt64FromEnv(into *int64, key string) error {
	value, err := env.GetInt64(key, 0)
	if err != nil {
		return err
	}
	*into = value
	return nil
}
