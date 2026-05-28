// Package dexbootstrap consolidates the shared CLI/env parsing + wiring used
// by the three DEX SQS workers (curve, uniswap-v3, balancer). Without this
// helper, each worker's main.go duplicates ~300 LOC of identical setup; the
// reviews for VEC-79 (N7-3 + S3) flagged the duplication, and the new
// internal/pkg/chainutil package gave us the validation surface to consolidate
// against.
package dexbootstrap

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

// Config is the resolved runtime configuration common to every DEX worker.
// Worker-specific config (e.g. UV3's NFPM address) is read separately from
// the worker's main.go after ParseConfig returns.
type Config struct {
	QueueURL          string
	RedisAddr         string
	DBURL             string
	AlchemyURL        string
	S3Bucket          string
	DeployEnv         string
	MaxMessages       int
	WaitTime          int
	VisibilityTimeout int
	ChainID           int64
}

// ParseConfig reads the canonical DEX-worker flag + env set and validates
// the cross-cutting invariants:
//   - all required env vars present
//   - CHAIN_ID parses as int64
//   - SQS_WAIT_TIME / SQS_VISIBILITY_TIMEOUT, when set, parse as int
//   - S3_BUCKET matches the chain ID + deploy environment via chainutil
//
// flagSetName lets each worker keep its own usage string (so `-help` still
// identifies the binary), but the flags themselves are uniform.
func ParseConfig(flagSetName string, args []string) (Config, error) {
	fs := flag.NewFlagSet(flagSetName, flag.ContinueOnError)
	queueURL := fs.String("queue", "", "SQS Queue URL")
	redisAddr := fs.String("redis", "", "Redis address")
	dbURL := fs.String("db", "", "PostgreSQL connection URL")
	maxMessages := fs.Int("max", 10, "Max messages per poll")
	waitTime := fs.Int("wait", 20, "Wait time in seconds (long polling)")
	visibilityTimeout := fs.Int("visibility-timeout", 300, "SQS visibility timeout in seconds")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	cfg := Config{
		QueueURL:          *queueURL,
		RedisAddr:         *redisAddr,
		DBURL:             *dbURL,
		MaxMessages:       *maxMessages,
		WaitTime:          *waitTime,
		VisibilityTimeout: *visibilityTimeout,
	}

	if cfg.QueueURL == "" {
		cfg.QueueURL = env.Get("AWS_SQS_QUEUE_URL", "")
	}
	if cfg.QueueURL == "" {
		return Config{}, fmt.Errorf("queue URL not provided (use -queue flag or AWS_SQS_QUEUE_URL env var)")
	}

	if cfg.DBURL == "" {
		cfg.DBURL = env.Get("DATABASE_URL", "")
	}
	if cfg.DBURL == "" {
		return Config{}, fmt.Errorf("database URL not provided (use -db flag or DATABASE_URL env var)")
	}

	alchemyAPIKey := os.Getenv("ALCHEMY_API_KEY")
	if alchemyAPIKey == "" {
		return Config{}, fmt.Errorf("ALCHEMY_API_KEY environment variable is required")
	}
	alchemyHTTPURL := env.Get("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2")
	cfg.AlchemyURL = fmt.Sprintf("%s/%s", alchemyHTTPURL, alchemyAPIKey)

	if cfg.RedisAddr == "" {
		cfg.RedisAddr = env.Get("REDIS_ADDR", "")
	}
	if cfg.RedisAddr == "" {
		return Config{}, fmt.Errorf("redis address not provided (use -redis flag or REDIS_ADDR env var)")
	}

	if waitTimeStr := env.Get("SQS_WAIT_TIME", ""); waitTimeStr != "" {
		v, err := strconv.Atoi(waitTimeStr)
		if err != nil {
			return Config{}, fmt.Errorf("parsing SQS_WAIT_TIME %q: %w", waitTimeStr, err)
		}
		cfg.WaitTime = v
	}
	if visTimeStr := env.Get("SQS_VISIBILITY_TIMEOUT", ""); visTimeStr != "" {
		v, err := strconv.Atoi(visTimeStr)
		if err != nil {
			return Config{}, fmt.Errorf("parsing SQS_VISIBILITY_TIMEOUT %q: %w", visTimeStr, err)
		}
		cfg.VisibilityTimeout = v
	}

	chainIDStr := env.Get("CHAIN_ID", "")
	if chainIDStr == "" {
		return Config{}, fmt.Errorf("CHAIN_ID environment variable is required (no silent default to mainnet)")
	}
	chainID, err := strconv.ParseInt(chainIDStr, 10, 64)
	if err != nil {
		return Config{}, fmt.Errorf("parsing CHAIN_ID %q: %w", chainIDStr, err)
	}
	cfg.ChainID = chainID

	cfg.S3Bucket = env.Get("S3_BUCKET", "")
	if cfg.S3Bucket == "" {
		return Config{}, fmt.Errorf("S3_BUCKET environment variable is required")
	}

	cfg.DeployEnv = env.Get("DEPLOY_ENV", "")
	if cfg.DeployEnv == "" {
		return Config{}, fmt.Errorf("DEPLOY_ENV environment variable is required")
	}

	// Cross-check the bucket name against chain ID + deploy env. Catches a
	// staging-bucket / prod-deploy mixup at boot — pre-fix, this would only
	// surface as missing/stale data hours later. chainutil is the package the
	// review-7 N7-3 finding explicitly named as the validation surface.
	if err := chainutil.ValidateS3BucketForChain(cfg.ChainID, cfg.S3Bucket, cfg.DeployEnv); err != nil {
		return Config{}, fmt.Errorf("S3 bucket / chain / env mismatch: %w", err)
	}

	return cfg, nil
}
