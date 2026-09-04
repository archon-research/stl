package main

import (
	"fmt"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

// config is the deployment's static configuration. The blocks are deliberately
// absent — they are per-run workflow input (see RepublishParams) — and so is the
// version, which each run derives per height from what s3Bucket already holds.
type config struct {
	chainID        int64
	deployEnv      string
	snsTopicARN    string
	snsEndpoint    string
	s3Bucket       string
	rpcURL         string
	redisAddr      string
	redisPassword  string
	redisKeyPrefix string
	enableTraces   bool
	enableBlobs    bool
}

const (
	defaultRedisAddr = "localhost:6379"

	// defaultRedisKeyPrefix is what production leaves unset; only a test driving
	// this binary sets REDIS_KEY_PREFIX, to namespace its keys in a shared Redis.
	defaultRedisKeyPrefix = "stl"

	// cacheTTL is the watcher's own Redis TTL. A republished payload has to
	// outlive the SQS hop for every subscriber, and nothing more: the backup
	// worker's RPC fallback covers a consumer that arrives after it expires.
	cacheTTL = 2 * time.Hour
)

func loadConfig() (config, error) {
	chainID, err := chainutil.RequireChainID()
	if err != nil {
		return config{}, err
	}
	// ExpectReceipts is not read: the watcher fetches receipts unconditionally
	// (live_data.cacheAndPublishBlockData), so it has no flag to mirror.
	expectation, known := chainutil.DefaultChainExpectations()[int64(chainID)]
	if !known {
		return config{}, fmt.Errorf(
			"chain %d has no declared block-data shape: add it to chainutil.DefaultChainExpectations, "+
				"or a republished block would carry a different data set than its watcher publishes", chainID)
	}

	deployEnv, err := env.Require("DEPLOY_ENV")
	if err != nil {
		return config{}, err
	}
	snsTopicARN, err := env.Require("AWS_SNS_TOPIC_ARN")
	if err != nil {
		return config{}, err
	}
	// The chain and the topic arrive as independent variables, and publishing
	// chain X's blocks onto chain Y's topic would hand every consumer of Y a
	// version-1 correction built from another chain's data.
	if err := chainutil.ValidateSNSTopicForChain(int64(chainID), snsTopicARN, deployEnv); err != nil {
		return config{}, fmt.Errorf("AWS_SNS_TOPIC_ARN / CHAIN_ID mismatch: %w", err)
	}

	// The raw archive is what the version of every repaired height is derived
	// from, so another chain's bucket would answer for heights this chain never
	// published — and land the correction in an occupied slot.
	s3Bucket, err := env.Require("S3_BUCKET")
	if err != nil {
		return config{}, err
	}
	if err := chainutil.ValidateS3BucketForChain(int64(chainID), s3Bucket, deployEnv); err != nil {
		return config{}, fmt.Errorf("S3_BUCKET / CHAIN_ID mismatch: %w", err)
	}

	rpcURL, err := resolveRPCURL()
	if err != nil {
		return config{}, err
	}

	return config{
		chainID:        int64(chainID),
		deployEnv:      deployEnv,
		snsTopicARN:    snsTopicARN,
		snsEndpoint:    env.Get("AWS_SNS_ENDPOINT", ""),
		s3Bucket:       s3Bucket,
		rpcURL:         rpcURL,
		redisAddr:      env.Get("REDIS_ADDR", defaultRedisAddr),
		redisPassword:  env.Get("REDIS_PASSWORD", ""),
		redisKeyPrefix: env.Get("REDIS_KEY_PREFIX", defaultRedisKeyPrefix),
		enableTraces:   expectation.ExpectTraces,
		enableBlobs:    expectation.ExpectBlobs,
	}, nil
}

// resolveRPCURL builds the node URL from the same ALCHEMY_HTTP_URL +
// ALCHEMY_API_KEY pair every other indexer uses, so this worker's secret wiring
// matches theirs. Neither has a default: a deployment that forgot the URL would
// otherwise fetch mainnet blocks for its own chain's heights and publish them on
// its own chain's topic.
func resolveRPCURL() (string, error) {
	httpURL, err := env.Require("ALCHEMY_HTTP_URL")
	if err != nil {
		return "", err
	}
	apiKey, err := env.Require("ALCHEMY_API_KEY")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s", httpURL, apiKey), nil
}
