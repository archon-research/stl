package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/smithy-go"

	"github.com/archon-research/stl/stl-verify/internal/pkg/archiveblock"
	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/pkg/retry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// noArchive is the version of a height the archive holds nothing for.
const noArchive = -1

// listRetryAttempts bounds the tries a partition listing gets. Hundreds of
// workers park on one partition, so a single throttled ListPrefix would
// otherwise fail every height behind it.
const listRetryAttempts = 5

func defaultListRetry() retry.Config {
	return retry.Config{
		MaxRetries:     listRetryAttempts - 1,
		InitialBackoff: 250 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		BackoffFactor:  2,
		Jitter:         true,
	}
}

// archiveState is what the archive holds at one height: the highest object
// version present, and the data types stored under it.
type archiveState struct {
	Version int
	Present map[s3key.DataType]bool
}

// blockAction names the decision reached for one height.
type blockAction string

const (
	actionFresh     blockAction = "fresh"
	actionSkip      blockAction = "skip"
	actionFill      blockAction = "fill"
	actionRepublish blockAction = "republish"
)

// blockPlan is what to write for one height.
type blockPlan struct {
	Action    blockAction
	Version   int
	DataTypes []s3key.DataType
}

// blockDecision is a plan plus the evidence it was reached from.
type blockDecision struct {
	BlockNumber   int64
	State         archiveState
	ArchivedHash  string
	CanonicalHash string
	Plan          blockPlan
}

// planBlock decides what one height needs, against the data types the chain's
// archive holds. A top version whose hash is not the canonical one, or that no
// archived object carries a hash for, is corrected at version+1.
func planBlock(types []s3key.DataType, state archiveState, archivedHash, canonicalHash string) blockPlan {
	if state.Version == noArchive {
		return blockPlan{Action: actionFresh, Version: 0, DataTypes: slices.Clone(types)}
	}
	if archivedHash == "" || !strings.EqualFold(archivedHash, canonicalHash) {
		return blockPlan{
			Action:    actionRepublish,
			Version:   s3key.NextVersion(state.Version, true),
			DataTypes: slices.Clone(types),
		}
	}

	missing := missingTypes(types, state.Present)
	if len(missing) == 0 {
		return blockPlan{Action: actionSkip, Version: state.Version}
	}
	return blockPlan{Action: actionFill, Version: state.Version, DataTypes: missing}
}

func missingTypes(types []s3key.DataType, present map[s3key.DataType]bool) []s3key.DataType {
	var missing []s3key.DataType
	for _, dataType := range types {
		if !present[dataType] {
			missing = append(missing, dataType)
		}
	}
	return missing
}

// indexPartition folds a partition listing into what the archive holds at each
// height. The fold is shared with the block republisher, which reads the same
// objects to decide the version it corrects a height at, and it refuses a key it
// cannot read rather than plan around a slot it cannot see.
func indexPartition(keys []string) (map[int64]archiveState, error) {
	occupied, err := s3key.Occupancies(keys)
	if err != nil {
		return nil, err
	}

	index := make(map[int64]archiveState, len(occupied))
	for blockNum, top := range occupied {
		index[blockNum] = archiveState{Version: top.Version, Present: top.DataTypes}
	}
	return index, nil
}

// PartitionCache caches what the archive holds, one listing per S3 partition.
type PartitionCache struct {
	mu        sync.RWMutex
	cache     map[string]map[int64]archiveState
	loads     map[string]*partitionLoad
	s3Reader  outbound.S3Reader
	bucket    string
	logger    *slog.Logger
	listRetry retry.Config
	hitCount  atomic.Int64
	missCount atomic.Int64
}

// partitionLoad collapses the listings the workers arriving at a cold partition
// would otherwise each issue; hundreds land on the same one at every boundary.
type partitionLoad struct {
	once sync.Once
	err  error
}

func NewPartitionCache(s3Reader outbound.S3Reader, bucket string, logger *slog.Logger) *PartitionCache {
	return &PartitionCache{
		cache:     make(map[string]map[int64]archiveState),
		loads:     make(map[string]*partitionLoad),
		s3Reader:  s3Reader,
		bucket:    bucket,
		logger:    logger,
		listRetry: defaultListRetry(),
	}
}

// ensurePartitionLoaded loads a partition into the cache if not already present.
func (pc *PartitionCache) ensurePartitionLoaded(ctx context.Context, part string) error {
	pc.mu.RLock()
	_, loaded := pc.cache[part]
	pc.mu.RUnlock()

	if loaded {
		pc.hitCount.Add(1)
		return nil
	}

	load := pc.loadFor(part)
	load.once.Do(func() { load.err = pc.listPartition(ctx, part) })
	if load.err != nil {
		pc.forgetLoad(part, load)
	}
	return load.err
}

func (pc *PartitionCache) loadFor(part string) *partitionLoad {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	load, ok := pc.loads[part]
	if !ok {
		load = &partitionLoad{}
		pc.loads[part] = load
	}
	return load
}

// forgetLoad drops a failed load so the next caller retries rather than
// inheriting a throttled listing for the rest of the run.
func (pc *PartitionCache) forgetLoad(part string, load *partitionLoad) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.loads[part] == load {
		delete(pc.loads, part)
	}
}

func (pc *PartitionCache) listPartition(ctx context.Context, part string) error {
	pc.missCount.Add(1)

	keyList, err := pc.listPrefixWithRetry(ctx, part)
	if err != nil {
		return fmt.Errorf("failed to list partition %s: %w", part, err)
	}
	index, err := indexPartition(keyList)
	if err != nil {
		return fmt.Errorf("reading partition %s: %w", part, err)
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	pc.cache[part] = index
	pc.logger.Debug("loaded partition from S3", "partition", part, "blockCount", len(index), "keyCount", len(keyList))
	return nil
}

func (pc *PartitionCache) listPrefixWithRetry(ctx context.Context, part string) ([]string, error) {
	logAttempt := func(attempt int, err error, backoff time.Duration) {
		pc.logger.Warn("retrying a partition listing", "partition", part, "attempt", attempt, "backoff", backoff, "error", err)
	}
	return retry.Do(ctx, pc.listRetry, retryableListing, logAttempt, func() ([]string, error) {
		return pc.s3Reader.ListPrefix(ctx, pc.bucket, s3key.PartitionPrefix(part))
	})
}

// retryableAPICodes are the S3 error codes worth another attempt: throttling,
// and the server-side faults that do not always carry a server fault flag.
var retryableAPICodes = map[string]bool{
	"SlowDown":             true,
	"Throttling":           true,
	"ThrottlingException":  true,
	"RequestThrottled":     true,
	"RequestLimitExceeded": true,
	"RequestTimeout":       true,
	"InternalError":        true,
	"ServiceUnavailable":   true,
}

// retryableListing decides whether a failed partition listing is worth another
// attempt. Retrying a permanent API error — a missing grant, a bucket that is
// not there, credentials that have expired — costs five attempts per partition
// on top of the SDK's own retries and walks the whole range before failing,
// which is why anything the service answers as a client fault stops the run.
func retryableListing(err error) bool {
	// A run being shut down is not a fault to ride out: retrying logs a warning
	// per partition on a SIGTERM and delays the exit for nothing.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return true
	}
	if retryableAPICodes[apiErr.ErrorCode()] || apiErr.ErrorFault() == smithy.FaultServer {
		return true
	}

	var statusErr interface{ HTTPStatusCode() int }
	return errors.As(err, &statusErr) && statusErr.HTTPStatusCode() >= 500
}

// TopVersion returns what the archive holds at a height, loading that partition's
// listing on first use.
func (pc *PartitionCache) TopVersion(ctx context.Context, blockNum int64) (archiveState, error) {
	part := partition.GetPartition(blockNum)
	if err := pc.ensurePartitionLoaded(ctx, part); err != nil {
		return archiveState{}, err
	}

	pc.mu.RLock()
	defer pc.mu.RUnlock()

	state, ok := pc.cache[part][blockNum]
	if !ok {
		return archiveState{Version: noArchive}, nil
	}
	return archiveState{Version: state.Version, Present: maps.Clone(state.Present)}, nil
}

func (pc *PartitionCache) GetStats() (hits, misses int64) {
	return pc.hitCount.Load(), pc.missCount.Load()
}

// blockPlanner decides what to write for a fetched block.
type blockPlanner struct {
	cache  *PartitionCache
	reader outbound.S3RangeReader
	bucket string
	types  []s3key.DataType
	stats  *Stats
}

// topVersion reports what the archive holds at a height.
func (p *blockPlanner) topVersion(ctx context.Context, blockNum int64) (archiveState, error) {
	s3Start := time.Now()
	state, err := p.cache.TopVersion(ctx, blockNum)
	p.stats.s3CheckTime.Add(time.Since(s3Start).Nanoseconds())
	p.stats.s3CheckCalls.Add(1)
	if err != nil {
		return archiveState{}, fmt.Errorf("block %d: %w", blockNum, err)
	}
	return state, nil
}

// decide weighs what the archive holds at a height against the hash the chain
// reports for it. Only the hash is read from the payload, so a header answers
// as well as a full block.
func (p *blockPlanner) decide(ctx context.Context, blockNum int64, state archiveState, payload json.RawMessage) (blockDecision, error) {
	canonicalHash, ok := archiveblock.HashFromPayload(payload)
	if !ok {
		return blockDecision{}, fmt.Errorf("block %d: RPC payload carries no hash", blockNum)
	}

	s3Start := time.Now()
	archivedHash, _, err := archiveblock.Hash(ctx, p.reader, p.bucket, blockNum, state.Version)
	p.stats.s3CheckTime.Add(time.Since(s3Start).Nanoseconds())
	p.stats.s3CheckCalls.Add(1)
	if err != nil {
		return blockDecision{}, err
	}

	return blockDecision{
		BlockNumber:   blockNum,
		State:         state,
		ArchivedHash:  archivedHash,
		CanonicalHash: canonicalHash,
		Plan:          planBlock(p.types, state, archivedHash, canonicalHash),
	}, nil
}

// fresh is the decision for a height the archive holds nothing at: it needs
// every data type at version 0 whatever the chain reports, so nothing is read to
// reach it.
func (p *blockPlanner) fresh(blockNum int64) blockDecision {
	state := archiveState{Version: noArchive}
	return blockDecision{
		BlockNumber: blockNum,
		State:       state,
		Plan:        planBlock(p.types, state, "", ""),
	}
}
