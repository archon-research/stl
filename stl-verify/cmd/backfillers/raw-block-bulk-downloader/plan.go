package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/pkg/retry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// archiveHashPrefixBytes is how much of an archived object is fetched to read
// its block hash: the hash sits in the first JSON fields, so a prefix is enough.
const archiveHashPrefixBytes = 8 << 10

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

// archivedTypes are the data types this tool archives, in upload order.
var archivedTypes = []s3key.DataType{s3key.Block, s3key.Receipts, s3key.Traces}

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

// planBlock decides what one height needs. A top version whose hash is not the
// canonical one, or that no archived object carries a hash for, is corrected at
// version+1.
func planBlock(state archiveState, archivedHash, canonicalHash string) blockPlan {
	if state.Version == noArchive {
		return blockPlan{Action: actionFresh, Version: 0, DataTypes: slices.Clone(archivedTypes)}
	}
	if archivedHash == "" || !strings.EqualFold(archivedHash, canonicalHash) {
		return blockPlan{Action: actionRepublish, Version: state.Version + 1, DataTypes: slices.Clone(archivedTypes)}
	}

	missing := missingTypes(state.Present)
	if len(missing) == 0 {
		return blockPlan{Action: actionSkip, Version: state.Version}
	}
	return blockPlan{Action: actionFill, Version: state.Version, DataTypes: missing}
}

func missingTypes(present map[s3key.DataType]bool) []s3key.DataType {
	var missing []s3key.DataType
	for _, dataType := range archivedTypes {
		if !present[dataType] {
			missing = append(missing, dataType)
		}
	}
	return missing
}

// indexPartition folds a partition listing into the highest version present at
// each height, with the data types stored under it.
func indexPartition(keys []string) map[int64]archiveState {
	index := make(map[int64]archiveState)
	for _, key := range keys {
		parsed, ok := s3key.Parse(key)
		if !ok {
			continue
		}

		state, seen := index[parsed.BlockNumber]
		switch {
		case !seen || parsed.Version > state.Version:
			index[parsed.BlockNumber] = archiveState{
				Version: parsed.Version,
				Present: map[s3key.DataType]bool{parsed.DataType: true},
			}
		case parsed.Version == state.Version:
			state.Present[parsed.DataType] = true
		}
	}
	return index
}

// hashSource is where an archived object carries the block hash.
type hashSource struct {
	DataType s3key.DataType
	Depth    int
	Field    string
}

var hashSources = []hashSource{
	{s3key.Block, 1, "hash"},
	{s3key.Receipts, 2, "blockHash"},
}

// archivedBlockHash reads the block hash the archive holds at the height's top
// version, from the block object or else the receipts. It returns "" when
// neither is there to answer.
func archivedBlockHash(ctx context.Context, reader outbound.S3RangeReader, bucket string, blockNum int64, state archiveState) (string, error) {
	part := partition.GetPartition(blockNum)

	for _, source := range hashSources {
		if !state.Present[source.DataType] {
			continue
		}

		key := s3key.BuildWithPartition(part, blockNum, state.Version, source.DataType)
		hash, err := hashFromArchivedObject(ctx, reader, bucket, key, source.Depth, source.Field)
		if err != nil {
			return "", err
		}
		if hash != "" {
			return hash, nil
		}
	}
	return "", nil
}

// hashFromArchivedObject returns the hash an object carries, or "" for one that
// reads fine and carries none: a zero-tx block's empty receipt list and a null
// payload identify no block, and neither may fail the height on every run.
func hashFromArchivedObject(ctx context.Context, reader outbound.S3RangeReader, bucket, key string, depth int, field string) (string, error) {
	stored, err := reader.ReadRange(ctx, bucket, key, 0, archiveHashPrefixBytes-1)
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", key, err)
	}

	plain, err := gunzipPrefix(stored)
	if err != nil {
		return "", fmt.Errorf("decompressing %s: %w", key, err)
	}

	hash, outcome := scanJSONStringField(plain, depth, field)
	if outcome == fieldTruncated {
		return "", fmt.Errorf("no %s in the first %d bytes of %s", field, archiveHashPrefixBytes, key)
	}
	return hash, nil
}

// gunzipPrefix decompresses what it can of a truncated gzip stream: the
// unexpected EOF that ends a ranged read is the expected outcome here.
func gunzipPrefix(stored []byte) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(stored))
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	plain, err := io.ReadAll(gz)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	return plain, nil
}

// fieldOutcome tells a document that carries no such field from a prefix that
// ended before the field could appear.
type fieldOutcome int

const (
	fieldFound fieldOutcome = iota
	fieldAbsent
	fieldTruncated
)

// jsonStringField returns the first string value of the named field at the given
// object depth, tolerating a document truncated mid-object.
func jsonStringField(doc []byte, depth int, field string) (string, bool) {
	value, outcome := scanJSONStringField(doc, depth, field)
	return value, outcome == fieldFound
}

// scanJSONStringField looks for the first string value of the named field at the
// given object depth. A complete document that closed without the field is
// fieldAbsent; one whose last token ran into the end of a truncated prefix is
// fieldTruncated.
func scanJSONStringField(doc []byte, depth int, field string) (string, fieldOutcome) {
	dec := json.NewDecoder(bytes.NewReader(doc))
	var objectFrames []bool
	expectKey, wanted, seen := false, false, false

	for {
		token, err := dec.Token()
		if err != nil {
			if seen && len(objectFrames) == 0 && errors.Is(err, io.EOF) {
				return "", fieldAbsent
			}
			return "", fieldTruncated
		}
		seen = true

		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{':
				objectFrames = append(objectFrames, true)
				expectKey = true
			case '[':
				objectFrames = append(objectFrames, false)
				expectKey = false
			default:
				objectFrames = objectFrames[:len(objectFrames)-1]
				expectKey = len(objectFrames) > 0 && objectFrames[len(objectFrames)-1]
			}
			wanted = false
			continue
		}

		if len(objectFrames) == 0 || !objectFrames[len(objectFrames)-1] {
			continue
		}
		if expectKey {
			key, _ := token.(string)
			wanted = len(objectFrames) == depth && key == field
			expectKey = false
			continue
		}

		expectKey = true
		if wanted {
			value, ok := token.(string)
			if !ok {
				return "", fieldAbsent
			}
			return value, fieldFound
		}
	}
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
	index := indexPartition(keyList)

	pc.mu.Lock()
	defer pc.mu.Unlock()

	pc.cache[part] = index
	pc.logger.Debug("loaded partition from S3", "partition", part, "blockCount", len(index), "keyCount", len(keyList))
	return nil
}

func (pc *PartitionCache) listPrefixWithRetry(ctx context.Context, part string) ([]string, error) {
	alwaysRetry := func(error) bool { return true }
	logAttempt := func(attempt int, err error, backoff time.Duration) {
		pc.logger.Warn("retrying a partition listing", "partition", part, "attempt", attempt, "backoff", backoff, "error", err)
	}
	return retry.Do(ctx, pc.listRetry, alwaysRetry, logAttempt, func() ([]string, error) {
		return pc.s3Reader.ListPrefix(ctx, pc.bucket, part+"/")
	})
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
	canonicalHash, ok := jsonStringField(payload, 1, "hash")
	if !ok {
		return blockDecision{}, fmt.Errorf("block %d: RPC payload carries no hash", blockNum)
	}

	s3Start := time.Now()
	archivedHash, err := archivedBlockHash(ctx, p.reader, p.bucket, blockNum, state)
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
		Plan:          planBlock(state, archivedHash, canonicalHash),
	}, nil
}

// freshDecision is the decision for a height the archive holds nothing at: it
// needs every data type at version 0 whatever the chain reports, so nothing is
// read to reach it.
func freshDecision(blockNum int64) blockDecision {
	state := archiveState{Version: noArchive}
	return blockDecision{
		BlockNumber: blockNum,
		State:       state,
		Plan:        planBlock(state, "", ""),
	}
}
