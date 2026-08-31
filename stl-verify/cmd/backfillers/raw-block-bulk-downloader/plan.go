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
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// archiveHashPrefixBytes is how much of an archived object is fetched to read
// its block hash: the hash sits in the first JSON fields, so a prefix is enough.
const archiveHashPrefixBytes = 8 << 10

// noArchive is the version of a height the archive holds nothing for.
const noArchive = -1

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
// canonical one, or cannot be read, is a losing fork corrected at version+1.
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

// archivedBlockHash reads the block hash the archive holds at the height's top
// version, from the block object or else the receipts. It returns "" when
// neither is there to answer.
func archivedBlockHash(ctx context.Context, reader outbound.S3RangeReader, bucket string, blockNum int64, state archiveState) (string, error) {
	part := partition.GetPartition(blockNum)
	switch {
	case state.Present[s3key.Block]:
		key := s3key.BuildWithPartition(part, blockNum, state.Version, s3key.Block)
		return hashFromArchivedObject(ctx, reader, bucket, key, 1, "hash")
	case state.Present[s3key.Receipts]:
		key := s3key.BuildWithPartition(part, blockNum, state.Version, s3key.Receipts)
		return hashFromArchivedObject(ctx, reader, bucket, key, 2, "blockHash")
	default:
		return "", nil
	}
}

func hashFromArchivedObject(ctx context.Context, reader outbound.S3RangeReader, bucket, key string, depth int, field string) (string, error) {
	stored, err := reader.ReadRange(ctx, bucket, key, 0, archiveHashPrefixBytes-1)
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", key, err)
	}

	plain, err := gunzipPrefix(stored)
	if err != nil {
		return "", fmt.Errorf("decompressing %s: %w", key, err)
	}

	hash, ok := jsonStringField(plain, depth, field)
	if !ok {
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

// jsonStringField returns the first string value of the named field at the given
// object depth, tolerating a document truncated mid-object.
func jsonStringField(doc []byte, depth int, field string) (string, bool) {
	dec := json.NewDecoder(bytes.NewReader(doc))
	var objectFrames []bool
	expectKey, wanted := false, false

	for {
		token, err := dec.Token()
		if err != nil {
			return "", false
		}

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
			return value, ok
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
		cache:    make(map[string]map[int64]archiveState),
		loads:    make(map[string]*partitionLoad),
		s3Reader: s3Reader,
		bucket:   bucket,
		logger:   logger,
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

	keyList, err := pc.s3Reader.ListPrefix(ctx, pc.bucket, part+"/")
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

// decide reads what the archive holds at the height and weighs it against the
// hash the chain reports for the block just fetched.
func (p *blockPlanner) decide(ctx context.Context, r outbound.BlockData) (blockDecision, error) {
	canonicalHash, ok := jsonStringField(r.Block, 1, "hash")
	if !ok {
		return blockDecision{}, fmt.Errorf("block %d: RPC payload carries no hash", r.BlockNumber)
	}

	s3Start := time.Now()
	state, archivedHash, err := p.archived(ctx, r.BlockNumber)
	p.stats.s3CheckTime.Add(time.Since(s3Start).Nanoseconds())
	p.stats.s3CheckCalls.Add(1)
	if err != nil {
		return blockDecision{}, err
	}

	return blockDecision{
		BlockNumber:   r.BlockNumber,
		State:         state,
		ArchivedHash:  archivedHash,
		CanonicalHash: canonicalHash,
		Plan:          planBlock(state, archivedHash, canonicalHash),
	}, nil
}

func (p *blockPlanner) archived(ctx context.Context, blockNum int64) (archiveState, string, error) {
	state, err := p.cache.TopVersion(ctx, blockNum)
	if err != nil {
		return archiveState{}, "", fmt.Errorf("block %d: %w", blockNum, err)
	}
	hash, err := archivedBlockHash(ctx, p.reader, p.bucket, blockNum, state)
	if err != nil {
		return archiveState{}, "", err
	}
	return state, hash, nil
}
