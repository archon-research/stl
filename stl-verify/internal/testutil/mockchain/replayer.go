// Implements the timed block emitter that cycles through stored block templates.
package mockchain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const defaultInterval = 12 * time.Second // default Ethereum block time

// maxCachedHashes is the maximum number of emitted derived hashes retained in memory.
// Older entries are evicted FIFO. The watcher's reorg walk-back never exceeds the
// finality window (~32 blocks), so 128 is more than sufficient.
const maxCachedHashes = 128

// zeroParentHash is the parent hash used for the first block in a chain (all zeroes).
var zeroParentHash = "0x" + strings.Repeat("0", 64)

// status holds a snapshot of the Replayer's current state.
type status struct {
	Running       bool
	TemplateIndex int
	BlocksEmitted int64
}

// Replayer cycles through a fixed set of block header templates, calling onBlock
// at a fixed interval (default: 12 s, matching Ethereum's block time).
//
// On each emission, block numbers increment monotonically and hashes are derived
// from the original template hash and the current loop index, producing an infinite
// valid chain from a finite set of templates.
type Replayer struct {
	mu sync.RWMutex

	running       bool
	templateIndex int
	loopIndex     int
	blocksEmitted int64

	lastBlockNumber int64  // block number of the most-recently emitted block
	prevDerivedHash string // derived hash of the most-recently emitted block

	// derivedHashToBlock maps a derived hash to its absolute block number (populated by emit).
	// Template index is computed from block number on demand, so no separate index map is needed.
	derivedHashToBlock map[string]int64
	// hashOrder tracks insertion order for FIFO eviction of derivedHashToBlock.
	hashOrder []string

	templates []outbound.BlockHeader
	store     *DataStore
	onBlock   func(outbound.BlockHeader)
	interval  time.Duration

	stopCh chan struct{}
	doneCh chan struct{}
}

// NewReplayer creates a Replayer that emits blocks from headers at the given interval.
// Pass 0 for interval to use the default (12 s, matching Ethereum's block time).
// onBlock is called with each emitted header; store is used to look up associated block data.
func NewReplayer(headers []outbound.BlockHeader, store *DataStore, onBlock func(outbound.BlockHeader), interval time.Duration) *Replayer {
	if interval < 0 {
		panic(fmt.Sprintf("mockchain: NewReplayer: interval must be non-negative, got %v", interval))
	}
	if interval == 0 {
		interval = defaultInterval
	}
	return &Replayer{
		templates:          headers,
		store:              store,
		onBlock:            onBlock,
		interval:           interval,
		derivedHashToBlock: make(map[string]int64),
	}
}

// baseBlockNumber returns the absolute block number of the first template.
// Returns 0 if no templates are loaded or if the first template has no number set.
func (r *Replayer) baseBlockNumber() int64 {
	if len(r.templates) == 0 || r.templates[0].Number == "" {
		return 0
	}
	n, err := hexutil.ParseInt64(r.templates[0].Number)
	if err != nil {
		panic(fmt.Sprintf("mockchain: baseBlockNumber: cannot parse %q: %v", r.templates[0].Number, err))
	}
	return n
}

// SetInterval sets the block emission interval. Can be called before or during replay.
// Returns an error if d <= 0. Takes effect on the next tick after the current interval fires.
func (r *Replayer) SetInterval(d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("mockchain: SetInterval: duration must be positive, got %v", d)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.interval = d
	return nil
}

// Start begins emitting blocks on the configured interval.
// Returns an error if no templates are loaded or if already running.
func (r *Replayer) Start() error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("mockchain: replayer is already running")
	}
	if len(r.templates) == 0 {
		r.mu.Unlock()
		return fmt.Errorf("mockchain: replayer has no block templates loaded")
	}
	r.running = true
	r.stopCh = make(chan struct{})
	r.doneCh = make(chan struct{})
	r.templateIndex = 0
	r.loopIndex = 0
	r.blocksEmitted = 0
	r.lastBlockNumber = 0
	r.prevDerivedHash = ""
	r.derivedHashToBlock = make(map[string]int64)
	r.hashOrder = r.hashOrder[:0]
	r.mu.Unlock()

	go r.emitLoop(r.stopCh, r.doneCh)
	return nil
}

func (r *Replayer) emitLoop(stopCh <-chan struct{}, doneCh chan struct{}) {
	defer close(doneCh)

	r.mu.RLock()
	currentInterval := r.interval
	r.mu.RUnlock()

	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			select {
			case <-stopCh:
				return
			default:
			}
			r.emit()
			// Re-read interval and reset ticker if it changed.
			r.mu.RLock()
			newInterval := r.interval
			r.mu.RUnlock()
			if newInterval != currentInterval {
				currentInterval = newInterval
				ticker.Reset(currentInterval)
			}
		}
	}
}

func (r *Replayer) emit() {
	header, onBlock := r.prepareEmission()

	if onBlock != nil {
		onBlock(header)
	}
}

// prepareEmission advances replay state under lock and returns the emitted header
// along with the callback to execute after releasing the lock.
func (r *Replayer) prepareEmission() (outbound.BlockHeader, func(outbound.BlockHeader)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	templateIndex := r.templateIndex
	template := r.templates[templateIndex]
	blockNumber := r.baseBlockNumber() + r.blocksEmitted

	parentHash := r.parentHashForEmit()

	header := patchHeader(template, blockNumber, r.loopIndex, parentHash)

	r.derivedHashToBlock[header.Hash] = blockNumber
	r.hashOrder = append(r.hashOrder, header.Hash)
	if len(r.hashOrder) > maxCachedHashes {
		oldest := r.hashOrder[0]
		r.hashOrder = r.hashOrder[1:]
		delete(r.derivedHashToBlock, oldest)
	}
	r.lastBlockNumber = blockNumber
	r.prevDerivedHash = header.Hash

	r.templateIndex++
	if r.templateIndex >= len(r.templates) {
		r.templateIndex = 0
		r.loopIndex++
	}
	r.blocksEmitted++

	return header, r.onBlock
}

// Stop halts block emission and returns the total number of blocks emitted.
// It is safe to call on a Replayer that was never started.
func (r *Replayer) Stop() int64 {
	r.mu.Lock()
	if !r.running {
		emitted := r.blocksEmitted
		r.mu.Unlock()
		return emitted
	}
	r.running = false
	stopCh := r.stopCh
	doneCh := r.doneCh
	close(stopCh)
	r.mu.Unlock()

	<-doneCh

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.blocksEmitted
}

// CurrentBlockNumber returns the block number of the last emitted block, or 0 if
// no blocks have been emitted.
func (r *Replayer) CurrentBlockNumber() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastBlockNumber
}

// TemplateIndexForHash returns the template index for a derived hash, or false if
// the hash has not been emitted yet.
func (r *Replayer) TemplateIndexForHash(hash string) (int, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	blockNum, ok := r.derivedHashToBlock[hash]
	if !ok {
		return 0, false
	}
	offset := blockNum - r.baseBlockNumber()
	return int(offset % int64(len(r.templates))), true
}

// TemplateIndexForNumber returns the template index for a given absolute block number.
// Returns false if the block number has not been emitted yet.
func (r *Replayer) TemplateIndexForNumber(blockNum int64) (int, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.blocksEmitted == 0 || blockNum > r.lastBlockNumber || blockNum < r.baseBlockNumber() {
		return 0, false
	}
	offset := blockNum - r.baseBlockNumber()
	return int(offset % int64(len(r.templates))), true
}

// HeaderForHash returns the fully patched BlockHeader for a given derived hash.
// Returns false if the hash has not been emitted yet.
func (r *Replayer) HeaderForHash(hash string) (outbound.BlockHeader, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	blockNum, ok := r.derivedHashToBlock[hash]
	if !ok {
		return outbound.BlockHeader{}, false
	}
	return r.headerForNumberLocked(blockNum), true
}

// HeaderForNumber returns the fully patched BlockHeader for a given absolute block number.
// Returns false if the block number has not been emitted yet.
func (r *Replayer) HeaderForNumber(blockNum int64) (outbound.BlockHeader, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.blocksEmitted == 0 || blockNum > r.lastBlockNumber || blockNum < r.baseBlockNumber() {
		return outbound.BlockHeader{}, false
	}
	return r.headerForNumberLocked(blockNum), true
}

// headerForNumberLocked reconstructs the patched header for blockNum deterministically.
// Must be called with r.mu held (at least read-locked).
func (r *Replayer) headerForNumberLocked(blockNum int64) outbound.BlockHeader {
	offset := blockNum - r.baseBlockNumber()
	templateIndex := int(offset % int64(len(r.templates)))
	loopIndex := int(offset / int64(len(r.templates)))
	template := r.templates[templateIndex]

	var parentHash string
	if offset == 0 {
		parentHash = zeroParentHash
	} else {
		prevOffset := offset - 1
		prevTemplateIndex := int(prevOffset % int64(len(r.templates)))
		prevLoopIndex := int(prevOffset / int64(len(r.templates)))
		parentHash = deriveHash(r.templates[prevTemplateIndex].Hash, prevLoopIndex)
	}

	return patchHeader(template, blockNum, loopIndex, parentHash)
}

// setReorgTip updates prevDerivedHash so the next emission uses the reorg tip as its parent.
// It does NOT change blocksEmitted or lastBlockNumber — the block number sequence continues normally.
// Must be called while holding r.mu (write lock).
func (r *Replayer) setReorgTip(hash string) {
	r.prevDerivedHash = hash
}

// getStatus returns a snapshot of the Replayer's current state.
func (r *Replayer) getStatus() status {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return status{
		Running:       r.running,
		TemplateIndex: r.templateIndex,
		BlocksEmitted: r.blocksEmitted,
	}
}

// deriveHash returns SHA-256("{originalHash}:{loopIndex}") as "0x" + 64 hex chars.
func deriveHash(originalHash string, loopIndex int) string {
	input := fmt.Sprintf("%s:%d", originalHash, loopIndex)
	sum := sha256.Sum256([]byte(input))
	return "0x" + hex.EncodeToString(sum[:])
}

// parentHashForEmit returns the parent hash for the next block emission.
// Must be called with r.mu held.
func (r *Replayer) parentHashForEmit() string {
	if r.blocksEmitted == 0 {
		return zeroParentHash
	}
	return r.prevDerivedHash
}

// patchHeader returns a copy of tmpl with Number, Hash, and ParentHash replaced.
func patchHeader(tmpl outbound.BlockHeader, blockNumber int64, loopIndex int, parentHash string) outbound.BlockHeader {
	h := tmpl
	h.Number = "0x" + strconv.FormatInt(blockNumber, 16)
	h.Hash = deriveHash(tmpl.Hash, loopIndex)
	h.ParentHash = parentHash
	return h
}
