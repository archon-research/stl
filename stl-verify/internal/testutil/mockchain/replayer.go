// Implements the timed block emitter that cycles through stored block templates.
package mockchain

import (
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const defaultInterval = 12 * time.Second // default Ethereum block time

// status holds a snapshot of the Replayer's current state.
type status struct {
	Running       bool
	TemplateIndex int
	BlocksEmitted int64
}

// Replayer cycles through a fixed set of block header templates, calling onBlock
// at a fixed interval (default: 12 s, matching Ethereum's block time).
type Replayer struct {
	mu sync.RWMutex

	running       bool
	templateIndex int
	blocksEmitted int64

	templates []outbound.BlockHeader
	store     *DataStore
	onBlock   func(outbound.BlockHeader)
	interval  time.Duration

	stopCh chan struct{}
	doneCh chan struct{}
}

// NewReplayer creates a Replayer that emits blocks from headers at the default interval.
// onBlock is called with each emitted header; store is used to look up associated block data.
func NewReplayer(headers []outbound.BlockHeader, store *DataStore, onBlock func(outbound.BlockHeader)) *Replayer {
	return &Replayer{
		templates: headers,
		store:     store,
		onBlock:   onBlock,
		interval:  defaultInterval,
	}
}

// Start begins emitting blocks on the configured interval. It is a no-op if
// already running or if no templates were provided.
func (r *Replayer) Start() {
	r.mu.Lock()
	if r.running || len(r.templates) == 0 {
		r.mu.Unlock()
		return
	}
	r.running = true
	r.stopCh = make(chan struct{})
	r.doneCh = make(chan struct{})
	r.templateIndex = 0
	r.blocksEmitted = 0
	r.mu.Unlock()

	go r.emitLoop(r.stopCh, r.doneCh)
}

func (r *Replayer) emitLoop(stopCh <-chan struct{}, doneCh chan struct{}) {
	defer close(doneCh)

	ticker := time.NewTicker(r.interval)
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
		}
	}
}

func (r *Replayer) emit() {
	r.mu.Lock()
	header := r.templates[r.templateIndex]
	r.templateIndex = (r.templateIndex + 1) % len(r.templates)
	r.blocksEmitted++
	onBlock := r.onBlock
	r.mu.Unlock()

	if onBlock != nil {
		onBlock(header)
	}
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
