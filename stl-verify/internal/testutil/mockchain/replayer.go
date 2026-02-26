// Implements the timed block emitter that cycles through stored block templates.
package mockchain

import (
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const defaultInterval = 12 * time.Second

type Status struct {
	Running       bool
	TemplateIndex int
	BlocksEmitted int64
}

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

func NewReplayer(headers []outbound.BlockHeader, store *DataStore, onBlock func(outbound.BlockHeader)) *Replayer {
	return &Replayer{
		templates: headers,
		store:     store,
		onBlock:   onBlock,
		interval:  defaultInterval,
	}
}

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
			r.emit()
		}
	}
}

func (r *Replayer) emit() {
	r.mu.Lock()

	header := r.templates[r.templateIndex]
	r.templateIndex = (r.templateIndex + 1) % len(r.templates)
	r.blocksEmitted++
	r.mu.Unlock()

	// Call the callback outside the lock to avoid potential deadlocks
	if r.onBlock != nil {
		r.onBlock(header)
	}
}

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

func (r *Replayer) GetStatus() Status {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return Status{
		Running:       r.running,
		TemplateIndex: r.templateIndex,
		BlocksEmitted: r.blocksEmitted,
	}
}
