// Wires DataStore, wsHandler, httpHandler, and Replayer into a single TCP server.
package mockchain

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// maxReorgDepth is the maximum number of blocks that can be reorged at once.
// This matches the Ethereum finality window (~32 slots) with a safety margin.
const maxReorgDepth = 64

// Server is a mock Ethereum JSON-RPC server that combines a WebSocket handler,
// an HTTP JSON-RPC handler, a DataStore, and a Replayer into a single TCP listener.
type Server struct {
	store         *DataStore
	ws            *wsHandler
	rpc           *httpHandler
	replayer      *Replayer
	httpSrv       *http.Server
	listener      net.Listener
	reorgCtrl     *reorgController
	adminSrv      *http.Server
	adminListener net.Listener
}

// NewServer creates a Server backed by the given DataStore with the given block emission interval.
func NewServer(store *DataStore, interval time.Duration) *Server {
	ws := newWSHandler()
	replayer := NewReplayer(store.Headers(), store, ws.Broadcast, interval)
	rpc := newHTTPHandler(store, replayer)
	s := &Server{
		store:    store,
		ws:       ws,
		rpc:      rpc,
		replayer: replayer,
	}
	s.reorgCtrl = &reorgController{
		replayer: replayer,
		store:    store,
		ws:       ws,
	}
	s.httpSrv = &http.Server{Handler: s}
	return s
}

// ServeHTTP routes incoming requests: WebSocket upgrades go to the WS handler;
// all other requests go to the HTTP JSON-RPC handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if websocket.IsWebSocketUpgrade(r) {
		s.ws.ServeHTTP(w, r)
		return
	}
	s.rpc.ServeHTTP(w, r)
}

// Start binds to addr, starts the Replayer, and begins serving requests.
func (s *Server) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", addr, err)
	}
	s.listener = ln
	s.replayer.Start()
	go func() {
		if err := s.httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("mockchain: server error", "error", err)
		}
	}()
	return nil
}

// StartAdmin binds to addr and starts the admin HTTP API server.
func (s *Server) StartAdmin(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listening on admin addr %s: %w", addr, err)
	}
	s.adminListener = ln
	adminHandler := newAdminHandler(s.replayer, s.reorgCtrl, s.ws, s.rpc)
	s.adminSrv = &http.Server{Handler: adminHandler}
	go func() {
		if err := s.adminSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("mockchain: admin server error", "error", err)
		}
	}()
	return nil
}

// Stop halts the Replayer and shuts down the HTTP server gracefully.
func (s *Server) Stop() error {
	s.replayer.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if s.adminSrv != nil {
		_ = s.adminSrv.Shutdown(ctx)
	}

	return s.httpSrv.Shutdown(ctx)
}

// Addr returns the server's listening address, or nil if not started.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Disconnect closes the active WebSocket connection without stopping the server.
// The connected client will receive a close error and is expected to reconnect.
func (s *Server) Disconnect() {
	s.ws.Disconnect()
}

// SetErrorMode sets the HTTP RPC error injection mode.
func (s *Server) SetErrorMode(mode ErrorMode) {
	s.rpc.SetErrorMode(mode)
}

// Reorg triggers a chain reorganisation of the given depth.
// It emits reorg blocks with alternative hashes for the last `depth` blocks
// and broadcasts the new tip via WebSocket.
func (s *Server) Reorg(depth int) error {
	return s.reorgCtrl.trigger(depth)
}

// ReorgCount returns the total number of reorgs triggered on this server.
func (s *Server) ReorgCount() int64 {
	return s.reorgCtrl.reorgCount.Load()
}

// reorgController triggers chain reorgs by emitting alternative blocks.
type reorgController struct {
	mu         sync.Mutex
	replayer   *Replayer
	store      *DataStore
	ws         *wsHandler
	reorgCount atomic.Int64
}

// trigger generates a reorg of depth blocks and broadcasts the new tip.
func (rc *reorgController) trigger(depth int) error {
	if depth < 1 || depth > maxReorgDepth {
		return fmt.Errorf("reorg depth must be between 1 and %d, got %d", maxReorgDepth, depth)
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.replayer.mu.RLock()
	running := rc.replayer.running
	tip := rc.replayer.lastBlockNumber
	rc.replayer.mu.RUnlock()

	if !running {
		return fmt.Errorf("replayer is not running")
	}
	if tip == 0 {
		return fmt.Errorf("no blocks emitted yet")
	}

	base := rc.replayer.baseBlockNumber()
	commonAncestorNum := tip - int64(depth)
	if commonAncestorNum < base {
		return fmt.Errorf("reorg depth %d exceeds available blocks (%d)", depth, tip-base)
	}

	// Get the common ancestor hash (the branch point).
	rc.replayer.mu.RLock()
	ancestorHeader := rc.replayer.headerForNumberLocked(commonAncestorNum)
	rc.replayer.mu.RUnlock()
	prevHash := ancestorHeader.Hash

	// Generate the reorg branch.
	reorgHeaders := make([]outbound.BlockHeader, 0, depth)
	nTemplates := int64(len(rc.replayer.templates))

	for i := range depth {
		blockNum := commonAncestorNum + 1 + int64(i)
		offset := blockNum - base
		templateIndex := int(offset % nTemplates)
		loopIndex := int(offset / nTemplates)
		template := rc.replayer.templates[templateIndex]

		reorgHash := deriveReorgHash(template.Hash, loopIndex, i)

		header := outbound.BlockHeader{
			Number:     "0x" + strconv.FormatInt(blockNum, 16),
			Hash:       reorgHash,
			ParentHash: prevHash,
			Timestamp:  template.Timestamp,
		}
		rc.store.AddReorgHeader(reorgHash, header)

		for _, dt := range []string{"receipts", "traces", "blobs"} {
			if raw, ok := rc.store.Get(templateIndex, dt); ok {
				rc.store.AddReorgBlock(reorgHash, dt, raw)
			}
		}

		prevHash = reorgHash
		reorgHeaders = append(reorgHeaders, header)
	}

	// Update replayer so the next canonical emission uses the reorg tip as its parent.
	rc.replayer.mu.Lock()
	rc.replayer.setReorgTip(reorgHeaders[len(reorgHeaders)-1].Hash)
	rc.replayer.mu.Unlock()

	// Broadcast all reorg branch blocks in order so the watcher receives each
	// as a normal newHead and inserts the full reorg branch into the canonical chain.
	for _, h := range reorgHeaders {
		rc.ws.Broadcast(h)
	}

	rc.reorgCount.Add(1)
	return nil
}

// deriveReorgHash returns SHA-256("{originalHash}:{loopIndex}:reorg:{reorgIndex}") as "0x" + 64 hex chars.
// The ":reorg:" infix ensures reorg hashes are distinct from canonical hashes.
func deriveReorgHash(originalHash string, loopIndex, reorgIndex int) string {
	input := fmt.Sprintf("%s:%d:reorg:%d", originalHash, loopIndex, reorgIndex)
	sum := sha256.Sum256([]byte(input))
	return "0x" + hex.EncodeToString(sum[:])
}
