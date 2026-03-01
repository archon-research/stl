// Wires DataStore, wsHandler, httpHandler, and Replayer into a single TCP server.
package mockchain

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Server is a mock Ethereum JSON-RPC server that combines a WebSocket handler,
// an HTTP JSON-RPC handler, a DataStore, and a Replayer into a single TCP listener.
type Server struct {
	store    *DataStore
	ws       *wsHandler
	rpc      *httpHandler
	replayer *Replayer
	httpSrv  *http.Server
	listener net.Listener
}

// NewServer creates a Server backed by the given DataStore.
func NewServer(store *DataStore) *Server {
	ws := newWSHandler()
	rpc := newHTTPHandler(store)
	replayer := NewReplayer(store.Headers(), store, ws.Broadcast)
	s := &Server{
		store:    store,
		ws:       ws,
		rpc:      rpc,
		replayer: replayer,
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

// Stop halts the Replayer and shuts down the HTTP server gracefully.
func (s *Server) Stop() error {
	s.replayer.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpSrv.Shutdown(ctx)
}

// Addr returns the server's listening address, or nil if not started.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}
