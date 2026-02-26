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

type Server struct {
	store    *DataStore
	ws       *wsHandler
	rpc      *httpHandler
	replayer *Replayer
	httpSrv  *http.Server
	listener net.Listener
}

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

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if websocket.IsWebSocketUpgrade(r) {
		s.ws.ServeHTTP(w, r)
		return
	}
	s.rpc.ServeHTTP(w, r)
}

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

func (s *Server) Stop() {
	s.replayer.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.httpSrv.Shutdown(ctx); err != nil {
		slog.Error("mockchain: shutdown error", "error", err)
	}
}

func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}
