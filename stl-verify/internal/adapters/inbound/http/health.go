// Package http provides inbound HTTP adapters for the verification service.
package http

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/inbound"
)

// HealthServerConfig holds configuration for the health server.
type HealthServerConfig struct {
	// Addr is the address to listen on (e.g., ":8080")
	Addr string

	// Logger for the health server
	Logger *slog.Logger

	// ReadTimeout for HTTP requests
	ReadTimeout time.Duration

	// WriteTimeout for HTTP responses
	WriteTimeout time.Duration
}

// HealthServerConfigDefaults returns a config with default values.
func HealthServerConfigDefaults() HealthServerConfig {
	return HealthServerConfig{
		Addr:         ":8080",
		Logger:       slog.Default(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
}

// HealthServer provides HTTP health check endpoints for ECS/Kubernetes deployments.
// It enables zero-downtime rolling deployments by reporting readiness only after
// the service has started processing data.
//
// Endpoints:
//   - /health/ready  - Returns 200 only when service is ready (readiness probe)
//   - /health/live   - Returns 200 when service is healthy (liveness probe)
//   - /health        - Combined health status for monitoring
//
// During deployment:
//  1. ECS starts new task
//  2. New task's /health/ready returns 503 until first block processed
//  3. Once ready (200), ECS marks new task as healthy
//  4. ECS sends SIGTERM to old task
//  5. Old task marks shuttingDown=true, health checks return 503
//  6. Old task gracefully shuts down
//  7. No gap because new task was processing before old stopped
type HealthServer struct {
	server       *http.Server
	checker      inbound.HealthChecker
	shuttingDown *atomic.Bool
	logger       *slog.Logger
}

// NewHealthServer creates a new health server.
func NewHealthServer(config HealthServerConfig, checker inbound.HealthChecker, shuttingDown *atomic.Bool) *HealthServer {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = 5 * time.Second
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 5 * time.Second
	}

	hs := &HealthServer{
		checker:      checker,
		shuttingDown: shuttingDown,
		logger:       config.Logger.With("component", "health-server"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health/ready", hs.handleReady)
	mux.HandleFunc("/health/live", hs.handleLive)
	mux.HandleFunc("/health", hs.handleHealth)

	hs.server = &http.Server{
		Addr:         config.Addr,
		Handler:      mux,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	}

	return hs
}

// Start begins listening for health check requests.
// This is non-blocking - it starts the server in a goroutine.
func (hs *HealthServer) Start() {
	go func() {
		hs.logger.Info("starting health server", "addr", hs.server.Addr)
		if err := hs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			hs.logger.Error("health server failed", "error", err)
		}
	}()
}

// Shutdown gracefully stops the health server.
func (hs *HealthServer) Shutdown(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return hs.server.Shutdown(ctx)
}

// handleReady handles the readiness probe.
// Returns 200 only after the service has processed at least one block.
// ECS uses this to decide when to stop the old task during rolling deployment.
func (hs *HealthServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if hs.shuttingDown.Load() {
		hs.respondJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "shutting_down"})
		return
	}
	if hs.checker.IsReady() {
		hs.respondJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	} else {
		hs.respondJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
	}
}

// handleLive handles the liveness probe.
// Returns 200 if the service is processing blocks regularly.
// ECS uses this to decide if the task needs to be restarted.
func (hs *HealthServer) handleLive(w http.ResponseWriter, r *http.Request) {
	if hs.shuttingDown.Load() {
		hs.respondJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "shutting_down"})
		return
	}
	if hs.checker.IsHealthy() {
		hs.respondJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
	} else {
		hs.respondJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unhealthy"})
	}
}

// handleHealth handles the combined health check endpoint.
// Returns full health status for monitoring and simple ALB health checks.
func (hs *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if hs.shuttingDown.Load() {
		hs.respondJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status":       "shutting_down",
			"ready":        false,
			"healthy":      false,
			"shuttingDown": true,
		})
		return
	}

	ready := hs.checker.IsReady()
	healthy := hs.checker.IsHealthy()
	status := "ok"
	statusCode := http.StatusOK

	if !ready || !healthy {
		status = "degraded"
		statusCode = http.StatusServiceUnavailable
	}

	hs.respondJSON(w, statusCode, map[string]any{
		"status":       status,
		"ready":        ready,
		"healthy":      healthy,
		"shuttingDown": false,
	})
}

func (hs *HealthServer) respondJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		hs.logger.Error("failed to encode JSON response", "error", err)
	}
}
