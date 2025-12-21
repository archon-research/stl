// Package http contains the HTTP inbound adapter (REST API handlers).
package http

import (
	"encoding/json"
	"net/http"

	"github.com/archon-research/stl/stl-verify/internal/ports/inbound"
)

// Handler implements HTTP handlers for the API.
type Handler struct {
	service inbound.VerificationService
}

// NewHandler creates a new HTTP handler with the given service.
func NewHandler(service inbound.VerificationService) *Handler {
	return &Handler{
		service: service,
	}
}

// RegisterRoutes registers the HTTP routes with the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.Health)
	// Add more routes as needed
}

// Health handles the health check endpoint.
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	if err := h.service.Ping(r.Context()); err != nil {
		h.respondError(w, http.StatusServiceUnavailable, "service unhealthy")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) respondJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func (h *Handler) respondError(w http.ResponseWriter, status int, message string) {
	h.respondJSON(w, status, map[string]string{"error": message})
}
