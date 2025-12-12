package http

import (
	"encoding/json"
	"net/http"
)

type Handler struct {
	// verifier ports.VerifierService
}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) VerifyTransaction(w http.ResponseWriter, r *http.Request) {
	// Parse transaction ID from request
	// Call verifier service
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
