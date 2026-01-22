package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// mockHealthChecker is a test implementation of HealthChecker
type mockHealthChecker struct {
	ready   bool
	healthy bool
}

func (m *mockHealthChecker) IsReady() bool   { return m.ready }
func (m *mockHealthChecker) IsHealthy() bool { return m.healthy }

func TestHealthServer_Ready(t *testing.T) {
	tests := []struct {
		name           string
		ready          bool
		shuttingDown   bool
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "ready returns 200",
			ready:          true,
			shuttingDown:   false,
			expectedStatus: http.StatusOK,
			expectedBody:   "ready",
		},
		{
			name:           "not ready returns 503",
			ready:          false,
			shuttingDown:   false,
			expectedStatus: http.StatusServiceUnavailable,
			expectedBody:   "not_ready",
		},
		{
			name:           "shutting down returns 503",
			ready:          true,
			shuttingDown:   true,
			expectedStatus: http.StatusServiceUnavailable,
			expectedBody:   "shutting_down",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := &mockHealthChecker{ready: tt.ready, healthy: true}
			var shuttingDown atomic.Bool
			shuttingDown.Store(tt.shuttingDown)

			hs := NewHealthServer(HealthServerConfig{Addr: ":0"}, checker, &shuttingDown)

			req := httptest.NewRequest("GET", "/health/ready", nil)
			w := httptest.NewRecorder()
			hs.handleReady(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, w.Code)
			}

			var resp map[string]string
			if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}
			if resp["status"] != tt.expectedBody {
				t.Errorf("expected status %q, got %q", tt.expectedBody, resp["status"])
			}
		})
	}
}

func TestHealthServer_Live(t *testing.T) {
	tests := []struct {
		name           string
		healthy        bool
		shuttingDown   bool
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "healthy returns 200",
			healthy:        true,
			shuttingDown:   false,
			expectedStatus: http.StatusOK,
			expectedBody:   "healthy",
		},
		{
			name:           "unhealthy returns 503",
			healthy:        false,
			shuttingDown:   false,
			expectedStatus: http.StatusServiceUnavailable,
			expectedBody:   "unhealthy",
		},
		{
			name:           "shutting down returns 503",
			healthy:        true,
			shuttingDown:   true,
			expectedStatus: http.StatusServiceUnavailable,
			expectedBody:   "shutting_down",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := &mockHealthChecker{ready: true, healthy: tt.healthy}
			var shuttingDown atomic.Bool
			shuttingDown.Store(tt.shuttingDown)

			hs := NewHealthServer(HealthServerConfig{Addr: ":0"}, checker, &shuttingDown)

			req := httptest.NewRequest("GET", "/health/live", nil)
			w := httptest.NewRecorder()
			hs.handleLive(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, w.Code)
			}

			var resp map[string]string
			if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}
			if resp["status"] != tt.expectedBody {
				t.Errorf("expected status %q, got %q", tt.expectedBody, resp["status"])
			}
		})
	}
}

func TestHealthServer_Health(t *testing.T) {
	tests := []struct {
		name           string
		ready          bool
		healthy        bool
		shuttingDown   bool
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "ready and healthy returns ok",
			ready:          true,
			healthy:        true,
			shuttingDown:   false,
			expectedStatus: http.StatusOK,
			expectedBody:   "ok",
		},
		{
			name:           "not ready returns degraded",
			ready:          false,
			healthy:        true,
			shuttingDown:   false,
			expectedStatus: http.StatusServiceUnavailable,
			expectedBody:   "degraded",
		},
		{
			name:           "not healthy returns degraded",
			ready:          true,
			healthy:        false,
			shuttingDown:   false,
			expectedStatus: http.StatusServiceUnavailable,
			expectedBody:   "degraded",
		},
		{
			name:           "shutting down returns shutting_down",
			ready:          true,
			healthy:        true,
			shuttingDown:   true,
			expectedStatus: http.StatusServiceUnavailable,
			expectedBody:   "shutting_down",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := &mockHealthChecker{ready: tt.ready, healthy: tt.healthy}
			var shuttingDown atomic.Bool
			shuttingDown.Store(tt.shuttingDown)

			hs := NewHealthServer(HealthServerConfig{Addr: ":0"}, checker, &shuttingDown)

			req := httptest.NewRequest("GET", "/health", nil)
			w := httptest.NewRecorder()
			hs.handleHealth(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, w.Code)
			}

			var resp map[string]any
			if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}
			if resp["status"] != tt.expectedBody {
				t.Errorf("expected status %q, got %q", tt.expectedBody, resp["status"])
			}
			if resp["ready"] != tt.ready && !tt.shuttingDown {
				t.Errorf("expected ready=%v, got %v", tt.ready, resp["ready"])
			}
			if resp["healthy"] != tt.healthy && !tt.shuttingDown {
				t.Errorf("expected healthy=%v, got %v", tt.healthy, resp["healthy"])
			}
		})
	}
}
