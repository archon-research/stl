package mockchain

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newTestAdminHandler creates an adminHandler backed by a fixture store and a Replayer
// with a time.Minute interval so background emissions never fire during tests.
func newTestAdminHandler(t *testing.T) (*adminHandler, *Replayer) {
	t.Helper()
	store := NewFixtureDataStore()
	ws := newWSHandler()
	replayer := NewReplayer(store.Headers(), store, ws.Broadcast, time.Minute)
	rpc := newHTTPHandler(store, replayer)
	rc := &reorgController{
		replayer: replayer,
		store:    store,
		ws:       ws,
	}
	return newAdminHandler(replayer, rc, ws, rpc), replayer
}

// adminDo sends a request to the admin handler and returns the recorder.
func adminDo(t *testing.T, h *adminHandler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var bodyReader io.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	}
	r := httptest.NewRequest(method, path, bodyReader)
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// TestAdmin_NotFound verifies that an unknown path returns 404.
func TestAdmin_NotFound(t *testing.T) {
	h, _ := newTestAdminHandler(t)
	w := adminDo(t, h, http.MethodGet, "/unknown", "")
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

// TestAdmin_Start covers the method guard, success, and duplicate-start conflict.
func TestAdmin_Start(t *testing.T) {
	t.Run("method guard", func(t *testing.T) {
		h, _ := newTestAdminHandler(t)
		w := adminDo(t, h, http.MethodGet, "/start", "")
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})

	t.Run("success", func(t *testing.T) {
		h, r := newTestAdminHandler(t)
		t.Cleanup(func() { r.Stop() })
		w := adminDo(t, h, http.MethodPost, "/start", "")
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		var body map[string]bool
		if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !body["ok"] {
			t.Error("expected ok:true")
		}
	})

	t.Run("already running returns 409", func(t *testing.T) {
		h, r := newTestAdminHandler(t)
		t.Cleanup(func() { r.Stop() })
		if w := adminDo(t, h, http.MethodPost, "/start", ""); w.Code != http.StatusOK {
			t.Fatalf("first start: expected 200, got %d", w.Code)
		}
		w := adminDo(t, h, http.MethodPost, "/start", "")
		if w.Code != http.StatusConflict {
			t.Errorf("expected 409, got %d", w.Code)
		}
	})
}

// TestAdmin_Stop covers the method guard and the last_block response field.
func TestAdmin_Stop(t *testing.T) {
	t.Run("method guard", func(t *testing.T) {
		h, _ := newTestAdminHandler(t)
		w := adminDo(t, h, http.MethodGet, "/stop", "")
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})

	t.Run("returns last_block field", func(t *testing.T) {
		h, _ := newTestAdminHandler(t)
		w := adminDo(t, h, http.MethodPost, "/stop", "")
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		var body map[string]any
		if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if _, ok := body["last_block"]; !ok {
			t.Error("expected last_block field in response")
		}
	})
}

// TestAdmin_Speed covers the method guard, valid interval, and rejection of invalid bodies.
func TestAdmin_Speed(t *testing.T) {
	t.Run("method guard", func(t *testing.T) {
		h, _ := newTestAdminHandler(t)
		w := adminDo(t, h, http.MethodGet, "/speed", "")
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})

	t.Run("valid interval", func(t *testing.T) {
		h, _ := newTestAdminHandler(t)
		w := adminDo(t, h, http.MethodPost, "/speed", `{"interval_ms":500}`)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})

	badBodies := []struct {
		name string
		body string
	}{
		{"zero interval", `{"interval_ms":0}`},
		{"negative interval", `{"interval_ms":-1}`},
		{"missing field", `{}`},
		{"bad JSON", `{notjson}`},
	}
	for _, tt := range badBodies {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newTestAdminHandler(t)
			w := adminDo(t, h, http.MethodPost, "/speed", tt.body)
			if w.Code != http.StatusBadRequest {
				t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
			}
		})
	}
}

// TestAdmin_Status covers the method guard and presence of all required response fields.
func TestAdmin_Status(t *testing.T) {
	t.Run("method guard", func(t *testing.T) {
		h, _ := newTestAdminHandler(t)
		w := adminDo(t, h, http.MethodPost, "/status", "")
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})

	t.Run("returns all fields", func(t *testing.T) {
		h, _ := newTestAdminHandler(t)
		w := adminDo(t, h, http.MethodGet, "/status", "")
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		var body map[string]any
		if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		for _, field := range []string{"running", "blocks_emitted", "template_index", "connected_clients", "reorg_count"} {
			if _, ok := body[field]; !ok {
				t.Errorf("expected field %q in status response", field)
			}
		}
		if running, _ := body["running"].(bool); running {
			t.Error("expected running=false before start")
		}
	})
}

// TestAdmin_Reorg covers the method guard, all invalid depth cases, no-blocks and not-running
// cases, and a full success path.
func TestAdmin_Reorg(t *testing.T) {
	t.Run("method guard", func(t *testing.T) {
		h, _ := newTestAdminHandler(t)
		w := adminDo(t, h, http.MethodGet, "/reorg", "")
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})

	// These all return 400 regardless of replayer state.
	badDepths := []struct {
		name  string
		query string
	}{
		{"missing depth", "/reorg"},
		{"non-numeric depth", "/reorg?depth=abc"},
		{"zero depth", "/reorg?depth=0"},
		{"negative depth", "/reorg?depth=-1"},
		{"depth too large", "/reorg?depth=65"},
	}
	for _, tt := range badDepths {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newTestAdminHandler(t)
			w := adminDo(t, h, http.MethodPost, tt.query, "")
			if w.Code != http.StatusBadRequest {
				t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
			}
		})
	}

	t.Run("replayer not running returns 400", func(t *testing.T) {
		h, _ := newTestAdminHandler(t)
		w := adminDo(t, h, http.MethodPost, "/reorg?depth=1", "")
		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Code)
		}
	})

	t.Run("no blocks emitted returns 400", func(t *testing.T) {
		h, r := newTestAdminHandler(t)
		t.Cleanup(func() { r.Stop() })
		if err := r.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
		w := adminDo(t, h, http.MethodPost, "/reorg?depth=1", "")
		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Code)
		}
	})

	t.Run("success", func(t *testing.T) {
		h, r := newTestAdminHandler(t)
		t.Cleanup(func() { r.Stop() })
		if err := r.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
		// Emit enough blocks for a depth-2 reorg (need at least depth+1).
		for range 3 {
			emitOrFail(t, r)
		}
		w := adminDo(t, h, http.MethodPost, "/reorg?depth=2", "")
		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})
}

// TestAdmin_Disconnect covers the method guard and success path.
func TestAdmin_Disconnect(t *testing.T) {
	t.Run("method guard", func(t *testing.T) {
		h, _ := newTestAdminHandler(t)
		w := adminDo(t, h, http.MethodGet, "/disconnect", "")
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})

	t.Run("success", func(t *testing.T) {
		h, _ := newTestAdminHandler(t)
		w := adminDo(t, h, http.MethodPost, "/disconnect", "")
		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
	})
}

// TestAdmin_Error covers the method guard, bad JSON, all invalid modes, and all valid modes.
func TestAdmin_Error(t *testing.T) {
	t.Run("method guard", func(t *testing.T) {
		h, _ := newTestAdminHandler(t)
		w := adminDo(t, h, http.MethodGet, "/error", "")
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})

	t.Run("bad JSON returns 400", func(t *testing.T) {
		h, _ := newTestAdminHandler(t)
		w := adminDo(t, h, http.MethodPost, "/error", `{notjson}`)
		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Code)
		}
	})

	invalidModes := []struct{ name, mode string }{
		{"empty string", ""},
		{"unknown name", "invalid"},
		{"wrong case", "NONE"},
	}
	for _, tt := range invalidModes {
		t.Run("invalid mode: "+tt.name, func(t *testing.T) {
			h, _ := newTestAdminHandler(t)
			w := adminDo(t, h, http.MethodPost, "/error", `{"mode":"`+tt.mode+`"}`)
			if w.Code != http.StatusBadRequest {
				t.Errorf("expected 400 for mode %q, got %d", tt.mode, w.Code)
			}
		})
	}

	validModes := []string{"none", "429", "500", "timeout", "once429", "once500"}
	for _, mode := range validModes {
		t.Run("valid mode: "+mode, func(t *testing.T) {
			h, _ := newTestAdminHandler(t)
			w := adminDo(t, h, http.MethodPost, "/error", `{"mode":"`+mode+`"}`)
			if w.Code != http.StatusOK {
				t.Errorf("expected 200 for mode %q, got %d: %s", mode, w.Code, w.Body.String())
			}
		})
	}
}

// TestParseErrorMode verifies all valid and invalid mode strings.
func TestParseErrorMode(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantOK   bool
		wantMode ErrorMode
	}{
		{"none", "none", true, ErrorModeNone},
		{"429", "429", true, ErrorMode429},
		{"500", "500", true, ErrorMode500},
		{"timeout", "timeout", true, ErrorModeTimeout},
		{"once429", "once429", true, ErrorModeOnce429},
		{"once500", "once500", true, ErrorModeOnce500},
		{"empty string", "", false, ErrorModeNone},
		{"unknown name", "invalid", false, ErrorModeNone},
		{"wrong case", "NONE", false, ErrorModeNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mode, ok := parseErrorMode(tt.input)
			if ok != tt.wantOK {
				t.Errorf("parseErrorMode(%q) ok = %v, want %v", tt.input, ok, tt.wantOK)
			}
			if ok && mode != tt.wantMode {
				t.Errorf("parseErrorMode(%q) mode = %v, want %v", tt.input, mode, tt.wantMode)
			}
		})
	}
}
