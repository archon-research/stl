package alchemy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestProxyTLSConfig_WebSocketDial verifies that the WebSocket dial to Alchemy
// fails without the proxy CA cert and succeeds with proxyTLSConfig().
//
// Go caches SystemCertPool() for the process lifetime, and in this sandbox the
// proxy CA is already installed system-wide. To simulate a developer machine
// where the CA is NOT in the system store, we construct cert pools explicitly:
//   - "without": empty pool → nothing trusted → TLS handshake fails
//   - "with":    proxyTLSConfig() builds the correct pool → handshake succeeds
//
// This test requires:
//   - HTTPS_PROXY pointing to a TLS-intercepting proxy
//   - SSL_CERT_FILE pointing to the proxy's CA cert
//   - ALCHEMY_WS_URL and ALCHEMY_API_KEY set
//
// Run with: go test -run TestProxyTLSConfig_WebSocketDial -v ./internal/adapters/outbound/alchemy/
func TestProxyTLSConfig_WebSocketDial(t *testing.T) {
	wsURL := os.Getenv("ALCHEMY_WS_URL")
	apiKey := os.Getenv("ALCHEMY_API_KEY")

	if wsURL == "" || apiKey == "" {
		t.Skip("ALCHEMY_WS_URL and ALCHEMY_API_KEY required")
	}
	if os.Getenv("SSL_CERT_FILE") == "" {
		t.Skip("SSL_CERT_FILE required (not behind a TLS-intercepting proxy)")
	}
	if os.Getenv("HTTPS_PROXY") == "" {
		t.Skip("HTTPS_PROXY required (not behind a TLS-intercepting proxy)")
	}

	target := wsURL + "/" + apiKey

	// --- Test 1: WITHOUT proxy CA cert → empty trust pool → x509 error ---
	t.Run("without_proxy_ca", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		emptyPool := x509.NewCertPool() // trusts nothing
		dialer := websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
			Proxy:            http.ProxyFromEnvironment,
			TLSClientConfig:  &tls.Config{RootCAs: emptyPool},
		}
		conn, _, err := dialer.DialContext(ctx, target, nil)
		if conn != nil {
			conn.Close()
		}
		if err == nil {
			t.Fatal("expected TLS error without proxy CA cert, but dial succeeded")
		}
	})

	// --- Test 2: WITH proxyTLSConfig() → pool includes proxy CA → success ---
	t.Run("with_proxy_ca", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		tlsCfg := proxyTLSConfig()
		if tlsCfg == nil {
			t.Fatal("proxyTLSConfig() returned nil despite SSL_CERT_FILE being set")
		}
		dialer := websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
			Proxy:            http.ProxyFromEnvironment,
			TLSClientConfig:  tlsCfg,
		}
		conn, _, err := dialer.DialContext(ctx, target, nil)
		if err != nil {
			t.Fatalf("expected dial to succeed with proxyTLSConfig(), got: %v", err)
		}
		conn.Close()
	})
}
