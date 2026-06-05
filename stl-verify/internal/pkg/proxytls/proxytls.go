// Package proxytls builds a TLS config that trusts an additional CA certificate
// for outbound connections (HTTPS and WebSocket) made from behind a
// TLS-intercepting proxy.
//
// Such proxies (e.g. Zscaler, corporate firewalls) terminate the upstream TLS
// connection and re-sign it with their own CA. Without adding that CA to the
// trust pool, dialers reject the proxy's certificate. The CA is supplied via
// the SSL_CERT_FILE environment variable.
//
// In production (where SSL_CERT_FILE is unset) Config returns nil and callers
// fall back to Go's default system certificate pool — no behaviour change.
package proxytls

import (
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"os"
)

// Config returns a *tls.Config that trusts the CA in SSL_CERT_FILE in addition
// to the system pool, or nil when SSL_CERT_FILE is unset or unreadable.
func Config() *tls.Config {
	certFile := os.Getenv("SSL_CERT_FILE")
	if certFile == "" {
		return nil
	}
	pem, err := os.ReadFile(certFile)
	if err != nil {
		slog.Warn("proxytls: SSL_CERT_FILE set but unreadable; falling back to default trust pool",
			"file", certFile, "error", err)
		return nil
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		slog.Warn("proxytls: system cert pool unavailable; starting from an empty pool", "error", err)
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pem) {
		slog.Warn("proxytls: no certificates parsed from SSL_CERT_FILE; check the PEM contents",
			"file", certFile)
	}
	return &tls.Config{RootCAs: pool} //nolint:gosec // only adds an extra CA to the default pool
}
