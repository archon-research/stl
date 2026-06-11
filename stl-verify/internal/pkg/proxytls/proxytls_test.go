package proxytls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// selfSignedCA generates a self-signed CA certificate and returns its PEM
// encoding alongside the parsed certificate (for later trust verification).
func selfSignedCA(t *testing.T) (caPEM []byte, ca *x509.Certificate) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "proxytls test CA"},
		NotBefore:             time.Unix(0, 0),
		NotAfter:              time.Unix(1<<31, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), ca
}

func TestConfig(t *testing.T) {
	caPEM, ca := selfSignedCA(t)

	tests := []struct {
		name string
		// fileContents, when non-nil, is written to a temp file whose path
		// becomes SSL_CERT_FILE. When nil, SSL_CERT_FILE points at a path that
		// does not exist (unless name == "unset").
		fileContents []byte
		unset        bool
		wantNil      bool
		// wantTrusted, when non-nil, must be trusted by the returned RootCAs.
		wantTrusted *x509.Certificate
	}{
		{name: "unset returns nil", unset: true, wantNil: true},
		{name: "missing file returns nil", wantNil: true},
		{name: "valid CA merged into pool", fileContents: caPEM, wantTrusted: ca},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch {
			case tt.unset:
				t.Setenv("SSL_CERT_FILE", "")
			case tt.fileContents == nil:
				t.Setenv("SSL_CERT_FILE", filepath.Join(t.TempDir(), "does-not-exist.pem"))
			default:
				path := filepath.Join(t.TempDir(), "ca.pem")
				if err := os.WriteFile(path, tt.fileContents, 0o600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("SSL_CERT_FILE", path)
			}

			got := Config()

			if tt.wantNil {
				if got != nil {
					t.Errorf("Config() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("Config() = nil, want non-nil")
			}
			if got.RootCAs == nil {
				t.Fatal("Config().RootCAs = nil, want a populated pool")
			}
			if tt.wantTrusted != nil {
				// Prove the CA was actually merged: verifying the CA against the
				// returned pool only succeeds if it is present in RootCAs.
				if _, err := tt.wantTrusted.Verify(x509.VerifyOptions{Roots: got.RootCAs}); err != nil {
					t.Errorf("CA not trusted by Config().RootCAs: %v", err)
				}
			}
		})
	}
}
