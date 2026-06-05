package proxytls

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfig(t *testing.T) {
	t.Run("unset returns nil", func(t *testing.T) {
		t.Setenv("SSL_CERT_FILE", "")
		if got := Config(); got != nil {
			t.Errorf("Config() = %v, want nil when SSL_CERT_FILE unset", got)
		}
	})

	t.Run("missing file returns nil", func(t *testing.T) {
		t.Setenv("SSL_CERT_FILE", filepath.Join(t.TempDir(), "does-not-exist.pem"))
		if got := Config(); got != nil {
			t.Errorf("Config() = %v, want nil when file unreadable", got)
		}
	})

	t.Run("existing file returns config", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ca.pem")
		if err := os.WriteFile(path, []byte("-----BEGIN CERTIFICATE-----\nnot-a-real-cert\n-----END CERTIFICATE-----\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("SSL_CERT_FILE", path)
		got := Config()
		if got == nil {
			t.Fatal("Config() = nil, want non-nil when SSL_CERT_FILE is set and readable")
		}
		if got.RootCAs == nil {
			t.Error("Config().RootCAs = nil, want a populated pool")
		}
	})
}
