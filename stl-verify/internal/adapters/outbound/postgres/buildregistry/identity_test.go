package buildregistry

import (
	"strings"
	"testing"
)

const validDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestResolveIdentity(t *testing.T) {
	tests := []struct {
		name        string
		gitHash     string
		argv0       string
		imageDigest string
		devIdentity bool
		want        Identity
		wantErr     string
	}{
		{
			name:        "full identity from the environment",
			gitHash:     "abc123",
			argv0:       "/app/sparklend-indexer",
			imageDigest: validDigest,
			want:        Identity{GitHash: "abc123", Service: "sparklend-indexer", ImageDigest: validDigest},
		},
		{
			name:        "service is the binary basename, whatever the invoking path",
			gitHash:     "abc123",
			argv0:       "./dist/oracle-price-indexer",
			imageDigest: validDigest,
			want:        Identity{GitHash: "abc123", Service: "oracle-price-indexer", ImageDigest: validDigest},
		},
		{
			name:        "dev identity fills a missing digest",
			gitHash:     "dev",
			argv0:       "main",
			devIdentity: true,
			want:        Identity{GitHash: "dev", Service: "main", ImageDigest: DevImageDigest},
		},
		{
			name:        "dev identity does not relax a digest that is set",
			gitHash:     "abc123",
			argv0:       "main",
			imageDigest: "not-a-digest",
			devIdentity: true,
			wantErr:     "IMAGE_DIGEST",
		},
		{
			name:    "missing git hash is an error",
			argv0:   "main",
			wantErr: "git hash not available",
		},
		{
			name:    "missing digest without the dev flag is an error",
			gitHash: "abc123",
			argv0:   "main",
			wantErr: "IMAGE_DIGEST is not set",
		},
		{
			name:        "malformed digest is an error",
			gitHash:     "abc123",
			argv0:       "main",
			imageDigest: "sha256:tooshort",
			wantErr:     "sha256:<64 hex>",
		},
		{
			name:        "empty argv0 is an error",
			gitHash:     "abc123",
			argv0:       "",
			imageDigest: validDigest,
			wantErr:     "service name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveIdentity(tt.gitHash, "", tt.argv0, tt.imageDigest, tt.devIdentity)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("resolveIdentity() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveIdentity() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("resolveIdentity() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
