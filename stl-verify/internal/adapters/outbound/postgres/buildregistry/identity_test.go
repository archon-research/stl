package buildregistry

import (
	"strings"
	"testing"
)

func TestResolveIdentity(t *testing.T) {
	tests := []struct {
		name    string
		gitHash string
		argv0   string
		want    Identity
		wantErr string
	}{
		{
			name:    "full identity from the environment",
			gitHash: "abc123",
			argv0:   "/app/sparklend-indexer",
			want:    Identity{GitHash: "abc123", Service: "sparklend-indexer"},
		},
		{
			name:    "service is the binary basename, whatever the invoking path",
			gitHash: "abc123",
			argv0:   "./dist/oracle-price-indexer",
			want:    Identity{GitHash: "abc123", Service: "oracle-price-indexer"},
		},
		{
			name:    "missing git hash is an error",
			argv0:   "main",
			wantErr: "git hash not available",
		},
		{
			// Dockerfile.common defaults the versioning args to "unknown"
			// (ORB-366), so this is what an image built without them reports.
			name:    "the literal unknown is an error, not an identity",
			gitHash: "unknown",
			argv0:   "/app/watcher",
			wantErr: "git hash not available",
		},
		{
			name:    "empty argv0 is an error",
			gitHash: "abc123",
			argv0:   "",
			wantErr: "service name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveIdentity(tt.gitHash, "", tt.argv0)
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
