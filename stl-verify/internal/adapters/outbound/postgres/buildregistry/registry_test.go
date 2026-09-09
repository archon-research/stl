package buildregistry

import (
	"strings"
	"testing"
)

// validateGitHash takes the hash as a parameter rather than resolving it from
// ambient build info, so its rejection of "" and "unknown" -- and its
// acceptance of a real-looking hash -- is fully determined by the input, with
// no dependency on db (it takes none) or on whether the test binary happens to
// carry VCS info (it never calls buildinfo.Resolve). Deterministic under any
// build mode.
func TestValidateGitHash(t *testing.T) {
	tests := []struct {
		name    string
		gitHash string
		wantErr bool
	}{
		{name: "empty string is rejected", gitHash: "", wantErr: true},
		{name: "the literal unknown is rejected", gitHash: "unknown", wantErr: true},
		{name: "a real-looking hash is accepted", gitHash: "abc123def456", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateGitHash(tt.gitHash)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("validateGitHash(%q) = nil error, want an error rejecting it", tt.gitHash)
				}
				if !strings.Contains(err.Error(), `"`+tt.gitHash+`"`) {
					t.Errorf("error = %q, want it to name the offending value %q", err.Error(), tt.gitHash)
				}
				return
			}
			if err != nil {
				t.Errorf("validateGitHash(%q) = %v, want nil error", tt.gitHash, err)
			}
		})
	}
}
