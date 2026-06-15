package blockverifier

import (
	"strings"
	"testing"
)

func TestNew_KnownChainsReturnEtherscanVerifier(t *testing.T) {
	chains := []int64{1, 10, 130, 8453, 42161, 43114}
	for _, chainID := range chains {
		t.Run(strings.ReplaceAll(formatChain(chainID), " ", "_"), func(t *testing.T) {
			v, err := New(chainID, Options{EtherscanAPIKey: "test-key"})
			if err != nil {
				t.Fatalf("New(%d) returned error: %v", chainID, err)
			}
			if v == nil {
				t.Fatalf("New(%d) returned nil verifier", chainID)
			}
			if got := v.Name(); got != "etherscan" {
				t.Fatalf("New(%d) verifier Name() = %q, want %q", chainID, got, "etherscan")
			}
		})
	}
}

func TestNew_UnknownChainErrors(t *testing.T) {
	_, err := New(999999, Options{EtherscanAPIKey: "test-key"})
	if err == nil {
		t.Fatal("New(999999) expected error, got nil")
	}
	if !strings.Contains(err.Error(), "no block verifier configured") {
		t.Fatalf("New(999999) error = %q, want it to mention 'no block verifier configured'", err.Error())
	}
}

func TestNew_MissingEtherscanKeyErrors(t *testing.T) {
	_, err := New(1, Options{EtherscanAPIKey: ""})
	if err == nil {
		t.Fatal("New(1) with empty key expected error, got nil")
	}
	if !strings.Contains(err.Error(), "etherscan API key required") {
		t.Fatalf("New(1) error = %q, want it to mention 'etherscan API key required'", err.Error())
	}
}

func formatChain(chainID int64) string {
	return "chain " + itoa(chainID)
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
