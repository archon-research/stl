package chainutil

import (
	"context"
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

func TestValidateS3BucketForChain(t *testing.T) {
	tests := []struct {
		name        string
		chainID     int64
		bucket      string
		environment string
		wantErr     bool
		errContains string
	}{
		{
			name:        "ethereum mainnet valid bucket",
			chainID:     1,
			bucket:      "stl-sentinelstaging-ethereum-raw",
			environment: "staging",
			wantErr:     false,
		},
		{
			name:        "ethereum mainnet with suffix",
			chainID:     1,
			bucket:      "stl-sentinelstaging-ethereum-raw-89d540d0",
			environment: "staging",
			wantErr:     false,
		},
		{
			name:        "avalanche valid bucket",
			chainID:     43114,
			bucket:      "stl-sentinelstaging-avalanche-raw",
			environment: "staging",
			wantErr:     false,
		},
		{
			name:        "ethereum chain ID with avalanche bucket",
			chainID:     1,
			bucket:      "stl-sentinelstaging-avalanche-raw",
			environment: "staging",
			wantErr:     true,
			errContains: "does not have expected prefix",
		},
		{
			name:        "avalanche chain ID with ethereum bucket",
			chainID:     43114,
			bucket:      "stl-sentinelstaging-ethereum-raw",
			environment: "staging",
			wantErr:     true,
			errContains: "does not have expected prefix",
		},
		{
			name:        "unknown chain ID",
			chainID:     999999,
			bucket:      "stl-sentinelstaging-ethereum-raw",
			environment: "staging",
			wantErr:     true,
			errContains: "unknown chain ID",
		},
		{
			name:        "case insensitive match",
			chainID:     1,
			bucket:      "stl-sentinelstaging-ETHEREUM-raw",
			environment: "staging",
			wantErr:     false,
		},
		{
			name:        "empty bucket name",
			chainID:     1,
			bucket:      "",
			environment: "staging",
			wantErr:     true,
			errContains: "does not have expected prefix",
		},
		{
			name:        "substring-only chain token should fail",
			chainID:     1,
			bucket:      "stl-sentinelstaging-notethereumish-raw",
			environment: "staging",
			wantErr:     true,
			errContains: "does not have expected prefix",
		},
		{
			name:        "empty environment",
			chainID:     1,
			bucket:      "stl-sentinelstaging-ethereum-raw",
			environment: "",
			wantErr:     true,
			errContains: "environment must not be empty",
		},
		{
			name:        "prod environment",
			chainID:     1,
			bucket:      "stl-sentinelprod-ethereum-raw",
			environment: "prod",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateS3BucketForChain(tt.chainID, tt.bucket, tt.environment)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateS3BucketForChain() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateS3BucketForChain() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("ValidateS3BucketForChain() unexpected error = %v", err)
			}
		})
	}
}

func TestValidateSNSTopicForChain(t *testing.T) {
	tests := []struct {
		name        string
		chainID     int64
		topicARN    string
		environment string
		wantErr     bool
		errContains string
	}{
		{
			name:        "ethereum staging valid",
			chainID:     1,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelstaging-ethereum-blocks.fifo",
			environment: "staging",
		},
		{
			name:        "avalanche staging valid",
			chainID:     43114,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelstaging-avalanche-blocks.fifo",
			environment: "staging",
		},
		{
			name:        "prod environment valid",
			chainID:     1,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelprod-ethereum-blocks.fifo",
			environment: "prod",
		},
		{
			name:        "ethereum chain ID with avalanche topic — must error",
			chainID:     1,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelstaging-avalanche-blocks.fifo",
			environment: "staging",
			wantErr:     true,
			errContains: "does not have expected suffix",
		},
		{
			name:        "staging chain with prod topic — must error",
			chainID:     1,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelprod-ethereum-blocks.fifo",
			environment: "staging",
			wantErr:     true,
			errContains: "does not have expected suffix",
		},
		{
			name:        "unknown chain ID",
			chainID:     999999,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelstaging-ethereum-blocks.fifo",
			environment: "staging",
			wantErr:     true,
			errContains: "unknown chain ID",
		},
		{
			name:        "empty environment",
			chainID:     1,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelstaging-ethereum-blocks.fifo",
			environment: "",
			wantErr:     true,
			errContains: "environment must not be empty",
		},
		{
			name:        "case-insensitive ARN match",
			chainID:     1,
			topicARN:    "ARN:AWS:SNS:EU-WEST-1:123456789012:STL-SENTINELSTAGING-ETHEREUM-BLOCKS.FIFO",
			environment: "staging",
		},
		{
			name:        "non-FIFO topic — must error",
			chainID:     1,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelstaging-ethereum-blocks",
			environment: "staging",
			wantErr:     true,
			errContains: "does not have expected suffix",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSNSTopicForChain(tt.chainID, tt.topicARN, tt.environment)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateSNSTopicForChain() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateSNSTopicForChain() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("ValidateSNSTopicForChain() unexpected error = %v", err)
			}
		})
	}
}

func TestEnvironmentFromBucket(t *testing.T) {
	tests := []struct {
		name        string
		bucket      string
		wantEnv     string
		wantErr     bool
		errContains string
	}{
		{name: "staging bare", bucket: "stl-sentinelstaging-ethereum-raw", wantEnv: "staging"},
		{name: "staging with suffix", bucket: "stl-sentinelstaging-ethereum-raw-89d540d0", wantEnv: "staging"},
		{name: "prod", bucket: "stl-sentinelprod-avalanche-raw", wantEnv: "prod"},
		{name: "case insensitive", bucket: "STL-SENTINELSTAGING-ethereum-raw", wantEnv: "staging"},
		{name: "wrong prefix", bucket: "my-test-bucket", wantErr: true, errContains: "does not start with"},
		{name: "no chain segment", bucket: "stl-sentinelstaging", wantErr: true, errContains: "malformed"},
		{name: "empty", bucket: "", wantErr: true, errContains: "does not start with"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := EnvironmentFromBucket(tt.bucket)
			if tt.wantErr {
				if err == nil {
					t.Errorf("EnvironmentFromBucket() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("EnvironmentFromBucket() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("EnvironmentFromBucket() unexpected error = %v", err)
				return
			}
			if env != tt.wantEnv {
				t.Errorf("EnvironmentFromBucket() = %q, want %q", env, tt.wantEnv)
			}
		})
	}
}

// Every chain the repo watches needs a declared block-data shape: a tool that
// re-publishes a block reads this to decide what to fetch, and refuses to start
// on a chain the map does not answer for.
func TestDefaultChainExpectations_CoversEveryKnownChain(t *testing.T) {
	expectations := DefaultChainExpectations()

	for chainID, name := range entity.ChainIDToS3Bucket {
		if _, ok := expectations[chainID]; !ok {
			t.Errorf("chain %d (%s) has no declared block-data expectation", chainID, name)
		}
	}
	for chainID := range expectations {
		if _, ok := entity.ChainIDToS3Bucket[chainID]; !ok {
			t.Errorf("chain %d is declared here but is not a chain the repo watches", chainID)
		}
	}
}

func TestAlchemyRPCURL(t *testing.T) {
	tests := []struct {
		name    string
		chainID int64
		apiKey  string
		httpURL string
		want    string
		wantErr string
	}{
		{
			name:    "mainnet falls back to the built-in endpoint",
			chainID: 1,
			apiKey:  "secret",
			want:    "https://eth-mainnet.g.alchemy.com/v2/secret",
		},
		{
			name:    "an explicit endpoint wins on mainnet",
			chainID: 1,
			apiKey:  "secret",
			httpURL: "https://eth-sepolia.g.alchemy.com/v2",
			want:    "https://eth-sepolia.g.alchemy.com/v2/secret",
		},
		{
			name:    "a trailing slash does not double up before the key",
			chainID: 8453,
			apiKey:  "secret",
			httpURL: "https://base-mainnet.g.alchemy.com/v2/",
			want:    "https://base-mainnet.g.alchemy.com/v2/secret",
		},
		{
			name:    "a remote cleartext endpoint is rejected before adding the key",
			chainID: 1,
			apiKey:  "secret",
			httpURL: "http://alchemy.example.com/v2",
			wantErr: "ALCHEMY_HTTP_URL must use HTTPS",
		},
		{
			name:    "a loopback endpoint may use HTTP for local RPC fixtures",
			chainID: 1,
			apiKey:  "secret",
			httpURL: "http://127.0.0.1:8545/v2",
			want:    "http://127.0.0.1:8545/v2/secret",
		},
		{
			name:    "an endpoint without a host is rejected during configuration",
			chainID: 1,
			apiKey:  "secret",
			httpURL: "https:alchemy.example.com/v2",
			wantErr: "ALCHEMY_HTTP_URL must be an absolute URL",
		},
		{
			name:    "an endpoint with only a port is rejected during configuration",
			chainID: 1,
			apiKey:  "secret",
			httpURL: "https://:443/v2",
			wantErr: "ALCHEMY_HTTP_URL must be an absolute URL",
		},
		{
			name:    "a query cannot capture the API key",
			chainID: 1,
			apiKey:  "secret",
			httpURL: "https://alchemy.example.com/v2?network=mainnet",
			wantErr: "ALCHEMY_HTTP_URL must not contain user info, a query, or a fragment",
		},
		{
			name:    "a fragment cannot capture the API key",
			chainID: 1,
			apiKey:  "secret",
			httpURL: "https://alchemy.example.com/v2#rpc",
			wantErr: "ALCHEMY_HTTP_URL must not contain user info, a query, or a fragment",
		},
		{
			name:    "credentials are not accepted in the endpoint",
			chainID: 1,
			apiKey:  "secret",
			httpURL: "https://user:password@alchemy.example.com/v2",
			wantErr: "ALCHEMY_HTTP_URL must not contain user info, a query, or a fragment",
		},
		{
			name:    "a non-mainnet chain must name its endpoint",
			chainID: 8453,
			apiKey:  "secret",
			wantErr: "ALCHEMY_HTTP_URL is required for chain 8453",
		},
		{
			name:    "an absent key is a hard error, never an unauthenticated URL",
			chainID: 1,
			httpURL: "https://eth-mainnet.g.alchemy.com/v2",
			wantErr: "requiring ALCHEMY_API_KEY",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ALCHEMY_API_KEY", tc.apiKey)
			t.Setenv("ALCHEMY_HTTP_URL", tc.httpURL)

			got, err := AlchemyRPCURL(tc.chainID)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one mentioning %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("AlchemyRPCURL: %v", err)
			}
			if got != tc.want {
				t.Errorf("url = %q, want %q", got, tc.want)
			}
		})
	}
}

type chainIDReaderFunc func(ctx context.Context) (*big.Int, error)

func (f chainIDReaderFunc) ChainID(ctx context.Context) (*big.Int, error) { return f(ctx) }

func TestAssertChainID(t *testing.T) {
	tests := []struct {
		name    string
		node    *big.Int
		nodeErr error
		want    int64
		wantErr string
	}{
		{name: "the node agrees with the config", node: big.NewInt(8453), want: 8453},
		{
			name:    "the node is on another chain",
			node:    big.NewInt(1),
			want:    8453,
			wantErr: "RPC chain ID mismatch: RPC reports 1, config says 8453",
		},
		{
			name:    "a chain id beyond int64 is a mismatch, not a truncated match",
			node:    new(big.Int).Add(big.NewInt(math.MaxInt64), big.NewInt(2)),
			want:    1,
			wantErr: "RPC chain ID mismatch",
		},
		{
			name:    "an unreachable node is reported as such",
			nodeErr: errors.New("connection refused"),
			want:    1,
			wantErr: "fetching RPC chain ID: connection refused",
		},
		{
			name:    "a nil answer is a mismatch, not a panic",
			want:    1,
			wantErr: "RPC chain ID mismatch",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := chainIDReaderFunc(func(context.Context) (*big.Int, error) { return tc.node, tc.nodeErr })

			err := AssertChainID(context.Background(), node, tc.want)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("AssertChainID: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want one mentioning %q", err, tc.wantErr)
			}
		})
	}
}

func TestAssertChainID_BoundsTheProbeWithItsOwnDeadline(t *testing.T) {
	started := time.Now()
	var deadline time.Time
	node := chainIDReaderFunc(func(ctx context.Context) (*big.Int, error) {
		deadline, _ = ctx.Deadline()
		return big.NewInt(1), nil
	})

	if err := AssertChainID(context.Background(), node, 1); err != nil {
		t.Fatalf("AssertChainID: %v", err)
	}
	if deadline.IsZero() {
		t.Fatal("the probe ran without a deadline; a sick node would hold startup for the dialer's whole retry budget")
	}
	if got := deadline.Sub(started); got < chainIDProbeTimeout-time.Second || got > chainIDProbeTimeout+time.Second {
		t.Fatalf("probe deadline = %s from start, want approximately %s", got, chainIDProbeTimeout)
	}
}
