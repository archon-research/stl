package main

import (
	"strings"
	"testing"
)

const (
	ethereumTopicARN = "arn:aws:sns:eu-west-1:579039992622:stl-sentinelstaging-ethereum-blocks.fifo"
	baseTopicARN     = "arn:aws:sns:eu-west-1:579039992622:stl-sentinelstaging-base-blocks.fifo"

	// The Terraform-generated suffix is what makes these names unguessable, and
	// what ValidateS3BucketForChain deliberately ignores.
	ethereumRawBucket = "stl-sentinelstaging-ethereum-raw-89d540d0"
	baseRawBucket     = "stl-sentinelstaging-base-raw-89d540d0"

	ethereumRPCURL = "https://eth-mainnet.g.alchemy.com/v2"
)

func TestLoadConfig(t *testing.T) {
	// The complete valid environment; each case overrides one key, and an empty
	// value stands for "unset" (env.Require and env.Get both read empty as unset).
	valid := map[string]string{
		"CHAIN_ID":          "1",
		"DEPLOY_ENV":        "staging",
		"AWS_SNS_TOPIC_ARN": ethereumTopicARN,
		"AWS_SNS_ENDPOINT":  "",
		"S3_BUCKET":         ethereumRawBucket,
		"ALCHEMY_API_KEY":   "test-key",
		"ALCHEMY_HTTP_URL":  ethereumRPCURL,
		"REDIS_ADDR":        "",
		"REDIS_PASSWORD":    "",
		"REDIS_KEY_PREFIX":  "",
	}

	tests := []struct {
		name            string
		override        map[string]string
		want            config
		wantErrContains string
	}{
		{
			name: "defaults fill the optional knobs",
			want: config{
				chainID:        1,
				deployEnv:      "staging",
				snsTopicARN:    ethereumTopicARN,
				s3Bucket:       ethereumRawBucket,
				rpcURL:         ethereumRPCURL + "/test-key",
				redisAddr:      defaultRedisAddr,
				redisKeyPrefix: defaultRedisKeyPrefix,
				enableTraces:   true,
			},
		},
		{
			name: "an L2 republishes no traces, matching its watcher",
			override: map[string]string{
				"CHAIN_ID":          "8453",
				"AWS_SNS_TOPIC_ARN": baseTopicARN,
				"S3_BUCKET":         baseRawBucket,
				"ALCHEMY_HTTP_URL":  "https://base-mainnet.g.alchemy.com/v2",
			},
			want: config{
				chainID:        8453,
				deployEnv:      "staging",
				snsTopicARN:    baseTopicARN,
				s3Bucket:       baseRawBucket,
				rpcURL:         "https://base-mainnet.g.alchemy.com/v2/test-key",
				redisAddr:      defaultRedisAddr,
				redisKeyPrefix: defaultRedisKeyPrefix,
			},
		},
		{
			name:     "the endpoint overrides point at LocalStack",
			override: map[string]string{"AWS_SNS_ENDPOINT": "http://localstack:4566", "REDIS_ADDR": "redis:6379", "REDIS_PASSWORD": "hunter2", "REDIS_KEY_PREFIX": "isolated"},
			want: config{
				chainID:        1,
				deployEnv:      "staging",
				snsTopicARN:    ethereumTopicARN,
				snsEndpoint:    "http://localstack:4566",
				s3Bucket:       ethereumRawBucket,
				rpcURL:         ethereumRPCURL + "/test-key",
				redisAddr:      "redis:6379",
				redisPassword:  "hunter2",
				redisKeyPrefix: "isolated",
				enableTraces:   true,
			},
		},
		{
			name:            "an absent chain id",
			override:        map[string]string{"CHAIN_ID": ""},
			wantErrContains: "CHAIN_ID",
		},
		{
			name:            "a chain with no declared block-data shape",
			override:        map[string]string{"CHAIN_ID": "999999"},
			wantErrContains: "999999",
		},
		{
			name:            "an absent deploy environment leaves the topic guard blind",
			override:        map[string]string{"DEPLOY_ENV": ""},
			wantErrContains: "DEPLOY_ENV",
		},
		{
			name:            "an absent topic",
			override:        map[string]string{"AWS_SNS_TOPIC_ARN": ""},
			wantErrContains: "AWS_SNS_TOPIC_ARN",
		},
		{
			name:            "another chain's topic",
			override:        map[string]string{"AWS_SNS_TOPIC_ARN": baseTopicARN},
			wantErrContains: "sns topic",
		},
		{
			name:            "another environment's topic",
			override:        map[string]string{"DEPLOY_ENV": "prod"},
			wantErrContains: "sns topic",
		},
		{
			name:            "an absent archive bucket leaves no version to derive",
			override:        map[string]string{"S3_BUCKET": ""},
			wantErrContains: "S3_BUCKET",
		},
		{
			name:            "another chain's archive bucket",
			override:        map[string]string{"S3_BUCKET": baseRawBucket},
			wantErrContains: "bucket",
		},
		{
			name:            "another environment's archive bucket",
			override:        map[string]string{"S3_BUCKET": "stl-sentinelprod-ethereum-raw-89d540d0"},
			wantErrContains: "bucket",
		},
		{
			name:            "an absent alchemy key is a hard error, never an unauthenticated URL",
			override:        map[string]string{"ALCHEMY_API_KEY": ""},
			wantErrContains: "ALCHEMY_API_KEY",
		},
		{
			// A default would hand a non-mainnet deployment mainnet blocks for its
			// heights and publish them on its own chain's topic.
			name:            "an absent node URL is refused rather than defaulted to mainnet",
			override:        map[string]string{"ALCHEMY_HTTP_URL": ""},
			wantErrContains: "ALCHEMY_HTTP_URL",
		},
		{
			// Presence is all this binary checks: an operator who points an L2 at a
			// mainnet URL gets exactly what the ConfigMap says, and the SNS topic and
			// bucket guards are what catch a cross-chain deployment.
			name: "a chain pointed at another chain's node is passed through",
			override: map[string]string{
				"CHAIN_ID":          "8453",
				"AWS_SNS_TOPIC_ARN": baseTopicARN,
				"S3_BUCKET":         baseRawBucket,
			},
			want: config{
				chainID:        8453,
				deployEnv:      "staging",
				snsTopicARN:    baseTopicARN,
				s3Bucket:       baseRawBucket,
				rpcURL:         ethereumRPCURL + "/test-key",
				redisAddr:      defaultRedisAddr,
				redisKeyPrefix: defaultRedisKeyPrefix,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range valid {
				t.Setenv(key, value)
			}
			for key, value := range tc.override {
				t.Setenv(key, value)
			}

			got, err := loadConfig()

			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("error = %v, want one mentioning %q", err, tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadConfig: %v", err)
			}
			if got != tc.want {
				t.Errorf("config = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// The task queue is also the OTel service name the vector-cronjobs alerts select
// by and the name of this chain's Deployment, so all three read the same string.
// Ethereum's worker keeps the unprefixed name every other Ethereum service has.
func TestTaskQueueName(t *testing.T) {
	tests := []struct {
		name            string
		chainID         string
		want            string
		wantErrContains string
	}{
		{name: "ethereum", chainID: "1", want: "block-republisher"},
		{name: "arbitrum", chainID: "42161", want: "arbitrum-block-republisher"},
		{name: "avalanche", chainID: "43114", want: "avalanche-block-republisher"},
		{name: "base", chainID: "8453", want: "base-block-republisher"},
		{name: "optimism", chainID: "10", want: "optimism-block-republisher"},
		{name: "unichain", chainID: "130", want: "unichain-block-republisher"},
		{name: "a chain with no name would poll a queue no operator can find", chainID: "999999", wantErrContains: "999999"},
		{name: "an absent chain id", chainID: "", wantErrContains: "CHAIN_ID"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CHAIN_ID", tc.chainID)

			got, err := taskQueueName()

			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("error = %v, want one mentioning %q", err, tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("taskQueueName() error = %v", err)
			}
			if got != tc.want {
				t.Errorf("taskQueueName() = %q, want %q", got, tc.want)
			}
		})
	}
}
