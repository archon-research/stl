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
		"ALCHEMY_HTTP_URL":  "",
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
				rpcURL:         defaultAlchemyHTTPURL + "/test-key",
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
				rpcURL:         defaultAlchemyHTTPURL + "/test-key",
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
