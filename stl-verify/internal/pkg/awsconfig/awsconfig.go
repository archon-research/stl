// Package awsconfig builds an aws.Config from a small Options struct so cmd/*
// entry points don't each reimplement the LocalStack-friendly static-creds
// fallback. See Load for behaviour.
package awsconfig

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// Options controls how Load builds an aws.Config.
type Options struct {
	// Region overrides AWS_REGION. If empty, AWS_REGION env is read, then DefaultRegion.
	Region string

	// DefaultRegion is used when both Region and AWS_REGION are empty.
	// If both this and AWS_REGION are empty Load returns an error.
	DefaultRegion string

	// Endpoint, when non-empty, overrides the base endpoint for all clients
	// built from the returned config (LocalStack-style).
	Endpoint string

	// StaticCredentialsFromEnv enables the LocalStack-friendly fallback:
	// when AWS_ACCESS_KEY_ID is set in the env, use static credentials from
	// AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY. When AWS_ACCESS_KEY_ID is
	// unset, the AWS SDK's default credential chain is used (IAM role,
	// instance profile, ~/.aws/credentials, etc.).
	StaticCredentialsFromEnv bool
}

// Load returns an aws.Config matching opts. Returns an error if no region is
// resolvable from opts or env.
func Load(ctx context.Context, opts Options) (aws.Config, error) {
	region := opts.Region
	if region == "" {
		region = os.Getenv("AWS_REGION")
	}
	if region == "" {
		region = opts.DefaultRegion
	}
	if region == "" {
		return aws.Config{}, fmt.Errorf("awsconfig: no region resolved (set opts.Region, opts.DefaultRegion, or AWS_REGION)")
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
	}

	if opts.Endpoint != "" {
		loadOpts = append(loadOpts, awsconfig.WithBaseEndpoint(opts.Endpoint))
	}

	if opts.StaticCredentialsFromEnv {
		if accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID"); accessKeyID != "" {
			secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
			if secretKey == "" {
				return aws.Config{}, fmt.Errorf("awsconfig: AWS_ACCESS_KEY_ID is set but AWS_SECRET_ACCESS_KEY is empty")
			}
			loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, ""),
			))
		}
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("awsconfig: load default: %w", err)
	}
	return cfg, nil
}
