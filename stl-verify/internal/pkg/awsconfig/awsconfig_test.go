package awsconfig

import (
	"context"
	"strings"
	"testing"
)

func TestLoad_RegionFromOpts(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-2")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	cfg, err := Load(context.Background(), Options{Region: "ap-southeast-1", DefaultRegion: "eu-west-1"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Region != "ap-southeast-1" {
		t.Fatalf("Region = %q, want %q", cfg.Region, "ap-southeast-1")
	}
}

func TestLoad_RegionFromEnv(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-2")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	cfg, err := Load(context.Background(), Options{DefaultRegion: "eu-west-1"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Region != "us-east-2" {
		t.Fatalf("Region = %q, want %q", cfg.Region, "us-east-2")
	}
}

func TestLoad_RegionDefault(t *testing.T) {
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	cfg, err := Load(context.Background(), Options{DefaultRegion: "eu-west-1"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Region != "eu-west-1" {
		t.Fatalf("Region = %q, want %q", cfg.Region, "eu-west-1")
	}
}

func TestLoad_RegionMissing(t *testing.T) {
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	_, err := Load(context.Background(), Options{})
	if err == nil {
		t.Fatal("Load: expected error when no region resolved, got nil")
	}
	if !strings.Contains(err.Error(), "no region resolved") {
		t.Fatalf("Load: error = %q, want substring %q", err.Error(), "no region resolved")
	}
}

func TestLoad_Endpoint(t *testing.T) {
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	const ep = "http://localhost:4566"
	cfg, err := Load(context.Background(), Options{DefaultRegion: "eu-west-1", Endpoint: ep})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.BaseEndpoint == nil {
		t.Fatal("BaseEndpoint = nil, want non-nil")
	}
	if got := *cfg.BaseEndpoint; got != ep {
		t.Fatalf("BaseEndpoint = %q, want %q", got, ep)
	}
}

func TestLoad_StaticCredsFromEnv(t *testing.T) {
	tests := []struct {
		name        string
		accessKeyID string
		secretKey   string
		wantAKID    string
		wantSecret  string
		wantSource  string
	}{
		{
			name:        "static creds when env set",
			accessKeyID: "AKIA-test",
			secretKey:   "secret-test",
			wantAKID:    "AKIA-test",
			wantSecret:  "secret-test",
			wantSource:  "StaticCredentials",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AWS_REGION", "eu-west-1")
			t.Setenv("AWS_ACCESS_KEY_ID", tc.accessKeyID)
			t.Setenv("AWS_SECRET_ACCESS_KEY", tc.secretKey)

			cfg, err := Load(context.Background(), Options{StaticCredentialsFromEnv: true})
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.Credentials == nil {
				t.Fatal("Credentials provider is nil")
			}
			creds, err := cfg.Credentials.Retrieve(context.Background())
			if err != nil {
				t.Fatalf("Retrieve: %v", err)
			}
			if creds.AccessKeyID != tc.wantAKID {
				t.Errorf("AccessKeyID = %q, want %q", creds.AccessKeyID, tc.wantAKID)
			}
			if creds.SecretAccessKey != tc.wantSecret {
				t.Errorf("SecretAccessKey = %q, want %q", creds.SecretAccessKey, tc.wantSecret)
			}
			if creds.Source != tc.wantSource {
				t.Errorf("Source = %q, want %q", creds.Source, tc.wantSource)
			}
		})
	}
}

// When StaticCredentialsFromEnv is true but AWS_ACCESS_KEY_ID is unset, Load
// must still build the cfg successfully — the default credential chain takes
// over. Credentials retrieval itself may fail in a CI environment without IAM
// roles or ~/.aws files, so we only assert that the cfg builds.
func TestLoad_StaticCredsFromEnv_FallsThroughWhenUnset(t *testing.T) {
	t.Setenv("AWS_REGION", "eu-west-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	cfg, err := Load(context.Background(), Options{StaticCredentialsFromEnv: true})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Region != "eu-west-1" {
		t.Fatalf("Region = %q, want %q", cfg.Region, "eu-west-1")
	}
}

func TestLoad_StaticCredsFromEnv_MissingSecret(t *testing.T) {
	t.Setenv("AWS_REGION", "eu-west-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKID")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	_, err := Load(context.Background(), Options{StaticCredentialsFromEnv: true, DefaultRegion: "us-east-1"})
	if err == nil {
		t.Fatal("Load: expected error when AWS_ACCESS_KEY_ID is set but AWS_SECRET_ACCESS_KEY is empty, got nil")
	}
	const want = "awsconfig: AWS_ACCESS_KEY_ID is set but AWS_SECRET_ACCESS_KEY is empty"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Load: error = %q, want substring %q", err.Error(), want)
	}
}
