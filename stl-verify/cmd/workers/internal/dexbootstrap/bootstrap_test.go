package dexbootstrap

import (
	"context"
	"strings"
	"testing"
)

// N8-1: dexbootstrap.loadAWSConfig defaulted to "us-east-1" while every other
// worker uses awsconfig.DefaultRegion ("eu-west-1") via the shared
// internal/pkg/awsconfig package. The fix is to call awsconfig.Load so the
// default is shared. Test asserts the unset-AWS_REGION default lands on
// eu-west-1, matching every other worker.
func TestLoadAWSConfig_DefaultsToEUWest1WhenAWSRegionUnset(t *testing.T) {
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	cfg, err := loadAWSConfig(context.Background())
	if err != nil {
		t.Fatalf("loadAWSConfig: %v", err)
	}
	if cfg.Region != "eu-west-1" {
		t.Fatalf("Region = %q, want eu-west-1 (matching awsconfig.DefaultRegion; every stl- deployment lives there)", cfg.Region)
	}
}

// N8-2: when AWS_ACCESS_KEY_ID is set but AWS_SECRET_ACCESS_KEY is empty,
// fail loudly rather than silently authenticate with a half-populated set.
// awsconfig.Load already does this; the inline copy in dexbootstrap did not.
func TestLoadAWSConfig_RejectsAKIDWithoutSecret(t *testing.T) {
	t.Setenv("AWS_REGION", "eu-west-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKID-without-secret")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	_, err := loadAWSConfig(context.Background())
	if err == nil {
		t.Fatal("loadAWSConfig: expected error when AWS_ACCESS_KEY_ID is set but AWS_SECRET_ACCESS_KEY is empty")
	}
	if !strings.Contains(err.Error(), "AWS_SECRET_ACCESS_KEY") {
		t.Errorf("error %q must reference AWS_SECRET_ACCESS_KEY so operator can fix the right var", err)
	}
}
