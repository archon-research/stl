//go:build integration

package main

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	sharedDSN           string
	sharedLocalStackCfg testutil.LocalStackConfig
)

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{
		TimescaleDSN:       &sharedDSN,
		LocalStack:         &sharedLocalStackCfg,
		LocalStackServices: "s3",
	}))
}
