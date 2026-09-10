package testutil

import (
	"strings"
	"testing"
)

// A name long enough to be truncated is the only interesting case: fitting a short
// one is arithmetic, while overrunning an AWS limit is a test that fails to create
// its own fixture.
var overlongTestName = strings.Repeat("Behaviour", 12)

func TestNameHelpers_StayInsideServiceLimits(t *testing.T) {
	t.Run(overlongTestName, func(t *testing.T) {
		for _, tc := range []struct {
			helper string
			got    string
			limit  int
		}{
			{"SanitizeTestName", SanitizeTestName(t.Name()), maxIdentifierLen},
			{"S3TestBucketName", S3TestBucketName(t, "backup-"), maxBucketLen},
			{"SQSTestFifoQueueName", SQSTestFifoQueueName(t, "dlq-producer-"), maxQueueLen},
		} {
			t.Run(tc.helper, func(t *testing.T) {
				if len(tc.got) > tc.limit {
					t.Errorf("%s = %q (%d chars), limit is %d", tc.helper, tc.got, len(tc.got), tc.limit)
				}
			})
		}
	})
}

func TestNameHelpers_KeepTheEndingTheirServiceRequires(t *testing.T) {
	t.Run(overlongTestName, func(t *testing.T) {
		if bucket := S3TestBucketName(t, "backup-"); strings.HasSuffix(bucket, "-") || strings.HasSuffix(bucket, ".") {
			t.Errorf("S3TestBucketName = %q, S3 rejects a name not ending in a letter or digit", bucket)
		}
		if queue := SQSTestFifoQueueName(t, "dlq-producer-"); !strings.HasSuffix(queue, fifoSuffix) {
			t.Errorf("SQSTestFifoQueueName = %q, SQS rejects a FIFO name not ending in %q", queue, fifoSuffix)
		}
	})
}

// Truncation is where two tests named for the same behaviour would otherwise
// collapse onto one database, bucket or queue.
func TestFitName_KeepsNamesSharingAPrefixApart(t *testing.T) {
	const limit = 20
	enabled := fitName("t_reserve_used_as_collateral_enabled", limit)
	disabled := fitName("t_reserve_used_as_collateral_disabled", limit)

	if enabled == disabled {
		t.Errorf("both names fitted to %q", enabled)
	}
	if len(enabled) != limit || len(disabled) != limit {
		t.Errorf("fitted lengths %d and %d, want %d", len(enabled), len(disabled), limit)
	}
}

// Sanitizing maps every rune outside [a-z0-9_] to "_", so two test names Go kept
// distinct can arrive as one string — and one database, bucket or queue.
func TestSanitizeTestName_KeepsNamesThatSanitizeAlikeApart(t *testing.T) {
	slash, underscore := SanitizeTestName("TestA/B_C"), SanitizeTestName("TestA_B/C")

	if slash == underscore {
		t.Errorf("both names sanitized to %q", slash)
	}
}
