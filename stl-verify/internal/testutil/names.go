package testutil

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

// Limits the services impose on the names tests hand them.
const (
	maxIdentifierLen = 63 // PostgreSQL identifier
	maxBucketLen     = 63 // S3 bucket
	maxQueueLen      = 80 // SQS queue
)

const fifoSuffix = ".fifo"

// SanitizeTestName converts a test name to a string safe for use as a
// PostgreSQL identifier, Redis key prefix, or AWS resource name suffix: lowercase
// alphanumeric and underscores, prefixed with "t_" and scoped by withProcessTag.
//
// Go keeps test names distinct, but this mapping does not: every rune outside the
// safe set becomes "_", so "TestA/B_C" and "TestA_B/C" arrive as one string. A
// digest of the name as given is what keeps them on separate resources.
func SanitizeTestName(testName string) string {
	s := strings.ToLower(testName)
	var b strings.Builder
	b.WriteString("t_")
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	b.WriteByte('_')
	b.WriteString(shortDigest(testName))

	return withProcessTag(b.String())
}

// S3TestBucketName builds a bucket name unique to the calling test, for suites
// that share one LocalStack container and so cannot share a bucket.
func S3TestBucketName(t *testing.T, prefix string) string {
	t.Helper()

	name := fitName(prefix+strings.ReplaceAll(SanitizeTestName(t.Name()), "_", "-"), maxBucketLen)
	// TrimRight, not TrimSuffix: truncation can land mid-run of separators, and
	// S3 rejects a name that does not end in a letter or digit.
	return strings.TrimRight(name, "-.")
}

// SQSTestFifoQueueName builds a FIFO queue name unique to the calling test, for
// suites that share one LocalStack container and so cannot share a queue.
func SQSTestFifoQueueName(t *testing.T, prefix string) string {
	t.Helper()

	// The suffix goes on after fitting: SQS requires a FIFO name to end in it, and
	// counts it against the same limit.
	return fitName(prefix+SanitizeTestName(t.Name()), maxQueueLen-len(fifoSuffix)) + fifoSuffix
}

// withProcessTag scopes a name to this test binary, so two packages cannot address
// — or drop — one another's resources on a server they share.
//
// Neither kind of name is unique on its own: Go guarantees test names are unique
// only within a package, and the names test files pass to SetupDBForMain are
// hand-written. Tagging here is what keeps that from being each caller's problem.
// It separates packages only — one process's own names are settled by shortDigest
// above and by claimMainDBName.
func withProcessTag(name string) string {
	suffix := "_" + processTag()
	return fitName(name, maxIdentifierLen-len(suffix)) + suffix
}

// fitName brings name inside limit, spending the tail on a digest of the whole
// name: plain truncation would put two names sharing a long prefix — sibling
// subtests, or two tests named for the same behaviour — on one resource.
func fitName(name string, limit int) string {
	if len(name) <= limit {
		return name
	}
	return name[:limit-digestLen] + shortDigest(name)
}

// Long enough that no suite collides by accident, short enough to leave a readable
// prefix inside a 63-character identifier.
const digestLen = 8

// shortDigest is the tail that keeps two names apart once whatever produced them —
// truncation, or sanitizing — has collapsed them onto one string.
func shortDigest(name string) string {
	sum := sha256.Sum256([]byte(name))
	return hex.EncodeToString(sum[:digestLen/2])
}
