package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	progressLogEvery    = int64(1000)
	progressLogInterval = 5 * time.Second
)

// s3Lister is the narrow subset of *s3.Client used for paginated listing.
// *s3.Client satisfies this interface, so the production wiring needs no adapter.
type s3Lister interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// streamKeysFromFile reads one S3 key per line from path and emits each key on
// out as it is read. Blank lines and lines whose first non-whitespace character
// is '#' are skipped. Leading and trailing whitespace is trimmed. When
// limit > 0 the producer stops after limit keys have been emitted.
//
// Before emitting, the file is scanned once to count emit-eligible lines so
// sum.total reflects the upper bound the operator should expect. The file is
// small enough (operator-managed inventory) that a second pass is cheap.
//
// The function blocks on each send so the channel's capacity acts as
// natural backpressure for the consumer. Returns the first error encountered
// (open/scan) or ctx.Err() on cancellation. The caller is responsible for
// closing out when this function returns.
func streamKeysFromFile(ctx context.Context, path string, limit int, sum *summary, out chan<- string) error {
	total, err := countEligibleLines(path)
	if err != nil {
		return err
	}
	if limit > 0 && total > int64(limit) {
		total = int64(limit)
	}
	if sum != nil {
		sum.total.Store(total)
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open keys file %s: %w", path, err)
	}
	defer f.Close()

	logger := slog.Default()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var emitted int64
	for scanner.Scan() {
		line, ok := eligibleLine(scanner.Text())
		if !ok {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- line:
		}
		emitted++
		if limit > 0 && emitted >= int64(limit) {
			logger.Info("limit reached", "source", "file", "limit", limit, "emitted", emitted)
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan keys file %s: %w", path, err)
	}
	return nil
}

// eligibleLine trims whitespace and returns (trimmed, true) if the line is a
// real key entry, or ("", false) for blank lines and lines whose first non-
// whitespace character is '#'. The pre-count and emit passes both use this so
// any drift in the eligibility rule stays in one place.
func eligibleLine(raw string) (string, bool) {
	line := strings.TrimSpace(raw)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", false
	}
	return line, true
}

// countEligibleLines counts non-blank, non-comment lines in path. The buffer
// settings match streamKeysFromFile so a line that scans-OK there scans-OK
// here.
func countEligibleLines(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open keys file %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var n int64
	for scanner.Scan() {
		if _, ok := eligibleLine(scanner.Text()); !ok {
			continue
		}
		n++
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("scan keys file %s: %w", path, err)
	}
	return n, nil
}

// streamKeysFromBucket paginates ListObjectsV2 on bucket/prefix and emits each
// object key whose Size <= maxSize on out as soon as the page returns. The
// function deliberately does NOT buffer pages: it sends keys one at a time on
// out (blocking when out is full) so the listing throttles to the consumer's
// speed, and workers can start refilling while later pages are still being
// fetched. When limit > 0 the producer stops after limit candidates have been
// emitted (pagination is not advanced past the page that hits the cap).
//
// sum.total is incremented as each candidate is emitted so an operator
// watching the progress log sees the running upper bound climb during the
// scan and stabilise once listing completes.
//
// Returns ctx.Err() on cancellation, the wrapped SDK error on failure, or nil
// when the listing is exhausted. The caller is responsible for closing out
// when this function returns. Periodic progress is logged to slog.Default()
// every 5s or 1000 listed objects, whichever comes first.
func streamKeysFromBucket(ctx context.Context, lister s3Lister, bucket, prefix string, maxSize int64, limit int, sum *summary, out chan<- string) error {
	paginator := s3.NewListObjectsV2Paginator(lister, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	logger := slog.Default()
	var (
		listed     int64
		candidates int64
		lastLogAt  = time.Now()
	)

	for paginator.HasMorePages() {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := paginator.NextPage(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return fmt.Errorf("list objects bucket=%s prefix=%s: %w", bucket, prefix, err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			listed++
			if listed%progressLogEvery == 0 || time.Since(lastLogAt) >= progressLogInterval {
				logger.Info("scan progress",
					"bucket", bucket,
					"prefix", prefix,
					"listed", listed,
					"candidates", candidates,
				)
				lastLogAt = time.Now()
			}
			if aws.ToInt64(obj.Size) > maxSize {
				continue
			}
			candidates++
			if sum != nil {
				sum.total.Add(1)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- aws.ToString(obj.Key):
			}
			if limit > 0 && candidates >= int64(limit) {
				logger.Info("limit reached",
					"source", "bucket",
					"bucket", bucket,
					"prefix", prefix,
					"limit", limit,
					"candidates", candidates,
				)
				return nil
			}
		}
	}

	logger.Info("scan complete",
		"bucket", bucket,
		"prefix", prefix,
		"listed", listed,
		"candidates", candidates,
	)
	return nil
}
