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
// is '#' are skipped. Leading and trailing whitespace is trimmed.
//
// The function blocks on each send so the channel's capacity acts as
// natural backpressure for the consumer. Returns the first error encountered
// (open/scan) or ctx.Err() on cancellation. The caller is responsible for
// closing out when this function returns.
func streamKeysFromFile(ctx context.Context, path string, out chan<- string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open keys file %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- line:
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan keys file %s: %w", path, err)
	}
	return nil
}

// streamKeysFromBucket paginates ListObjectsV2 on bucket/prefix and emits each
// object key whose Size <= maxSize on out as soon as the page returns. The
// function deliberately does NOT buffer pages: it sends keys one at a time on
// out (blocking when out is full) so the listing throttles to the consumer's
// speed, and workers can start refilling while later pages are still being
// fetched.
//
// Returns ctx.Err() on cancellation, the wrapped SDK error on failure, or nil
// when the listing is exhausted. The caller is responsible for closing out
// when this function returns. Periodic progress is logged to slog.Default()
// every 5s or 1000 listed objects, whichever comes first.
func streamKeysFromBucket(ctx context.Context, lister s3Lister, bucket, prefix string, maxSize int64, out chan<- string) error {
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
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- aws.ToString(obj.Key):
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
