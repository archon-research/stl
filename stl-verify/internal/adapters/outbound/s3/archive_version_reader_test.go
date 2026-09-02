package s3

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	archiveBucket = "stl-sentinelstaging-ethereum-raw-89d540d0"
	archiveHeight = int64(25395651)
)

// listingOf answers one ListObjectsV2 page of the given keys, recording the
// bucket and prefix it was asked for.
func listingOf(keys ...string) (*mockS3API, *s3.ListObjectsV2Input) {
	var seen s3.ListObjectsV2Input
	mock := &mockS3API{
		listObjectsV2Func: func(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			seen = *params
			contents := make([]types.Object, 0, len(keys))
			for _, key := range keys {
				contents = append(contents, types.Object{Key: aws.String(key)})
			}
			return &s3.ListObjectsV2Output{Contents: contents}, nil
		},
	}
	return mock, &seen
}

func newArchiveVersions(client s3API) *ArchiveVersionReader {
	reader := &Reader{client: client, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return NewArchiveVersionReader(reader, archiveBucket)
}

// The prefix is what keeps the listing to one height on a bucket holding
// millions of objects.
func TestArchiveVersionReader_ListsTheHeightsOwnPrefixOnly(t *testing.T) {
	mock, seen := listingOf()

	if _, _, err := newArchiveVersions(mock).HighestVersion(context.Background(), archiveHeight); err != nil {
		t.Fatalf("HighestVersion: %v", err)
	}

	if got := aws.ToString(seen.Bucket); got != archiveBucket {
		t.Errorf("listed bucket %q, want %q", got, archiveBucket)
	}
	if got, want := aws.ToString(seen.Prefix), "25395000-25395999/25395651_"; got != want {
		t.Errorf("listed prefix %q, want %q", got, want)
	}
}

func TestArchiveVersionReader_ReportsWhatTheArchiveHoldsAtTheHeight(t *testing.T) {
	tests := []struct {
		name        string
		keys        []string
		wantVersion int
		wantFound   bool
	}{
		{name: "a height the archive never received"},
		{
			name:      "the live version alone",
			keys:      []string{"25395000-25395999/25395651_0_block.json.gz"},
			wantFound: true,
		},
		{
			name: "a correction that only got its block object written",
			keys: []string{
				"25395000-25395999/25395651_0_block.json.gz",
				"25395000-25395999/25395651_0_receipts.json.gz",
				"25395000-25395999/25395651_1_block.json.gz",
			},
			wantVersion: 1,
			wantFound:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock, _ := listingOf(tc.keys...)

			version, found, err := newArchiveVersions(mock).HighestVersion(context.Background(), archiveHeight)

			if err != nil {
				t.Fatalf("HighestVersion: %v", err)
			}
			if found != tc.wantFound {
				t.Fatalf("found = %v, want %v", found, tc.wantFound)
			}
			if found && version != tc.wantVersion {
				t.Errorf("version = %d, want %d", version, tc.wantVersion)
			}
		})
	}
}

// A throttled listing says nothing about the height, so it must reach the caller
// as a failure rather than as an empty archive — which would republish over an
// occupied slot.
func TestArchiveVersionReader_SurfacesAFailedListing(t *testing.T) {
	mock := &mockS3API{
		listObjectsV2Func: func(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return nil, errors.New("503 SlowDown")
		},
	}

	_, found, err := newArchiveVersions(mock).HighestVersion(context.Background(), archiveHeight)

	if err == nil {
		t.Fatal("HighestVersion succeeded against a failing listing")
	}
	if found {
		t.Error("reported the archive as read despite the failure")
	}
}

// The worker probes at startup rather than discovering a missing s3:ListBucket
// grant on the first repair, half an hour into a run.
func TestArchiveVersionReader_PingListsOneKeyFromTheBucket(t *testing.T) {
	mock, seen := listingOf("25395000-25395999/25395651_0_block.json.gz")

	if err := newArchiveVersions(mock).Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	if got := aws.ToString(seen.Bucket); got != archiveBucket {
		t.Errorf("probed bucket %q, want %q", got, archiveBucket)
	}
	if got := aws.ToInt32(seen.MaxKeys); got != 1 {
		t.Errorf("probed with MaxKeys %d, want 1", got)
	}
	if seen.Prefix != nil {
		t.Errorf("probed prefix %q, want the bucket itself", aws.ToString(seen.Prefix))
	}
}

func TestArchiveVersionReader_PingSurfacesADeniedListing(t *testing.T) {
	mock := &mockS3API{
		listObjectsV2Func: func(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return nil, errors.New("AccessDenied: User is not authorized to perform: s3:ListBucket")
		},
	}

	err := newArchiveVersions(mock).Ping(context.Background())

	if err == nil {
		t.Fatal("Ping succeeded against a bucket it cannot list")
	}
	if !strings.Contains(err.Error(), archiveBucket) {
		t.Errorf("error = %v, want it to name the bucket", err)
	}
}
