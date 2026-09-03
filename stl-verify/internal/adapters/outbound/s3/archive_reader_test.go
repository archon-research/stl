package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
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

func newArchiveReader(client s3API) *ArchiveReader {
	reader := &Reader{client: client, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return NewArchiveReader(reader, archiveBucket)
}

// The prefix is what keeps the listing to one height on a bucket holding
// millions of objects.
func TestArchiveReader_ListsTheHeightsOwnPrefixOnly(t *testing.T) {
	mock, seen := listingOf()

	if _, _, err := newArchiveReader(mock).HighestVersion(context.Background(), archiveHeight); err != nil {
		t.Fatalf("HighestVersion: %v", err)
	}

	if got := aws.ToString(seen.Bucket); got != archiveBucket {
		t.Errorf("listed bucket %q, want %q", got, archiveBucket)
	}
	if got, want := aws.ToString(seen.Prefix), "25395000-25395999/25395651_"; got != want {
		t.Errorf("listed prefix %q, want %q", got, want)
	}
}

func TestArchiveReader_ReportsWhatTheArchiveHoldsAtTheHeight(t *testing.T) {
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

			version, found, err := newArchiveReader(mock).HighestVersion(context.Background(), archiveHeight)

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

// An object under the height's own prefix that carries no version is a slot
// nothing can read, so the height fails rather than being planned around.
func TestArchiveReader_SurfacesAKeyItCannotRead(t *testing.T) {
	mock, _ := listingOf("25395000-25395999/25395651_0_block.json.gz", "25395000-25395999/notes.txt")

	_, _, err := newArchiveReader(mock).HighestVersion(context.Background(), archiveHeight)

	if !errors.Is(err, s3key.ErrUnrecognisedKey) {
		t.Fatalf("error = %v, want ErrUnrecognisedKey", err)
	}
	if !strings.Contains(err.Error(), "notes.txt") {
		t.Errorf("error = %v, want it to name the key", err)
	}
}

// A throttled listing says nothing about the height, so it must reach the caller
// as a failure rather than as an empty archive — which would republish over an
// occupied slot.
func TestArchiveReader_SurfacesAFailedListing(t *testing.T) {
	mock := &mockS3API{
		listObjectsV2Func: func(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return nil, errors.New("503 SlowDown")
		},
	}

	_, found, err := newArchiveReader(mock).HighestVersion(context.Background(), archiveHeight)

	if err == nil {
		t.Fatal("HighestVersion succeeded against a failing listing")
	}
	if found {
		t.Error("reported the archive as read despite the failure")
	}
}

// The worker probes at startup rather than discovering a missing s3:ListBucket
// grant on the first repair, half an hour into a run.
func TestArchiveReader_PingListsOneKeyFromTheBucket(t *testing.T) {
	mock, seen := listingOf("25395000-25395999/25395651_0_block.json.gz")

	if err := newArchiveReader(mock).Ping(context.Background()); err != nil {
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

func TestArchiveReader_PingSurfacesADeniedListing(t *testing.T) {
	mock := &mockS3API{
		listObjectsV2Func: func(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return nil, errors.New("AccessDenied: User is not authorized to perform: s3:ListBucket")
		},
	}

	err := newArchiveReader(mock).Ping(context.Background())

	if err == nil {
		t.Fatal("Ping succeeded against a bucket it cannot list")
	}
	if !strings.Contains(err.Error(), archiveBucket) {
		t.Errorf("error = %v, want it to name the bucket", err)
	}
}

// gzippedBlock is a stored block payload: the adapter reads only its first
// kilobytes, so it must decompress a prefix rather than the whole object.
func gzippedBlock(t *testing.T, hash string) []byte {
	t.Helper()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := fmt.Fprintf(gz, `{"hash":%q,"number":"0x1836b83"}`, hash); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

// objectServer answers a ranged GET from stored bodies, recording the ranges and
// keys asked for, and answers NoSuchKey the way S3 does for anything else.
func objectServer(objects map[string][]byte, seen *[]string) *mockS3API {
	return &mockS3API{getObjectFunc: func(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
		*seen = append(*seen, aws.ToString(params.Key)+" "+aws.ToString(params.Range))
		body, ok := objects[aws.ToString(params.Key)]
		if !ok {
			return nil, &types.NoSuchKey{}
		}
		return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
	}}
}

func TestArchiveReader_BlockHashAtReadsThePrefixOfTheVersionsBlockObject(t *testing.T) {
	const hash = "0x4d1c1a52b1f5e5a0c6f0b0a0d9e8c7b6a594837261504f3e2d1c0b9a8f7e6d5c"
	var seen []string
	blockKey := s3key.Build(archiveHeight, 1, s3key.Block)
	mock := objectServer(map[string][]byte{blockKey: gzippedBlock(t, hash)}, &seen)

	got, found, err := newArchiveReader(mock).BlockHashAt(context.Background(), archiveHeight, 1)

	if err != nil {
		t.Fatalf("BlockHashAt: %v", err)
	}
	if !found || got != hash {
		t.Errorf("BlockHashAt = %q (found %v), want %q", got, found, hash)
	}
	if want := blockKey + " bytes=0-8191"; len(seen) != 1 || seen[0] != want {
		t.Errorf("reads = %v, want one ranged read %q", seen, want)
	}
}

// A version holding nothing that names a block is a height to repair, not a
// failure — the same answer the bulk downloader plans from.
func TestArchiveReader_BlockHashAtReportsAVersionThatNamesNoBlock(t *testing.T) {
	var seen []string
	mock := objectServer(nil, &seen)

	got, found, err := newArchiveReader(mock).BlockHashAt(context.Background(), archiveHeight, 1)

	if err != nil {
		t.Fatalf("BlockHashAt: %v", err)
	}
	if found || got != "" {
		t.Errorf("BlockHashAt = %q (found %v), want nothing", got, found)
	}
	if len(seen) != 2 {
		t.Errorf("reads = %v, want the block object and then the receipts", seen)
	}
}

func TestArchiveReader_BlockHashAtSurfacesAFailedRead(t *testing.T) {
	mock := &mockS3API{getObjectFunc: func(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
		return nil, errors.New("503 SlowDown")
	}}

	_, found, err := newArchiveReader(mock).BlockHashAt(context.Background(), archiveHeight, 1)

	if err == nil {
		t.Fatal("BlockHashAt succeeded against a failing read")
	}
	if found {
		t.Error("reported a hash it never read")
	}
	if !strings.Contains(err.Error(), archiveBucket) {
		t.Errorf("error = %v, want it to name the bucket", err)
	}
}
