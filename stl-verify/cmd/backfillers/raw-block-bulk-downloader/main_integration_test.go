//go:build integration

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedLocalStackCfg testutil.LocalStackConfig

// rawBucketPrefix mirrors the raw archive buckets the tool writes to.
const rawBucketPrefix = "stl-sentineltest-ethereum-raw-"

// forkedBlock is the ARCT-379 shape: a height whose only archived version is a
// losing fork.
const forkedBlock = int64(25395651)

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{
		LocalStack:         &sharedLocalStackCfg,
		LocalStackServices: "s3",
	}))
}

func TestRunIntegration_RepublishesALosingForkAtTheNextVersion(t *testing.T) {
	ctx := context.Background()
	client, bucket := archiveBucket(t, ctx)
	seedArchivedVersion(t, ctx, client, bucket, 0, forkHash)

	mustRunDownloader(t, ctx, bucket, downloaderRun{})

	for _, dataType := range archivedTypes {
		if got := storedBlockHash(t, ctx, client, bucket, 1, dataType); got != canonicalHash {
			t.Errorf("%s at version 1 carries hash %q, want the canonical %q", dataType, got, canonicalHash)
		}
		if got := storedBlockHash(t, ctx, client, bucket, 0, dataType); got != forkHash {
			t.Errorf("%s at version 0 carries hash %q, want the untouched fork %q", dataType, got, forkHash)
		}
	}

}

func TestRunIntegration_ASecondRunOverACorrectedHeightWritesNothing(t *testing.T) {
	ctx := context.Background()
	client, bucket := archiveBucket(t, ctx)
	seedArchivedVersion(t, ctx, client, bucket, 0, forkHash)
	mustRunDownloader(t, ctx, bucket, downloaderRun{})

	mustRunDownloader(t, ctx, bucket, downloaderRun{})

	if versions := archivedVersions(t, ctx, client, bucket); !slices.Equal(versions, []int{0, 1}) {
		t.Errorf("archived versions after a second run = %v, want the first run's %v", versions, []int{0, 1})
	}
}

func TestRunIntegration_AFailedBlockFailsTheRun(t *testing.T) {
	ctx := context.Background()
	client, bucket := archiveBucket(t, ctx)
	seedArchivedVersion(t, ctx, client, bucket, 0, forkHash)

	err := runDownloader(t, ctx, bucket, downloaderRun{rpcFailure: true})

	if err == nil {
		t.Fatal("expected run() to fail: an exit code of 0 would hide the hole left in the archive")
	}
	if versions := archivedVersions(t, ctx, client, bucket); !slices.Equal(versions, []int{0}) {
		t.Errorf("archived versions = %v, want only the seeded %v", versions, []int{0})
	}
}

func TestRunIntegration_CorrectsATwiceForkedHeightAtVersionTwo(t *testing.T) {
	ctx := context.Background()
	client, bucket := archiveBucket(t, ctx)
	seedArchivedVersion(t, ctx, client, bucket, 0, forkHash)
	seedArchivedVersion(t, ctx, client, bucket, 1, forkHash)

	mustRunDownloader(t, ctx, bucket, downloaderRun{})

	if versions := archivedVersions(t, ctx, client, bucket); !slices.Equal(versions, []int{0, 1, 2}) {
		t.Errorf("archived versions = %v, want %v: a second correction goes above the losing top version", versions, []int{0, 1, 2})
	}
	for _, dataType := range archivedTypes {
		if got := storedBlockHash(t, ctx, client, bucket, 2, dataType); got != canonicalHash {
			t.Errorf("%s at version 2 carries hash %q, want the canonical %q", dataType, got, canonicalHash)
		}
	}
}

func TestRunIntegration_LeavesACanonicalArchiveUntouched(t *testing.T) {
	ctx := context.Background()
	client, bucket := archiveBucket(t, ctx)
	seedArchivedVersion(t, ctx, client, bucket, 0, canonicalHash)

	mustRunDownloader(t, ctx, bucket, downloaderRun{})

	if versions := archivedVersions(t, ctx, client, bucket); !slices.Equal(versions, []int{0}) {
		t.Errorf("archived versions = %v, want only the canonical %v: a re-run must not duplicate it", versions, []int{0})
	}
}

func TestRunIntegration_DryRunWritesNothing(t *testing.T) {
	ctx := context.Background()
	client, bucket := archiveBucket(t, ctx)
	seedArchivedVersion(t, ctx, client, bucket, 0, forkHash)

	mustRunDownloader(t, ctx, bucket, downloaderRun{dryRun: true})

	if versions := archivedVersions(t, ctx, client, bucket); !slices.Equal(versions, []int{0}) {
		t.Errorf("archived versions after a dry run = %v, want only the seeded %v", versions, []int{0})
	}
}

func archiveBucket(t *testing.T, ctx context.Context) (*awss3.Client, string) {
	t.Helper()

	client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	bucket := testutil.S3TestBucketName(t, rawBucketPrefix)
	testutil.EnsureBucket(t, ctx, client, bucket)
	return client, bucket
}

// downloaderRun is what varies between the runs the tests drive.
type downloaderRun struct {
	dryRun     bool
	rpcFailure bool
}

func mustRunDownloader(t *testing.T, ctx context.Context, bucket string, opts downloaderRun) {
	t.Helper()

	if err := runDownloader(t, ctx, bucket, opts); err != nil {
		t.Fatalf("run() error = %v", err)
	}
}

// runDownloader drives run() against LocalStack and a fake Erigon serving the
// canonical block.
func runDownloader(t *testing.T, ctx context.Context, bucket string, opts downloaderRun) error {
	t.Helper()

	rpc := startFakeErigon(t, opts.rpcFailure)
	t.Setenv("AWS_S3_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	cfg := Config{
		RPCURL:              rpc.URL,
		StartBlock:          forkedBlock,
		EndBlock:            forkedBlock,
		Bucket:              bucket,
		Region:              sharedLocalStackCfg.Region,
		DryRun:              opts.dryRun,
		BlockReceiptWorkers: 1,
		TraceWorkers:        1,
		UploadWorkers:       1,
		BlockBatchSize:      1,
		TraceBatchSize:      1,
	}
	return run(ctx, cfg, discardLogger())
}

// startFakeErigon answers the batched block, receipt and trace calls with a
// canonical block at the forked height.
func startFakeErigon(t *testing.T, failing bool) *httptest.Server {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var requests []struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&requests); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		responses := make([]map[string]any, 0, len(requests))
		for _, req := range requests {
			if failing {
				responses = append(responses, map[string]any{
					"jsonrpc": "2.0",
					"id":      req.ID,
					"error":   map[string]any{"code": -32000, "message": "block not found"},
				})
				continue
			}

			result, err := erigonResult(req.Method)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			responses = append(responses, map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(responses); err != nil {
			t.Errorf("encode RPC response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func erigonResult(method string) (json.RawMessage, error) {
	switch method {
	case "eth_getBlockByNumber":
		return blockJSON(canonicalHash, 2), nil
	case "eth_getBlockReceipts":
		return receiptsJSON(canonicalHash, 2), nil
	case "trace_block":
		return tracesJSON(canonicalHash), nil
	default:
		return nil, fmt.Errorf("unexpected RPC method %q", method)
	}
}

func seedArchivedVersion(t *testing.T, ctx context.Context, client *awss3.Client, bucket string, version int, hash string) {
	t.Helper()

	bodies := map[s3key.DataType][]byte{
		s3key.Block:    blockJSON(hash, 2),
		s3key.Receipts: receiptsJSON(hash, 2),
		s3key.Traces:   tracesJSON(hash),
	}
	for dataType, body := range bodies {
		key := s3key.Build(forkedBlock, version, dataType)
		_, err := client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket:          aws.String(bucket),
			Key:             aws.String(key),
			Body:            bytes.NewReader(gzipped(t, body)),
			ContentEncoding: aws.String("gzip"),
		})
		if err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
}

// storedBlockHash reads the block hash an archived object carries, so a rewrite
// of a key is visible even though the key itself did not change.
func storedBlockHash(t *testing.T, ctx context.Context, client *awss3.Client, bucket string, version int, dataType s3key.DataType) string {
	t.Helper()

	key := s3key.Build(forkedBlock, version, dataType)
	out, err := client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	defer out.Body.Close()

	stored, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(stored))
	if err != nil {
		t.Fatalf("gunzip %s: %v", key, err)
	}
	defer gz.Close()

	plain, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}

	depth, field := 2, "blockHash"
	if dataType == s3key.Block {
		depth, field = 1, "hash"
	}
	hash, ok := jsonStringField(plain, depth, field)
	if !ok {
		t.Fatalf("no %s in %s", field, key)
	}
	return hash
}

func archivedVersions(t *testing.T, ctx context.Context, client *awss3.Client, bucket string) []int {
	t.Helper()

	out, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("list %s: %v", bucket, err)
	}

	seen := map[int]bool{}
	for _, obj := range out.Contents {
		parsed, ok := s3key.Parse(*obj.Key)
		if !ok {
			t.Fatalf("unparsable key in the archive: %s", *obj.Key)
		}
		seen[parsed.Version] = true
	}

	versions := make([]int, 0, len(seen))
	for version := range seen {
		versions = append(versions, version)
	}
	slices.Sort(versions)
	return versions
}

func tracesJSON(blockHash string) []byte {
	return fmt.Appendf(nil, `[{"blockHash":%q,"type":"call","action":{"input":%q}}]`, blockHash, randomHex(64))
}
