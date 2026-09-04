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
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/archon-research/stl/stl-verify/internal/pkg/archiveblock"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedLocalStackCfg testutil.LocalStackConfig

// rawBucketPrefix mirrors the raw archive buckets the tool writes to.
const rawBucketPrefix = "stl-sentineltest-ethereum-raw-"

// forkedBlock is a height whose only archived version is a losing fork.
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

	for _, dataType := range ethereumTypes() {
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

	_, err := runDownloader(t, ctx, bucket, downloaderRun{rpcFailure: true})

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
	for _, dataType := range ethereumTypes() {
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

// The audit an operator runs on a chain: a dry run over the archive leaves the
// holes it found in a file, and touches nothing.
func TestRunIntegration_ADryRunReportsTheHoleItLeftInPlace(t *testing.T) {
	ctx := context.Background()
	client, bucket := archiveBucket(t, ctx)
	seedArchivedVersion(t, ctx, client, bucket, 0, forkHash)
	reportPath := filepath.Join(t.TempDir(), "holes.jsonl")

	mustRunDownloader(t, ctx, bucket, downloaderRun{dryRun: true, reportPath: reportPath})

	lines := reportLines(t, reportPath)
	if len(lines) != 1 {
		t.Fatalf("report lines = %+v, want the one forked height", lines)
	}
	if lines[0].Block != forkedBlock || lines[0].Action != actionRepublish || lines[0].Version != 1 {
		t.Errorf("report line = %+v, want block %d republished at version 1", lines[0], forkedBlock)
	}
	if versions := archivedVersions(t, ctx, client, bucket); !slices.Equal(versions, []int{0}) {
		t.Errorf("archived versions after the audit = %v, want only the seeded %v", versions, []int{0})
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

func TestRunIntegration_PlansACanonicalArchiveFromHeadersAlone(t *testing.T) {
	ctx := context.Background()
	client, bucket := archiveBucket(t, ctx)
	seedArchivedVersion(t, ctx, client, bucket, 0, canonicalHash)

	node := mustRunDownloader(t, ctx, bucket, downloaderRun{})

	headers, fullBlocks := node.reads()
	if fullBlocks != 0 {
		t.Errorf("full block fetches = %d, want none: an archived height's plan needs one hash, not 0.5-2 MB", fullBlocks)
	}
	if headers != 1 {
		t.Errorf("header fetches = %d, want 1", headers)
	}
}

func TestRunIntegration_RefusesARangeAboveTheFinalizedHead(t *testing.T) {
	ctx := context.Background()
	client, bucket := archiveBucket(t, ctx)

	_, err := runDownloader(t, ctx, bucket, downloaderRun{finalizedHead: forkedBlock - 1})

	if err == nil {
		t.Fatal("expected the run refused: a fork archived above the finalized head can never be corrected")
	}
	if versions := archivedVersions(t, ctx, client, bucket); len(versions) != 0 {
		t.Errorf("archived versions = %v, want none: the refusal must land before any write", versions)
	}
}

func TestRunIntegration_AllowUnfinalizedArchivesAboveTheFinalizedHead(t *testing.T) {
	ctx := context.Background()
	client, bucket := archiveBucket(t, ctx)

	mustRunDownloader(t, ctx, bucket, downloaderRun{finalizedHead: forkedBlock - 1, allowUnfinalized: true})

	if versions := archivedVersions(t, ctx, client, bucket); !slices.Equal(versions, []int{0}) {
		t.Errorf("archived versions = %v, want %v: --allow-unfinalized overrides the guard", versions, []int{0})
	}
}

// A bucket the run cannot reach — a typo, or a grant it does not have — must
// stop it at startup rather than after every partition has burned its retries.
func TestRunIntegration_RefusesABucketItCannotReach(t *testing.T) {
	ctx := context.Background()
	missing := testutil.S3TestBucketName(t, rawBucketPrefix)

	_, err := runDownloader(t, ctx, missing, downloaderRun{})

	if err == nil {
		t.Fatal("run() succeeded against a bucket that is not there")
	}
	for _, want := range []string{missing, "s3:ListBucket"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to mention %q", err, want)
		}
	}
}

// A report path the run cannot write is the operator's whole answer, so it must
// stop the run before it spends an hour of RPC and S3 reads reaching it.
func TestRunIntegration_RefusesAnUnwritableReportBeforeAnyWork(t *testing.T) {
	ctx := context.Background()
	_, bucket := archiveBucket(t, ctx)
	unwritable := filepath.Join(t.TempDir(), "no-such-directory", "holes.jsonl")

	node, err := runDownloader(t, ctx, bucket, downloaderRun{dryRun: true, reportPath: unwritable})

	if err == nil {
		t.Fatal("run() succeeded with a report path it cannot create")
	}
	if !strings.Contains(err.Error(), unwritable) {
		t.Errorf("error = %v, want it to name the report path %q", err, unwritable)
	}
	if served := node.served(); served != 0 {
		t.Errorf("the node answered %d calls before the report failed, want none", served)
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
	dryRun           bool
	rpcFailure       bool
	allowUnfinalized bool
	// finalizedHead defaults to the height the tests archive.
	finalizedHead int64
	// reportPath is where the run writes its decisions; empty means no report.
	reportPath string
}

func mustRunDownloader(t *testing.T, ctx context.Context, bucket string, opts downloaderRun) *fakeErigon {
	t.Helper()

	node, err := runDownloader(t, ctx, bucket, opts)
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	return node
}

// runDownloader drives run() against LocalStack and a fake Erigon serving the
// canonical block.
func runDownloader(t *testing.T, ctx context.Context, bucket string, opts downloaderRun) (*fakeErigon, error) {
	t.Helper()

	node := startFakeErigon(t, opts)
	t.Setenv("AWS_S3_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	cfg := Config{
		ChainID:             ethereumChainID,
		RPCURL:              node.url,
		StartBlock:          forkedBlock,
		EndBlock:            forkedBlock,
		Bucket:              bucket,
		Region:              sharedLocalStackCfg.Region,
		DryRun:              opts.dryRun,
		ReportPath:          opts.reportPath,
		AllowUnfinalized:    opts.allowUnfinalized,
		BlockReceiptWorkers: 1,
		TraceWorkers:        1,
		UploadWorkers:       1,
		BlockBatchSize:      1,
		TraceBatchSize:      1,
	}
	return node, run(ctx, cfg, testutil.DiscardLogger())
}

// fakeErigon answers the block, receipt and trace calls with a canonical block
// at the forked height, and counts the reads so a test can tell a header from a
// full block.
type fakeErigon struct {
	url           string
	failing       bool
	finalizedHead int64

	mu         sync.Mutex
	requests   int
	headers    int
	fullBlocks int
}

func startFakeErigon(t *testing.T, opts downloaderRun) *fakeErigon {
	t.Helper()

	node := &fakeErigon{failing: opts.rpcFailure, finalizedHead: opts.finalizedHead}
	if node.finalizedHead == 0 {
		node.finalizedHead = forkedBlock
	}

	server := httptest.NewServer(http.HandlerFunc(node.serve))
	t.Cleanup(server.Close)
	node.url = server.URL
	return node
}

func (f *fakeErigon) reads() (headers, fullBlocks int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.headers, f.fullBlocks
}

// served counts every request the node answered, whatever it asked for, so a
// test can assert a run stopped before it reached the node at all.
func (f *fakeErigon) served() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

// rpcRequest is as much of a JSON-RPC call as the fake node needs to answer it.
type rpcRequest struct {
	ID     int               `json:"id"`
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}

// serve answers a batch with an array and a single call with an object, the way
// a node does.
func (f *fakeErigon) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests++
	f.mu.Unlock()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var batch []rpcRequest
	if err := json.Unmarshal(body, &batch); err == nil {
		f.reply(w, batch, true)
		return
	}

	var single rpcRequest
	if err := json.Unmarshal(body, &single); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.reply(w, []rpcRequest{single}, false)
}

func (f *fakeErigon) reply(w http.ResponseWriter, requests []rpcRequest, batched bool) {
	responses := make([]map[string]any, 0, len(requests))
	for _, req := range requests {
		// The finalized head answers even on a failing node, so a test of a
		// failed block is not a test of the finality guard.
		if f.failing && !isFinalizedTag(req) {
			responses = append(responses, map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"error":   map[string]any{"code": -32000, "message": "block not found"},
			})
			continue
		}

		result, err := f.result(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		responses = append(responses, map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	}

	w.Header().Set("Content-Type", "application/json")
	var payload any = responses
	if !batched {
		payload = responses[0]
	}
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		panic(err)
	}
}

func (f *fakeErigon) result(req rpcRequest) (json.RawMessage, error) {
	switch req.Method {
	case "eth_getBlockByNumber":
		return f.blockResult(req)
	case "eth_getBlockReceipts":
		return receiptsJSON(canonicalHash, 2), nil
	case "trace_block":
		return tracesJSON(canonicalHash), nil
	default:
		return nil, fmt.Errorf("unexpected RPC method %q", req.Method)
	}
}

func isFinalizedTag(req rpcRequest) bool {
	return req.Method == "eth_getBlockByNumber" && len(req.Params) > 0 && string(req.Params[0]) == `"finalized"`
}

func (f *fakeErigon) blockResult(req rpcRequest) (json.RawMessage, error) {
	if len(req.Params) != 2 {
		return nil, fmt.Errorf("eth_getBlockByNumber takes 2 params, got %d", len(req.Params))
	}
	if isFinalizedTag(req) {
		return headerJSON(canonicalHash, f.finalizedHead), nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if string(req.Params[1]) == "true" {
		f.fullBlocks++
		return blockJSON(canonicalHash, 2), nil
	}
	f.headers++
	return headerJSON(canonicalHash, forkedBlock), nil
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

	if dataType == s3key.Block {
		hash, ok := archiveblock.HashFromPayload(plain)
		if !ok {
			t.Fatalf("no hash in %s", key)
		}
		return hash
	}

	// Receipts and traces are lists whose entries name the block they belong to.
	var entries []struct {
		BlockHash string `json:"blockHash"`
	}
	if err := json.Unmarshal(plain, &entries); err != nil {
		t.Fatalf("decoding %s: %v", key, err)
	}
	if len(entries) == 0 || entries[0].BlockHash == "" {
		t.Fatalf("no blockHash in %s", key)
	}
	return entries[0].BlockHash
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

// headerJSON is an eth_getBlockByNumber payload with fullTx false: the fields a
// header carries, and no transaction bodies.
func headerJSON(hash string, blockNum int64) []byte {
	return fmt.Appendf(nil, `{"hash":%q,"number":"0x%x","parentHash":%q}`, hash, blockNum, forkHash)
}

func tracesJSON(blockHash string) []byte {
	return fmt.Appendf(nil, `[{"blockHash":%q,"type":"call","action":{"input":%q}}]`, blockHash, randomHex(64))
}
