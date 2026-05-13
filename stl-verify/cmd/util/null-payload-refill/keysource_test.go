package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ---- file source ------------------------------------------------------------

func TestStreamKeysFromFile_HappyPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.txt")
	body := "" +
		"a/b/c\n" +
		"  d/e/f  \n" + // whitespace trimmed
		"\n" + // blank line skipped
		"# a comment\n" + // comment skipped
		"g/h/i\n" +
		"j/k/l\n" +
		"m/n/o\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	out := make(chan string, 16)
	err := streamKeysFromFile(context.Background(), path, out)
	close(out)
	if err != nil {
		t.Fatalf("streamKeysFromFile: %v", err)
	}

	want := []string{"a/b/c", "d/e/f", "g/h/i", "j/k/l", "m/n/o"}
	var got []string
	for k := range out {
		got = append(got, k)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d keys (%v), want %d (%v)", len(got), got, len(want), want)
	}
	for i, k := range want {
		if got[i] != k {
			t.Errorf("got[%d] = %q, want %q", i, got[i], k)
		}
	}
}

func TestStreamKeysFromFile_OpenError(t *testing.T) {
	out := make(chan string, 1)
	err := streamKeysFromFile(context.Background(), "/nonexistent/path/that/does/not/exist", out)
	close(out)
	if err == nil {
		t.Fatal("expected error for non-existent file")
	}
	for k := range out {
		t.Errorf("unexpected key emitted: %q", k)
	}
}

func TestStreamKeysFromFile_ContextCancel(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.txt")
	// 1000 keys; consumer will stop reading partway and trigger context cancel.
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := range 1000 {
		if _, err := f.WriteString("k" + string(rune('a'+i%26)) + "/" + string(rune('a'+(i/26)%26)) + "\n"); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan string) // unbuffered: producer blocks until consumer reads

	done := make(chan error, 1)
	go func() {
		done <- streamKeysFromFile(ctx, path, out)
	}()

	// Drain 10 keys, then cancel.
	for i := range 10 {
		select {
		case <-out:
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout draining key %d", i)
		}
	}
	cancel()

	// Drain anything remaining so the producer can finish without deadlock.
	drainDone := make(chan struct{})
	go func() {
		for range out {
		}
		close(drainDone)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected non-nil error after cancel, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("producer did not exit after cancel")
	}
	close(out)
	<-drainDone
}

// ---- bucket source ---------------------------------------------------------

// mockLister implements s3Lister. It returns pages from the configured slice
// in order and records every call.
type mockLister struct {
	pages    []*s3.ListObjectsV2Output
	errors   []error // per-page error; nil means use the page
	calls    atomic.Int32
	onCall   func(idx int) // called synchronously per ListObjectsV2 invocation
	inputs   []*s3.ListObjectsV2Input
	inputsMu sync.Mutex
}

func (m *mockLister) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	idx := int(m.calls.Add(1) - 1)
	m.inputsMu.Lock()
	m.inputs = append(m.inputs, params)
	m.inputsMu.Unlock()
	if m.onCall != nil {
		m.onCall(idx)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if idx < len(m.errors) && m.errors[idx] != nil {
		return nil, m.errors[idx]
	}
	if idx >= len(m.pages) {
		return &s3.ListObjectsV2Output{IsTruncated: aws.Bool(false)}, nil
	}
	return m.pages[idx], nil
}

func (m *mockLister) callCount() int { return int(m.calls.Load()) }

func makeObj(key string, size int64) types.Object {
	return types.Object{Key: aws.String(key), Size: aws.Int64(size)}
}

func TestStreamKeysFromBucket_FiltersBySize(t *testing.T) {
	lister := &mockLister{
		pages: []*s3.ListObjectsV2Output{{
			Contents: []types.Object{
				makeObj("p/a", 28),
				makeObj("p/b", 40),
				makeObj("p/c", 41),
				makeObj("p/d", 100),
			},
			IsTruncated: aws.Bool(false),
		}},
	}

	out := make(chan string, 8)
	err := streamKeysFromBucket(context.Background(), lister, "bucket", "", 40, out)
	close(out)
	if err != nil {
		t.Fatalf("streamKeysFromBucket: %v", err)
	}

	var got []string
	for k := range out {
		got = append(got, k)
	}
	want := []string{"p/a", "p/b"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i, k := range want {
		if got[i] != k {
			t.Errorf("got[%d] = %q, want %q", i, got[i], k)
		}
	}
}

func TestStreamKeysFromBucket_StreamsAcrossPages(t *testing.T) {
	// Three pages of 5 objects each, all under threshold.
	pages := make([]*s3.ListObjectsV2Output, 3)
	for p := range 3 {
		objs := make([]types.Object, 5)
		for i := range 5 {
			objs[i] = makeObj("p"+string(rune('0'+p))+"/"+string(rune('a'+i)), 20)
		}
		var token *string
		if p < 2 {
			token = aws.String("token-" + string(rune('0'+p)))
		}
		pages[p] = &s3.ListObjectsV2Output{
			Contents:              objs,
			IsTruncated:           aws.Bool(p < 2),
			NextContinuationToken: token,
		}
	}

	lister := &mockLister{pages: pages}

	out := make(chan string) // unbuffered → producer blocks until consumer reads
	done := make(chan error, 1)
	go func() {
		done <- streamKeysFromBucket(context.Background(), lister, "bucket", "", 100, out)
	}()

	// Read first key; capture call count. The unbuffered channel guarantees the
	// producer issued at least one ListObjectsV2 call before any key arrives,
	// and page 2 cannot have been requested yet because page 1's first item
	// must be consumed before the producer can move past it. If the producer
	// were batching everything, observedAtFirstKey would be 3; streaming = 1.
	select {
	case <-out:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for first key")
	}
	observedAtFirstKey := lister.callCount()

	// Drain the remaining keys until the producer exits.
	count := 1
	drainDone := make(chan struct{})
	go func() {
		for range out {
			count++
		}
		close(drainDone)
	}()

	if err := <-done; err != nil {
		t.Fatalf("streamKeysFromBucket: %v", err)
	}
	close(out)
	<-drainDone

	if count != 15 {
		t.Errorf("emitted %d keys, want 15", count)
	}
	// The "streaming, not batched" assertion: by the time we'd read the first
	// key, page 2 must NOT have been requested yet. (Channel cap is 0, so
	// each item must be consumed before the next is produced; the producer
	// cannot have advanced to page 2 before we read page 1's first item.)
	if observedAtFirstKey != 1 {
		t.Errorf("ListObjectsV2 call count at first key = %d, want 1 (streaming, not batched)", observedAtFirstKey)
	}
}

func TestStreamKeysFromBucket_HonorsPrefix(t *testing.T) {
	lister := &mockLister{
		pages: []*s3.ListObjectsV2Output{{
			Contents:    []types.Object{makeObj("partition/k", 10)},
			IsTruncated: aws.Bool(false),
		}},
	}
	out := make(chan string, 4)
	if err := streamKeysFromBucket(context.Background(), lister, "bucket", "partition/", 40, out); err != nil {
		t.Fatalf("streamKeysFromBucket: %v", err)
	}
	close(out)

	lister.inputsMu.Lock()
	defer lister.inputsMu.Unlock()
	if len(lister.inputs) == 0 {
		t.Fatalf("no ListObjectsV2 calls")
	}
	if got := aws.ToString(lister.inputs[0].Bucket); got != "bucket" {
		t.Errorf("bucket = %q, want bucket", got)
	}
	if got := aws.ToString(lister.inputs[0].Prefix); got != "partition/" {
		t.Errorf("prefix = %q, want partition/", got)
	}
}

func TestStreamKeysFromBucket_PropagatesError(t *testing.T) {
	page := &s3.ListObjectsV2Output{
		Contents:              []types.Object{makeObj("p/a", 10), makeObj("p/b", 10)},
		IsTruncated:           aws.Bool(true),
		NextContinuationToken: aws.String("tok"),
	}
	listErr := errors.New("page 2 boom")
	lister := &mockLister{
		pages:  []*s3.ListObjectsV2Output{page, nil},
		errors: []error{nil, listErr},
	}

	out := make(chan string, 8)
	err := streamKeysFromBucket(context.Background(), lister, "bucket", "", 100, out)
	close(out)
	if err == nil {
		t.Fatalf("expected error from second page")
	}
	if !errors.Is(err, listErr) {
		t.Errorf("error not wrapped: got %v, want %v wrapped", err, listErr)
	}

	var got []string
	for k := range out {
		got = append(got, k)
	}
	if len(got) != 2 {
		t.Errorf("expected first page's 2 keys to be emitted before error, got %v", got)
	}
}

func TestStreamKeysFromBucket_ContextCancel(t *testing.T) {
	// Page 1 with 1 key, then we cancel before page 2 is requested.
	pages := []*s3.ListObjectsV2Output{
		{
			Contents:              []types.Object{makeObj("p/a", 10)},
			IsTruncated:           aws.Bool(true),
			NextContinuationToken: aws.String("tok"),
		},
		{
			Contents:    []types.Object{makeObj("p/b", 10)},
			IsTruncated: aws.Bool(false),
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	lister := &mockLister{pages: pages}

	out := make(chan string) // unbuffered
	done := make(chan error, 1)
	go func() {
		done <- streamKeysFromBucket(ctx, lister, "bucket", "", 100, out)
	}()

	// Consume the first key, then cancel.
	select {
	case k := <-out:
		if k != "p/a" {
			t.Errorf("first key = %q, want p/a", k)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for first key")
	}
	cancel()

	// Drain remainder so the producer goroutine never blocks on send.
	drainDone := make(chan struct{})
	go func() {
		for range out {
		}
		close(drainDone)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected non-nil error after cancel")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("producer did not exit after cancel")
	}
	close(out)
	<-drainDone
	// At most 2 calls (initial + maybe one more in flight); never more.
	if c := lister.callCount(); c > 2 {
		t.Errorf("ListObjectsV2 call count = %d, want <= 2 after cancel", c)
	}
}
