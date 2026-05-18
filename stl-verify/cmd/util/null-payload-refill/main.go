// Package main provides null-payload-refill, a stateful one-shot tool that
// heals null payload S3 objects in chain-raw buckets.
//
// Candidate keys come from one of two sources:
//
//   - --keys-file: a pre-built list, one S3 key per line. Use this when an
//     operator has already extracted the null inventory with `aws s3 ls`.
//   - bucket scan: when --keys-file is omitted, the tool paginates
//     ListObjectsV2 on --bucket (optionally narrowed by --prefix) and treats
//     objects with Size <= --max-size as candidates. Keys stream straight from
//     the lister into the worker pool, so refills overlap with the scan rather
//     than waiting for the full listing to complete.
//
// For each candidate key the tool:
//
//  1. Verifies the existing S3 object is still null (gunzip + IsNullOrEmpty).
//  2. Looks up the canonical block hash from the RPC via eth_getBlockByNumber.
//  3. Fetches the corresponding data from RPC via GetBlockDataByHash.
//  4. Overwrites the S3 object.
//  5. Republishes a BlockEvent to the watcher's SNS topic.
//
// The tool does not connect to Postgres: all metadata it needs is derived from
// RPC, so it can run anywhere with AWS + RPC credentials.
//
// Limitation — keys with version > 0 cannot be refilled
//
// S3 keys include a version slot — v0 is the first canonical attempt, v1+ are
// re-attempts triggered by chain reorgs (each version slot holds the data for
// a different block hash that was canonical at the time). The DB row in
// block_states is the only place that records which hash maps to which slot.
//
// Without the DB, we can only ask RPC "what is the canonical block at this
// number now?" — that answer corresponds to v0 only. Using the current
// canonical hash to refill a v1 slot would write data for the wrong block
// hash (today's canonical, not yesterday's orphan), silently corrupting the
// slot far worse than the original null. So we skip those keys with
// reason=cannot-determine-orphan-hash and let them remain null. The operator
// can grep the state file post-run for that reason.
//
// In practice every null record observed in the wild is v0 (per the VEC-241
// inventory), so this skip path has never fired against real data. If it
// ever does, the fix is either (a) extend the keys-file format to carry a
// hash hint per key (operator pulls the hash from a fresh snapshot or by
// hand), or (b) restore the DB lookup path as an optional --postgres-url
// fallback.
//
// Progress is persisted to a JSONL state file so the tool is resumable.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsS3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	"golang.org/x/sync/errgroup"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	snsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sns"
	"github.com/archon-research/stl/stl-verify/internal/pkg/awsconfig"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

const (
	defaultMaxSize          = 40
	defaultConcurrency      = 4
	defaultProgressInterval = 10 * time.Second
)

func main() {
	opts, fs, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fs.Usage()
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	exitCode, err := Run(ctx, opts, logger)
	if err != nil {
		logger.Error("run failed", "error", err)
		if exitCode == 0 {
			exitCode = 1
		}
	}
	os.Exit(exitCode)
}

// Options holds CLI configuration.
type Options struct {
	Bucket      string
	ChainID     int64
	KeysFile    string
	StateFile   string
	SNSTopicARN string
	RPCURL      string
	SNSEndpoint string
	S3Endpoint  string
	AWSRegion   string
	DryRun      bool
	Concurrency int

	// ProgressInterval is how often a "progress" log line is emitted while the
	// run is in flight. Set to 0 or negative to disable periodic progress.
	ProgressInterval time.Duration

	// MaxSize is the candidate threshold in bytes for the bucket-scan path.
	// Objects with Size <= MaxSize are treated as null candidates. Ignored when
	// KeysFile is set.
	MaxSize int64

	// Prefix narrows ListObjectsV2 to a sub-tree of the bucket (e.g. a single
	// partition). Empty means scan the whole bucket. Ignored when KeysFile is
	// set.
	Prefix string

	// Limit caps the number of candidate keys emitted by the producer. 0 means
	// no limit (process every key from the file / every matching object from
	// the scan). Useful as a safety cap for large runs or to size a smoke test.
	Limit int
}

func parseFlags(args []string) (Options, *flag.FlagSet, error) {
	fs := flag.NewFlagSet("null-payload-refill", flag.ContinueOnError)
	var opts Options
	fs.StringVar(&opts.Bucket, "bucket", "", "S3 bucket containing the null objects (required)")
	fs.Int64Var(&opts.ChainID, "chain-id", 0, "Chain ID used in published BlockEvents and operator diagnostics (required)")
	fs.StringVar(&opts.KeysFile, "keys-file", "", "Optional path to a file of S3 keys, one per line. If empty, the tool scans --bucket via ListObjectsV2 and treats objects with Size <= --max-size as candidates.")
	fs.StringVar(&opts.StateFile, "state-file", "", "JSONL state file path; defaults to <keys-file>.state or null-payload-refill.state when scanning")
	fs.StringVar(&opts.SNSTopicARN, "sns-topic-arn", "", "SNS FIFO topic ARN to publish BlockEvents to (required)")
	fs.StringVar(&opts.RPCURL, "rpc-url", "", "JSON-RPC endpoint URL (required)")
	fs.StringVar(&opts.SNSEndpoint, "sns-endpoint", "", "Optional SNS endpoint override (for LocalStack)")
	fs.StringVar(&opts.S3Endpoint, "s3-endpoint", "", "Optional S3 endpoint override (for LocalStack)")
	fs.StringVar(&opts.AWSRegion, "aws-region", "", "Optional AWS region override")
	fs.BoolVar(&opts.DryRun, "dry-run", false, "If set, perform all reads/RPC but skip S3 writes and SNS publishes")
	fs.IntVar(&opts.Concurrency, "concurrency", defaultConcurrency, "Number of parallel workers")
	fs.DurationVar(&opts.ProgressInterval, "progress-interval", defaultProgressInterval, "How often to log aggregate progress (0 to disable)")
	fs.Int64Var(&opts.MaxSize, "max-size", defaultMaxSize, "Scan path only: candidate threshold in bytes. Gzipped null is ~28B; valid gzipped blocks are kilobytes+. Ignored when --keys-file is set.")
	fs.StringVar(&opts.Prefix, "prefix", "", "Scan path only: optional ListObjectsV2 prefix (e.g. \"85149000-85149999/\") to narrow the scan. Ignored when --keys-file is set.")
	fs.IntVar(&opts.Limit, "limit", 0, "Cap the number of candidate keys to emit (0 = no limit). Useful as a safety cap or to size a smoke test.")
	if err := fs.Parse(args); err != nil {
		return Options{}, fs, err
	}

	if opts.Bucket == "" {
		return opts, fs, errors.New("--bucket is required")
	}
	if opts.ChainID <= 0 {
		return opts, fs, errors.New("--chain-id is required and must be > 0")
	}
	if opts.SNSTopicARN == "" {
		return opts, fs, errors.New("--sns-topic-arn is required")
	}
	if opts.RPCURL == "" {
		return opts, fs, errors.New("--rpc-url is required")
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 1
	}
	if opts.KeysFile == "" && opts.MaxSize <= 0 {
		return opts, fs, errors.New("--max-size must be > 0 when scanning the bucket")
	}
	if opts.StateFile == "" {
		if opts.KeysFile != "" {
			opts.StateFile = opts.KeysFile + ".state"
		} else {
			opts.StateFile = "null-payload-refill.state"
		}
	}
	return opts, fs, nil
}

// Run wires up dependencies and processes candidate S3 keys. Keys are sourced
// either from opts.KeysFile (one key per line) or, when KeysFile is empty, by
// streaming ListObjectsV2 against opts.Bucket and filtering objects with
// Size <= opts.MaxSize. Returns (exitCode, error) where exitCode is non-zero
// when any key failed or the key source produced an error.
func Run(ctx context.Context, opts Options, logger *slog.Logger) (int, error) {
	logKeySourceConfig(logger, opts)

	state, err := Load(opts.StateFile)
	if err != nil {
		return 1, fmt.Errorf("load state: %w", err)
	}
	defer func() {
		if err := state.Close(); err != nil {
			logger.Error("state close failed", "error", err)
		}
	}()

	awsCfg, err := awsconfig.Load(ctx, awsconfig.Options{Region: opts.AWSRegion})
	if err != nil {
		return 1, fmt.Errorf("load aws config: %w", err)
	}

	s3Client := awsS3.NewFromConfig(awsCfg, s3OptsFor(opts))
	s3Reader := s3adapter.NewReaderWithOptions(awsCfg, logger, s3OptsFor(opts))
	s3Overwriter := s3adapter.NewWriterWithOptions(awsCfg, logger, s3OptsFor(opts))

	snsClient := awssns.NewFromConfig(awsCfg, func(o *awssns.Options) {
		if opts.SNSEndpoint != "" {
			o.BaseEndpoint = aws.String(opts.SNSEndpoint)
		}
	})
	publisher, err := snsadapter.NewEventSink(snsClient, snsadapter.Config{
		TopicARN: opts.SNSTopicARN,
		Logger:   logger,
	})
	if err != nil {
		return 1, fmt.Errorf("create sns event sink: %w", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			logger.Error("publisher close failed", "error", err)
		}
	}()

	rpc, err := alchemy.NewClient(alchemy.ClientConfig{
		HTTPURL:      opts.RPCURL,
		Logger:       logger,
		EnableTraces: true,
		EnableBlobs:  true,
		ParallelRPC:  false,
	})
	if err != nil {
		return 1, fmt.Errorf("create rpc client: %w", err)
	}

	refiller, err := NewRefiller(RefillerOptions{
		Bucket:       opts.Bucket,
		ChainID:      opts.ChainID,
		S3Reader:     s3Reader,
		S3Overwriter: s3Overwriter,
		RPCClient:    rpc,
		Publisher:    publisher,
		State:        state,
		Logger:       logger,
		DryRun:       opts.DryRun,
	})
	if err != nil {
		return 1, fmt.Errorf("create refiller: %w", err)
	}

	// errgroup gives us first-error-wins + cancel-peers + Wait-returns-first-err
	// for free, replacing the previous bespoke fatalErrSink. The parent ctx is
	// the signal-bound context from main(); errgroup's derived gctx is
	// cancelled by either a worker error or the signal, so SIGINT propagation
	// remains intact.
	g, gctx := errgroup.WithContext(ctx)

	// Buffer the key channel by twice the worker count so a slow producer or
	// consumer does not stall the other side unnecessarily, while still
	// providing backpressure: the producer cannot run more than 2*Concurrency
	// keys ahead of the slowest worker.
	bufSize := max(opts.Concurrency*2, 1)
	keysCh := make(chan string, bufSize)

	sum := newSummary()
	// Producer participates in the errgroup so a List failure (e.g.
	// AccessDenied from ListObjectsV2) is surfaced via Wait. We treat
	// context.Canceled as benign (signal-driven exit). The deferred close on
	// keysCh terminates worker for-range loops once listing is done.
	g.Go(func() error {
		defer close(keysCh)
		err := streamKeys(gctx, opts, s3Client, sum, keysCh)
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	})

	// Periodic progress so the operator sees motion on long runs without
	// scanning every per-key INFO line. The progress goroutine cannot live
	// inside the errgroup: it only exits on ctx.Done(), and errgroup.Wait()
	// blocks on every Go-func — that would deadlock (Wait waits for
	// progress, progress waits for Wait to cancel gctx). Instead, spawn it
	// outside, then close `progressDone` after Wait returns so logSummary
	// reliably fires AFTER the last progress line.
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		reportProgress(gctx, sum, logger, opts.ProgressInterval)
	}()
	runProcessKeys(g, gctx, refiller, keysCh, opts.Concurrency, sum, logger)

	waitErr := g.Wait()
	// gctx is cancelled by errgroup the moment Wait returns; the progress
	// goroutine sees that and exits on its next select iteration. Wait for
	// it so the final summary line doesn't race with a trailing progress.
	<-progressDone
	logSummary(logger, sum)

	// context.Canceled is the benign signal-driven exit path; any other error
	// is a real abort (fatal worker outcome or producer failure).
	if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		return 1, fmt.Errorf("aborted: %w", waitErr)
	}

	if sum.failed.Load() > 0 {
		return 1, nil
	}
	return 0, nil
}

// streamKeys dispatches to the file or bucket producer based on opts.
func streamKeys(ctx context.Context, opts Options, lister s3Lister, sum *summary, out chan<- string) error {
	if opts.KeysFile != "" {
		return streamKeysFromFile(ctx, opts.KeysFile, opts.Limit, sum, out)
	}
	return streamKeysFromBucket(ctx, lister, opts.Bucket, opts.Prefix, opts.MaxSize, opts.Limit, sum, out)
}

// logKeySourceConfig prints a single INFO line describing where keys come from
// so the operator immediately sees which path is active and which flags are
// being ignored.
func logKeySourceConfig(logger *slog.Logger, opts Options) {
	if opts.KeysFile != "" {
		logger.Info("key source: file",
			"keys_file", opts.KeysFile,
			"note", "--max-size and --prefix are ignored when --keys-file is set",
		)
		return
	}
	logger.Info("key source: bucket scan",
		"bucket", opts.Bucket,
		"prefix", opts.Prefix,
		"max_size_bytes", opts.MaxSize,
	)
}

// s3OptsFor returns an S3 option function that applies the endpoint override
// when one is configured.
func s3OptsFor(opts Options) func(*awsS3.Options) {
	return func(o *awsS3.Options) {
		if opts.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.S3Endpoint)
			o.UsePathStyle = true
		}
	}
}

// summary tracks counts across the run.
//
// total is the producer's best estimate of how many keys it will emit. For the
// file source it is set once after a single pre-count pass. For the bucket
// source it grows as the scan finds candidates, so during a scan total is a
// running lower bound rather than the final figure. Zero means unknown.
type summary struct {
	processed atomic.Int64
	total     atomic.Int64
	refilled  atomic.Int64
	skipped   atomic.Int64
	failed    atomic.Int64
	dryRun    atomic.Int64

	mu           sync.Mutex
	reasonCounts map[string]int64
}

func newSummary() *summary { return &summary{reasonCounts: make(map[string]int64)} }

func (s *summary) record(out Outcome) {
	if out.Stage == "" {
		return
	}
	s.processed.Add(1)
	switch out.Stage {
	case StageSNS:
		s.refilled.Add(1)
	case StageSkip:
		s.skipped.Add(1)
	case StageFail:
		s.failed.Add(1)
	case StageDryRun:
		s.dryRun.Add(1)
	}
	if out.Reason != "" {
		s.mu.Lock()
		s.reasonCounts[out.Reason]++
		s.mu.Unlock()
	}
}

// keyProcessor is the narrow surface runProcessKeys needs from *Refiller.
// Defining it locally keeps the worker loop unit-testable: tests inject a
// stub that returns a scripted Outcome without spinning up an S3/RPC fake.
type keyProcessor interface {
	Process(ctx context.Context, key string) Outcome
}

// runProcessKeys schedules concurrency worker goroutines on g. Each worker
// reads keys from jobs and calls r.Process. On the first fatal Outcome any
// worker observes, the worker returns an error so errgroup cancels gctx and
// records the first error for g.Wait. Producer + peer workers unwind via
// gctx cancellation. Per-key (non-fatal) outcomes log and continue.
func runProcessKeys(g *errgroup.Group, gctx context.Context, r keyProcessor, jobs <-chan string, concurrency int, sum *summary, logger *slog.Logger) {
	if concurrency < 1 {
		concurrency = 1
	}
	for i := 0; i < concurrency; i++ {
		workerID := i
		g.Go(func() error {
			return runWorker(gctx, workerID, r, jobs, sum, logger)
		})
	}
}

// runWorker is the per-goroutine loop. Returning a non-nil error from any
// worker causes errgroup to cancel gctx — that's how fatal outcomes abort
// the run and unblock peers.
func runWorker(gctx context.Context, workerID int, r keyProcessor, jobs <-chan string, sum *summary, logger *slog.Logger) error {
	wlog := logger.With("worker", workerID)
	for key := range jobs {
		select {
		case <-gctx.Done():
			return nil
		default:
		}
		out := r.Process(gctx, key)
		sum.record(out)
		if out.Fatal {
			wlog.Error("fatal error — aborting run",
				"key", key,
				"reason", out.Reason,
				"error", out.Err,
			)
			return fmt.Errorf("key %s: %s: %w", key, out.Reason, out.Err)
		}
		if out.Stage == "" {
			// Cancellation produces an empty Outcome — no log, no error;
			// the for-range will exit when jobs is drained / closed.
			continue
		}
		wlog.Info("processed key",
			"key", key,
			"stage", string(out.Stage),
			"reason", out.Reason,
			"skipped", out.Skipped,
		)
	}
	return nil
}

// reportProgress emits an aggregate "progress" log line every interval until
// ctx is cancelled. interval <= 0 disables periodic progress entirely (used
// by tests that want quiet output, or operators who prefer per-key INFO).
func reportProgress(ctx context.Context, s *summary, logger *slog.Logger, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			logger.Info("progress",
				"processed", s.processed.Load(),
				"total", s.total.Load(),
				"refilled", s.refilled.Load(),
				"skipped", s.skipped.Load(),
				"failed", s.failed.Load(),
				"dry_run", s.dryRun.Load(),
			)
		}
	}
}

func logSummary(logger *slog.Logger, s *summary) {
	logger.Info("run complete",
		"processed", s.processed.Load(),
		"total", s.total.Load(),
		"refilled", s.refilled.Load(),
		"skipped", s.skipped.Load(),
		"failed", s.failed.Load(),
		"dry_run", s.dryRun.Load(),
	)
	s.mu.Lock()
	defer s.mu.Unlock()
	for reason, count := range s.reasonCounts {
		logger.Info("reason breakdown", "reason", reason, "count", count)
	}
}
