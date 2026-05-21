package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/archon-research/stl/stl-verify/internal/pkg/gziputil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const defaultFetchTimeout = 15 * time.Second

// Outcome summarises what happened to a single key during processing.
//
// Err carries the underlying error for fail outcomes so callers can inspect
// the chain (e.g. errors.Is(out.Err, context.DeadlineExceeded)) without
// re-parsing the human-readable Reason string.
//
// Fatal=true means a run-level error (environment broken: RPC down, malformed
// input, programming error) — the worker loop must abort the run instead of
// moving on to the next key. The flag is signal-only: the state file still
// records stage:fail so a resumed run sees the entry. Per-item data outcomes
// (no-such-key, cannot-determine-orphan-hash) keep Fatal=false and use
// StageSkip so the run continues to the next key. Already-healthy S3 keys
// still route through the publish-only path (StageSNS, not StageSkip) so
// the operator's input list gets 100% SNS coverage.
type Outcome struct {
	Stage   Stage
	Reason  string
	Err     error
	Skipped bool
	Fatal   bool
}

// verifyResult is the tri-state returned by verifyStillNull. The caller relies
// on the NotFound case to surface a distinct skip reason ("no-such-key") so
// pointing the tool at the wrong bucket does not silently funnel every key
// through the publish-only path.
type verifyResult int

const (
	verifyNull verifyResult = iota
	verifyNonNull
	verifyNotFound
)

// Refiller is the per-key worker that heals a single null S3 record.
type Refiller struct {
	bucket       string
	chainID      int64
	s3Reader     outbound.S3Reader
	s3Overwriter outbound.S3Overwriter
	rpcClient    outbound.BlockchainClient
	publisher    outbound.EventSink
	state        *State
	logger       *slog.Logger
	dryRun       bool
	fetchTimeout time.Duration
}

// RefillerOptions bundles dependencies for NewRefiller.
type RefillerOptions struct {
	Bucket       string
	ChainID      int64
	S3Reader     outbound.S3Reader
	S3Overwriter outbound.S3Overwriter
	RPCClient    outbound.BlockchainClient
	Publisher    outbound.EventSink
	State        *State
	Logger       *slog.Logger
	DryRun       bool
	FetchTimeout time.Duration
}

// NewRefiller validates inputs and returns a configured Refiller.
func NewRefiller(opts RefillerOptions) (*Refiller, error) {
	if opts.Bucket == "" {
		return nil, errors.New("bucket is required")
	}
	if opts.ChainID <= 0 {
		return nil, errors.New("ChainID must be > 0")
	}
	if opts.S3Reader == nil {
		return nil, errors.New("S3Reader is required")
	}
	if opts.S3Overwriter == nil {
		return nil, errors.New("S3Overwriter is required")
	}
	if opts.RPCClient == nil {
		return nil, errors.New("RPCClient is required")
	}
	if opts.Publisher == nil {
		return nil, errors.New("publisher is required")
	}
	if opts.State == nil {
		return nil, errors.New("State is required")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	timeout := opts.FetchTimeout
	if timeout <= 0 {
		timeout = defaultFetchTimeout
	}
	return &Refiller{
		bucket:       opts.Bucket,
		chainID:      opts.ChainID,
		s3Reader:     opts.S3Reader,
		s3Overwriter: opts.S3Overwriter,
		rpcClient:    opts.RPCClient,
		publisher:    opts.Publisher,
		state:        opts.State,
		logger:       logger.With("component", "null-payload-refill"),
		dryRun:       opts.DryRun,
		fetchTimeout: timeout,
	}, nil
}

// Process runs the full per-key refill flow and persists progress to the state file.
func (r *Refiller) Process(ctx context.Context, key string) Outcome {
	parsed, priorStage, out, done := r.verify(ctx, key)
	if done {
		return out
	}
	bs, out, done := r.loadCanonicalBlock(ctx, key, parsed)
	if done {
		return out
	}
	// Resume-from-StageS3 skips the by-hash data fetch entirely. S3 was
	// already healed by a prior run; the only remaining work is the SNS
	// publish, which reads from bs (loaded above) and ignores data. Re-
	// fetching here would make resume depend on the RPC being healthy again,
	// stranding keys whose S3 object is already good if the RPC is now down.
	var data json.RawMessage
	if priorStage != StageS3 {
		data, out, done = r.fetchData(ctx, key, parsed, bs)
		if done {
			return out
		}
	}
	return r.writeAndPublish(ctx, key, bs, data, priorStage)
}

// verify resolves the prior stage, parses the key, and (when no S3 write
// has been recorded yet) verifies the object is still null. done=true means the
// caller must return the outcome immediately without continuing to later phases.
func (r *Refiller) verify(ctx context.Context, key string) (s3key.Key, Stage, Outcome, bool) {
	priorStage, priorReason := r.state.Lookup(key)
	// StageDryRun is only terminal when we're still dry-running. A subsequent
	// real run against the same state file must re-process every dry-run-
	// recorded key — otherwise the operator who dry-ran first would silently
	// skip all keys forever.
	if priorStage == StageSNS || priorStage == StageSkip || (priorStage == StageDryRun && r.dryRun) {
		return s3key.Key{}, priorStage, Outcome{Stage: priorStage, Reason: priorReason, Skipped: true}, true
	}

	parsed, ok := s3key.Parse(key)
	if !ok {
		return s3key.Key{}, priorStage, r.fatal(key, "invalid-key-format", fmt.Errorf("s3key.Parse rejected key %q", key)), true
	}

	if priorStage == StageS3 {
		return parsed, priorStage, Outcome{}, false
	}

	result, err := r.verifyStillNull(ctx, key)
	if err != nil {
		if parentCancelled(ctx) {
			return parsed, priorStage, r.cancelled(), true
		}
		return parsed, priorStage, r.fatal(key, "s3-verify-error", err), true
	}
	switch result {
	case verifyNull:
		return parsed, priorStage, Outcome{}, false
	case verifyNonNull:
		// S3 is healthy; route through the publish-only path so the
		// operator's input key still gets a SNS event (see
		// writeAndPublish for the corner-case rationale).
		return parsed, StageS3, Outcome{}, false
	case verifyNotFound:
		return parsed, priorStage, r.skip(key, "no-such-key"), true
	default:
		return parsed, priorStage, r.fatal(key, "unknown-verify-result", fmt.Errorf("unexpected verifyResult %d", result)), true
	}
}

// loadCanonicalBlock fetches the canonical block header from RPC by number,
// derives the block hash + parent hash + timestamp, and returns them as a
// synthetic *outbound.BlockState for use in the SNS BlockEvent. For
// version > 0 keys we skip — the orphan-chain hash for that slot cannot be
// recovered from RPC alone, and no such records exist in the wild today.
func (r *Refiller) loadCanonicalBlock(ctx context.Context, key string, parsed s3key.Key) (*outbound.BlockState, Outcome, bool) {
	if parsed.Version != 0 {
		return nil, r.skip(key, "cannot-determine-orphan-hash"), true
	}
	blockJSON, err := r.rpcClient.GetBlockByNumber(ctx, parsed.BlockNumber, false)
	if err != nil {
		if parentCancelled(ctx) {
			return nil, r.cancelled(), true
		}
		return nil, r.fatal(key, "rpc-error", err), true
	}
	if rpcutil.IsNullOrEmpty(blockJSON) {
		return nil, r.fatal(key, "rpc-still-null", rpcutil.ErrUpstreamNullResult), true
	}
	var hdr struct {
		Hash       string `json:"hash"`
		ParentHash string `json:"parentHash"`
		Timestamp  string `json:"timestamp"`
	}
	if err := json.Unmarshal(blockJSON, &hdr); err != nil {
		return nil, r.fatal(key, "block-decode-error", fmt.Errorf("unmarshal block header: %w", err)), true
	}
	if hdr.Hash == "" {
		return nil, r.fatal(key, "block-decode-error", fmt.Errorf("empty hash in eth_getBlockByNumber response for block %d", parsed.BlockNumber)), true
	}
	ts, err := hexutil.ParseInt64(hdr.Timestamp)
	if err != nil {
		return nil, r.fatal(key, "block-timestamp-decode-error", fmt.Errorf("parse block timestamp %q: %w", hdr.Timestamp, err)), true
	}
	return &outbound.BlockState{
		Number:         parsed.BlockNumber,
		Hash:           hdr.Hash,
		ParentHash:     hdr.ParentHash,
		Version:        parsed.Version,
		BlockTimestamp: ts,
		ReceivedAt:     ts,
	}, Outcome{}, false
}

// fetchData calls the RPC for the canonical block hash and extracts the field
// matching parsed.DataType. A still-null RPC response is fatal: the
// canonical-block lookup just succeeded for the same block, so failing to
// resolve the by-hash data is unexpected — the operator should investigate
// (upstream node out of sync, wrong endpoint, block genuinely missing) rather
// than have the tool burn through every key recording the same symptom. The
// fatal state record lets the operator either fix the upstream cause and
// resume, or hand-edit the state file to skip that one block permanently.
func (r *Refiller) fetchData(ctx context.Context, key string, parsed s3key.Key, bs *outbound.BlockState) (json.RawMessage, Outcome, bool) {
	bd, err := r.rpcClient.GetBlockDataByHash(ctx, parsed.BlockNumber, bs.Hash, true)
	if err != nil {
		if parentCancelled(ctx) {
			return nil, r.cancelled(), true
		}
		return nil, r.fatal(key, "rpc-error", err), true
	}
	data, rpcErr := pickField(bd, parsed.DataType)
	if rpcErr != nil {
		if parentCancelled(ctx) {
			return nil, r.cancelled(), true
		}
		return nil, r.fatal(key, "rpc-error", rpcErr), true
	}
	if rpcutil.IsNullOrEmpty(data) {
		return nil, r.fatal(key, "rpc-still-null", rpcutil.ErrUpstreamNullResult), true
	}
	return data, Outcome{}, false
}

// writeAndPublish handles dry-run short-circuit, S3 overwrite, and SNS publish,
// recording progress at each stage so a resumed run can skip already-completed
// work.
//
// Corner case CC-1 (100% SNS coverage on partial-completion):
// If WriteFile succeeds but the immediately-following recordState(StageS3) fails
// (e.g. disk full), the run aborts fatal and the state file ends with no record
// for this key. The verify phase's verifyNonNull branch covers the resume — it
// returns synthetic StageS3, this function then skips the WriteFile and
// proceeds straight to Publish + recordState(StageSNS). That guarantees every
// key in the operator's input list gets a SNS event even when our S3 write
// landed but our state record didn't.
func (r *Refiller) writeAndPublish(ctx context.Context, key string, bs *outbound.BlockState, data json.RawMessage, priorStage Stage) Outcome {
	if r.dryRun {
		return r.success(key, StageDryRun)
	}

	if priorStage != StageS3 {
		if err := r.s3Overwriter.WriteFile(ctx, r.bucket, key, bytes.NewReader(data), true); err != nil {
			if parentCancelled(ctx) {
				return r.cancelled()
			}
			return r.fatal(key, "s3-write-error", err)
		}
		// Corner case CC-1: a recordState failure here leaves S3 healed
		// without a state record; the verify phase covers it on resume.
		if out := r.success(key, StageS3); out.Fatal {
			return out
		}
	}

	event := outbound.BlockEvent{
		ChainID:        r.chainID,
		BlockNumber:    bs.Number,
		Version:        bs.Version,
		BlockHash:      bs.Hash,
		ParentHash:     bs.ParentHash,
		BlockTimestamp: bs.BlockTimestamp,
		ReceivedAt:     time.Unix(bs.ReceivedAt, 0).UTC(),
	}
	if err := r.publisher.Publish(ctx, event); err != nil {
		if parentCancelled(ctx) {
			return r.cancelled()
		}
		return r.fatal(key, "sns-error", err)
	}
	return r.success(key, StageSNS)
}

// parentCancelled reports whether the parent context (the one signal-bound at
// the top of Run) has been cancelled or timed out. Checking the parent ctx
// directly avoids classifying per-fetch derived-timeout errors (e.g.
// fetchCtx in verifyStillNull) as "cancellation" — a slow S3 read should
// surface as a real failure, not a silent skip.
func parentCancelled(ctx context.Context) bool {
	return ctx.Err() != nil
}

// verifyStillNull streams the existing S3 object and reports whether the
// content is still null, non-null, or missing. NoSuchKey is reported distinctly
// so the caller can flag operator misconfiguration (e.g. wrong --bucket) rather
// than silently treating every key as already-healed.
func (r *Refiller) verifyStillNull(ctx context.Context, key string) (verifyResult, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, r.fetchTimeout)
	defer cancel()

	reader, err := r.s3Reader.StreamFile(fetchCtx, r.bucket, key)
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return verifyNotFound, nil
		}
		return verifyNonNull, fmt.Errorf("stream s3 object: %w", err)
	}
	defer reader.Close()

	raw, err := io.ReadAll(reader)
	if err != nil {
		return verifyNonNull, fmt.Errorf("read s3 object: %w", err)
	}
	// StreamFile auto-decompresses .gz keys, so the buffer is plain JSON.
	// Defensive: also pass through gziputil in case the bucket layout changes.
	raw, err = gziputil.Decompress(raw)
	if err != nil {
		return verifyNonNull, fmt.Errorf("decompress s3 object: %w", err)
	}
	if rpcutil.IsNullOrEmpty(json.RawMessage(raw)) {
		return verifyNull, nil
	}
	return verifyNonNull, nil
}

// pickField extracts the json.RawMessage and per-field RPC error for dataType.
// dataType is assumed to be one of the known DataType enum values; s3key.Parse
// rejects unknown values, so callers operating on parsed keys never reach the
// default branch.
func pickField(bd outbound.BlockData, dataType s3key.DataType) (json.RawMessage, error) {
	switch dataType {
	case s3key.Block:
		return bd.Block, bd.BlockErr
	case s3key.Receipts:
		return bd.Receipts, bd.ReceiptsErr
	case s3key.Traces:
		return bd.Traces, bd.TracesErr
	case s3key.Blobs:
		return bd.Blobs, bd.BlobsErr
	default:
		return nil, fmt.Errorf("unknown dataType %q", dataType)
	}
}

// cancelled reports an interrupted Process call without writing a state record.
// The empty Stage signals to the worker loop and summary that this key was not
// processed and should be retried when the operator resumes the run.
func (r *Refiller) cancelled() Outcome {
	return Outcome{}
}

// recordState centralises the state.Record + structured-error-log pattern
// shared by skip/fail/success. Callers that observe a non-nil return must
// escalate to fatal — local-disk failures mean every subsequent key would
// also fail to persist. fatal itself records best-effort and never returns
// the recordState error (the original cause is what matters).
func (r *Refiller) recordState(key string, stage Stage, reason string) error {
	if err := r.state.Record(key, stage, reason); err != nil {
		r.logger.Error("state Record failed", "key", key, "stage", stage, "reason", reason, "error", err)
		return err
	}
	return nil
}

func (r *Refiller) skip(key, reason string) Outcome {
	if err := r.recordState(key, StageSkip, reason); err != nil {
		return r.fatal(key, "state-write-error", err)
	}
	return Outcome{Stage: StageSkip, Reason: reason}
}

// success records the stage and returns a successful Outcome. On state-write
// failure it returns a fatal Outcome so the caller aborts the run — local
// disk failures mean every subsequent key would also fail to persist.
func (r *Refiller) success(key string, stage Stage) Outcome {
	if err := r.recordState(key, stage, ""); err != nil {
		return r.fatal(key, "state-write-error", err)
	}
	return Outcome{Stage: stage}
}

// fatal records a run-stopping failure to state (best-effort) and returns an
// Outcome with Fatal=true so the worker loop aborts the run instead of
// continuing to other keys. If the state write itself fails we still want to
// abort — log the secondary error via recordState but do not recurse: the
// returned Outcome carries the original cause.
func (r *Refiller) fatal(key, reason string, cause error) Outcome {
	_ = r.recordState(key, StageFail, reason)
	return Outcome{Stage: StageFail, Reason: reason, Err: cause, Fatal: true}
}
