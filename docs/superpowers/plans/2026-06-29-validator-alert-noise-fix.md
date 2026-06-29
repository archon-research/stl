# Data Validator Alert Noise Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the `VectorCronjobRunFailing` Slack spam from the per-chain `watcher-data-validator` cronjobs by treating a transient canonical-source (Etherscan) outage as an inconclusive check rather than a run failure, and by staggering the per-chain schedules so they stop colliding on the shared Etherscan rate limit.

**Architecture:** Two independent changes. (1) Root-cause fix: the Etherscan adapter tags retry-exhausted transient failures with a new port-level sentinel error; the validator records those checks as `skipped` instead of `error`, so a throttled fetch no longer flips a whole run red. A genuine hash mismatch (`failed`) or a real local-data/config error (`error`) still fails the run. (2) Defence in depth: the shared Temporal cronjob scheduler gains a per-job interval offset, set per chain in the k8s configmaps, and now reconciles an already-created schedule so the offset actually takes effect on redeploy.

**Tech Stack:** Go 1.26, Temporal Go SDK, Kustomize (k8s overlays), slog, golang.org/x/time/rate (unchanged).

## Global Constraints

- Go 1.26+; output any built binaries to `stl/dist`.
- Hexagonal dependency rule: domain/service depend only on ports; adapters may import ports; never import an adapter from the service layer.
- Never modify an existing migration file. (No migrations in this plan.)
- Errors: wrap with `fmt.Errorf("...: %w", err)`; never ignore errors; fail hard on unexpected errors; panic only in `cmd`/`main`.
- Tests: table-driven, one behaviour per test, mock outbound ports for unit tests; services need both unit and integration tests; integration tests may only mock data sources we do not control (Etherscan).
- Comments explain *why*, not *what*; default to none.
- Style/copy rule: do not use the em dash character anywhere in code, comments, commit messages, or config.
- After the substantive change, run the parallel review phase defined in CLAUDE.md before declaring done.

## File Structure

- `stl-verify/internal/ports/outbound/block_verifier.go` — add the `ErrCanonicalSourceUnavailable` sentinel that the service uses to recognise a transient canonical-source failure.
- `stl-verify/internal/adapters/outbound/etherscan/client.go` — classify retry-exhausted transient HTTP failures and wrap them with the sentinel.
- `stl-verify/internal/services/data_validator/report.go` — add the `skipped` status and counter.
- `stl-verify/internal/services/data_validator/service.go` — map transient canonical errors to `skipped`; surface the skipped count in logs.
- `stl-verify/internal/adapters/outbound/temporal/temporal.go` — add interval-offset support and reconcile an existing schedule.
- `stl-verify/cmd/cronjobs/watcher-data-validator/main.go` — wire the offset env into `CronjobConfig`.
- `k8s/overlays/staging/configmaps.yaml`, `k8s/overlays/prod/configmaps.yaml` — per-chain `DATA_VALIDATION_SCHEDULE_OFFSET` values.

Tasks 1-4 (semantic fix) are independent of tasks 5-7 (staggering). Task 4 depends on 1-3; task 6 depends on 5; task 7 depends on 5-6.

---

### Task 1: Add the transient-source sentinel to the BlockVerifier port

**Files:**
- Modify: `stl-verify/internal/ports/outbound/block_verifier.go`
- Test: covered indirectly by tasks 2 and 4 (a bare sentinel needs no isolated test).

**Interfaces:**
- Produces: `outbound.ErrCanonicalSourceUnavailable` (an `error` value) used by the etherscan adapter (task 2) to wrap transient failures and by the data_validator service (task 4) via `errors.Is`.

- [ ] **Step 1: Add the sentinel error**

Edit `stl-verify/internal/ports/outbound/block_verifier.go`. Change the import line and add the sentinel below `CanonicalBlock`:

```go
package outbound

import (
	"context"
	"errors"
)

// ErrCanonicalSourceUnavailable marks a transient failure to reach the canonical
// chain source (rate-limit, timeout, 5xx) that survived the adapter's own retries.
// It means "we could not check this block right now", NOT "our stored data is
// wrong", so the validator records the check as skipped instead of failing the run.
var ErrCanonicalSourceUnavailable = errors.New("canonical source temporarily unavailable")

// CanonicalBlock represents a block from a canonical chain data source.
type CanonicalBlock struct {
```

(Leave the rest of the file unchanged.)

- [ ] **Step 2: Verify it compiles**

Run: `cd stl-verify && go build ./internal/ports/outbound/`
Expected: no output, exit 0.

- [ ] **Step 3: Commit**

```bash
git add stl-verify/internal/ports/outbound/block_verifier.go
git commit -m "feat(data-validator): add ErrCanonicalSourceUnavailable port sentinel"
```

---

### Task 2: Tag transient Etherscan failures with the sentinel

**Files:**
- Modify: `stl-verify/internal/adapters/outbound/etherscan/client.go` (function `doRequest`, around line 263; imports near line 7)
- Test: `stl-verify/internal/adapters/outbound/etherscan/client_test.go`

**Interfaces:**
- Consumes: `outbound.ErrCanonicalSourceUnavailable` (task 1), `httpclient.NonRetryableError`.
- Produces: `GetBlockByNumber` / `GetBlockByHash` / `GetLatestBlockNumber` now return an error that satisfies `errors.Is(err, outbound.ErrCanonicalSourceUnavailable)` when the underlying HTTP call failed transiently after exhausting retries. Permanent failures (non-retryable parse/API/4xx errors) are returned unchanged.

- [ ] **Step 1: Write the failing test**

Add to `stl-verify/internal/adapters/outbound/etherscan/client_test.go`. This drives a client at a mock server that always returns Etherscan's rate-limit body, so retries exhaust and the final error must be tagged transient:

```go
func TestGetBlockByNumber_RateLimitExhausted_IsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"0","message":"NOTOK","result":"Max calls per sec rate limit reached (3/sec)"}`))
	}))
	defer srv.Close()

	client, err := NewClient(ClientConfig{
		APIKey:         "test-key",
		ChainID:        1,
		BaseURL:        srv.URL,
		MaxRetries:     1,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	_, err = client.GetBlockByNumber(context.Background(), 100)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, outbound.ErrCanonicalSourceUnavailable) {
		t.Fatalf("expected ErrCanonicalSourceUnavailable, got %v", err)
	}
}

func TestGetBlockByNumber_APIError_IsNotTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"0","message":"NOTOK","result":"Invalid API Key"}`))
	}))
	defer srv.Close()

	client, err := NewClient(ClientConfig{APIKey: "test-key", ChainID: 1, BaseURL: srv.URL, MaxRetries: 1})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	_, err = client.GetBlockByNumber(context.Background(), 100)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if errors.Is(err, outbound.ErrCanonicalSourceUnavailable) {
		t.Fatalf("non-retryable API error must NOT be tagged transient, got %v", err)
	}
}
```

Ensure the test file imports `errors`, `net/http`, `net/http/httptest`, `time`, `context`, and the `outbound` port package. Check existing imports first and add only what is missing.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd stl-verify && go test ./internal/adapters/outbound/etherscan/ -run 'TestGetBlockByNumber_(RateLimitExhausted_IsTransient|APIError_IsNotTransient)' -v`
Expected: `TestGetBlockByNumber_RateLimitExhausted_IsTransient` FAILS ("expected ErrCanonicalSourceUnavailable"); the APIError test should already PASS.

- [ ] **Step 3: Classify the error in `doRequest`**

In `stl-verify/internal/adapters/outbound/etherscan/client.go`, add `"errors"` to the import block, then replace `doRequest` (currently lines 263-266):

```go
func (c *Client) doRequest(ctx context.Context, params url.Values, result any) error {
	fullURL := fmt.Sprintf("%s?%s", c.config.BaseURL, params.Encode())
	err := c.httpClient.DoRequest(ctx, httpclient.RequestConfig{URL: fullURL}, result)
	if err == nil {
		return nil
	}
	// A retryable failure that survived every retry (rate-limit, 5xx, network) means
	// the canonical source is temporarily unavailable, not that our stored data is
	// wrong. Tag it so the validator records an inconclusive check, not a hard error.
	// httpclient wraps permanent failures (4xx, parse errors, non-retryable API
	// errors) in NonRetryableError; those pass through untagged.
	var nonRetryable *httpclient.NonRetryableError
	if errors.As(err, &nonRetryable) {
		return err
	}
	return fmt.Errorf("%w: %w", outbound.ErrCanonicalSourceUnavailable, err)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd stl-verify && go test ./internal/adapters/outbound/etherscan/ -v`
Expected: PASS (all, including both new tests).

- [ ] **Step 5: Commit**

```bash
git add stl-verify/internal/adapters/outbound/etherscan/client.go stl-verify/internal/adapters/outbound/etherscan/client_test.go
git commit -m "feat(data-validator): tag retry-exhausted etherscan failures as transient"
```

---

### Task 3: Add the `skipped` status to the report

**Files:**
- Modify: `stl-verify/internal/services/data_validator/report.go`
- Test: `stl-verify/internal/services/data_validator/report_test.go` (create if absent)

**Interfaces:**
- Produces: `data_validator.StatusSkipped` constant (`"skipped"`); `Report.Skipped int` field, incremented by `AddCheck`; `Report.Success()` semantics unchanged (`Failed == 0 && Errors == 0`), so a skipped check does not fail a run. Consumed by task 4.

- [ ] **Step 1: Write the failing test**

Create or append to `stl-verify/internal/services/data_validator/report_test.go`:

```go
package data_validator

import "testing"

func TestReport_SkippedDoesNotFailRun(t *testing.T) {
	r := NewReport(0, 10)
	r.AddCheck(CheckResult{Name: "spot", Status: StatusPassed})
	r.AddCheck(CheckResult{Name: "spot2", Status: StatusSkipped})
	r.Finalize()

	if r.Skipped != 1 {
		t.Fatalf("Skipped = %d, want 1", r.Skipped)
	}
	if !r.Success() {
		t.Fatalf("Success() = false, want true (skipped must not fail the run)")
	}
}

func TestReport_FailedFailsRun(t *testing.T) {
	r := NewReport(0, 10)
	r.AddCheck(CheckResult{Name: "spot", Status: StatusFailed})
	r.Finalize()

	if r.Success() {
		t.Fatal("Success() = true, want false (a hash mismatch must fail the run)")
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd stl-verify && go test ./internal/services/data_validator/ -run 'TestReport_(SkippedDoesNotFailRun|FailedFailsRun)' -v`
Expected: FAIL to compile ("undefined: StatusSkipped", "r.Skipped undefined").

- [ ] **Step 3: Add the status and counter**

In `stl-verify/internal/services/data_validator/report.go`:

Add `Skipped` to the `Report` struct summary block (after `Errors int`):

```go
	// Summary statistics.
	Passed  int `json:"passed"`
	Failed  int `json:"failed"`
	Errors  int `json:"errors"`
	Skipped int `json:"skipped"`
```

Add the status constant to the `const` block:

```go
const (
	StatusPassed  = "passed"
	StatusFailed  = "failed"
	StatusError   = "error"
	StatusSkipped = "skipped"
)
```

Add the `case` to `AddCheck`:

```go
	case StatusError:
		r.Errors++
	case StatusSkipped:
		r.Skipped++
	}
```

In `FormatText`, add a skipped icon in the per-check switch (after the `StatusError` case):

```go
		case StatusError:
			statusIcon = "[ERROR]"
		case StatusSkipped:
			statusIcon = "[SKIPPED]"
		}
```

And include skipped in the summary line:

```go
	sb.WriteString(fmt.Sprintf("\nSUMMARY: %d passed, %d failed, %d errors, %d skipped\n",
		r.Passed, r.Failed, r.Errors, r.Skipped))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd stl-verify && go test ./internal/services/data_validator/ -run 'TestReport_' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add stl-verify/internal/services/data_validator/report.go stl-verify/internal/services/data_validator/report_test.go
git commit -m "feat(data-validator): add skipped check status to report"
```

---

### Task 4: Record transient canonical failures as skipped

**Files:**
- Modify: `stl-verify/internal/services/data_validator/service.go` (`spotCheckBlock` ~line 336, `validateSingleReorg` ~line 239, the completion log in `Validate` ~line 131)
- Test: `stl-verify/internal/services/data_validator/service_test.go`

**Interfaces:**
- Consumes: `outbound.ErrCanonicalSourceUnavailable` (task 1), `StatusSkipped` (task 3).
- Produces: the service's public `Validate(ctx)` now returns a `Report` where a transient canonical fetch error yields a `skipped` check (run stays `Success`), while a permanent canonical error, a local-DB error, a missing local block, a nil canonical block, or a hash mismatch keep their existing fail/error behaviour.

- [ ] **Step 1: Write the failing tests**

Add to `stl-verify/internal/services/data_validator/service_test.go`. Use the file's existing mock repository and mock verifier patterns; if the mock verifier does not yet allow a per-call error, extend it minimally (a configurable `GetBlockByNumberFunc` field is the lightest approach). The two behaviours:

```go
func TestValidate_TransientCanonicalError_SkipsCheckAndRunSucceeds(t *testing.T) {
	repo := newMockBlockStateRepo() // existing helper: a populated, integrity-valid range
	verifier := &mockBlockVerifier{
		name: "etherscan",
		getBlockByNumber: func(_ context.Context, _ int64) (*outbound.CanonicalBlock, error) {
			return nil, fmt.Errorf("fetching block: %w", outbound.ErrCanonicalSourceUnavailable)
		},
	}
	svc, err := NewService(DefaultConfig(), repo, verifier)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	report, err := svc.Validate(context.Background())
	if err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
	if report.Errors != 0 {
		t.Fatalf("Errors = %d, want 0 (transient must be skipped)", report.Errors)
	}
	if report.Skipped == 0 {
		t.Fatal("Skipped = 0, want > 0")
	}
	if !report.Success() {
		t.Fatal("Success() = false, want true (transient throttle must not fail the run)")
	}
}

func TestValidate_PermanentCanonicalError_RecordsErrorAndRunFails(t *testing.T) {
	repo := newMockBlockStateRepo()
	verifier := &mockBlockVerifier{
		name: "etherscan",
		getBlockByNumber: func(_ context.Context, _ int64) (*outbound.CanonicalBlock, error) {
			return nil, fmt.Errorf("API error: invalid api key")
		},
	}
	svc, err := NewService(DefaultConfig(), repo, verifier)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	report, err := svc.Validate(context.Background())
	if err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
	if report.Errors == 0 {
		t.Fatal("Errors = 0, want > 0 (a permanent canonical error is a real problem)")
	}
	if report.Success() {
		t.Fatal("Success() = true, want false")
	}
}
```

Match the exact mock type/field names already in `service_test.go`; the names above are illustrative. If a `mockBlockVerifier` already exists with fixed return values, add an optional func field rather than introducing a second mock type.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd stl-verify && go test ./internal/services/data_validator/ -run 'TestValidate_(TransientCanonicalError|PermanentCanonicalError)' -v`
Expected: FAIL (transient currently records an `error`, so `report.Errors` is 1 and `Success()` is false).

- [ ] **Step 3: Add a classification helper and apply it**

In `stl-verify/internal/services/data_validator/service.go`, add `"errors"` to the imports, then add a helper near the bottom of the file:

```go
// canonicalCheckStatus chooses the status for a failed canonical-source fetch.
// A transient outage (rate-limit, timeout, 5xx) is inconclusive, not a data
// discrepancy, so it is skipped rather than failing the whole run. A permanent
// error (bad key, parse failure) stays a hard error.
func canonicalCheckStatus(err error) string {
	if errors.Is(err, outbound.ErrCanonicalSourceUnavailable) {
		return StatusSkipped
	}
	return StatusError
}
```

In `spotCheckBlock`, change the canonical-fetch error branch (currently sets `Status: StatusError` after `s.blockVerifier.GetBlockByNumber`) to:

```go
	canonicalBlock, err := s.blockVerifier.GetBlockByNumber(ctx, blockNum)
	duration := time.Since(start)

	if err != nil {
		return CheckResult{
			Name:     name,
			Status:   canonicalCheckStatus(err),
			Message:  fmt.Sprintf("Failed to fetch from %s: %v", s.blockVerifier.Name(), err),
			Duration: duration,
		}
	}
```

(Leave the local-block fetch error, the local-block nil branch, the canonical nil branch, and the hash-mismatch branch exactly as they are: those remain `StatusError` / `StatusFailed`.)

In `validateSingleReorg`, change only the canonical-fetch error branch (after `s.blockVerifier.GetBlockByNumber(ctx, event.BlockNumber)`):

```go
	if err != nil {
		return CheckResult{
			Name:     name,
			Status:   canonicalCheckStatus(err),
			Message:  fmt.Sprintf("Failed to fetch block: %v", err),
			Duration: duration,
			Details: map[string]any{
				"reorg_id":     event.ID,
				"block_number": event.BlockNumber,
			},
		}
	}
```

(Leave the canonical-nil and hash-mismatch branches unchanged.)

In `Validate`, add the skipped count to the completion log:

```go
	s.logger.Info("validation complete",
		"passed", report.Passed,
		"failed", report.Failed,
		"errors", report.Errors,
		"skipped", report.Skipped,
		"duration", report.Duration,
	)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd stl-verify && go test ./internal/services/data_validator/ -v`
Expected: PASS (new tests plus all existing ones in the package).

- [ ] **Step 5: Update the integration test assertion if needed**

Open `stl-verify/internal/services/data_validator/service_integration_test.go`. If any assertion expects a specific run-failure on a mocked-Etherscan transient path, align it with the new skip behaviour. If the integration test only mocks Etherscan returning valid blocks (the happy path), no change is needed. Run:

Run: `cd stl-verify && go test ./internal/services/data_validator/ -tags integration -v` (use the repo's integration tag/target; check the Makefile `test-integration` target for the exact invocation)
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add stl-verify/internal/services/data_validator/service.go stl-verify/internal/services/data_validator/service_test.go stl-verify/internal/services/data_validator/service_integration_test.go
git commit -m "fix(data-validator): skip transient canonical failures instead of failing the run"
```

---

### Task 5: Add interval-offset support and schedule reconciliation to the cronjob scheduler

**Files:**
- Modify: `stl-verify/internal/adapters/outbound/temporal/temporal.go` (`CronjobConfig` struct ~line 55-79, `ensureSchedule` ~line 220-264)
- Test: `stl-verify/internal/adapters/outbound/temporal/temporal_test.go`

**Interfaces:**
- Produces: `CronjobConfig.IntervalOffsetEnv string` (optional; empty means no offset). A new pure helper `buildScheduleSpec(cfg CronjobConfig, getenv func(string) string) (client.ScheduleSpec, error)` returning a spec whose single `ScheduleIntervalSpec` carries `Every` and `Offset`. `ensureSchedule` now reconciles: on `AlreadyExists` it updates the existing schedule's spec so a changed interval or offset takes effect without manual deletion.
- Consumes: nothing new from other tasks. Consumed by task 6.

> Blast-radius note: `ensureSchedule` is shared by every Temporal cronjob (anchorage-indexer, offchain-price-indexer, etc.). The reconcile path changes how an existing schedule is treated on worker start (previously a no-op). Validate the exact Temporal Go SDK update API against the vendored SDK version before writing the code: confirm the field path for the schedule spec inside `client.ScheduleUpdateInput` and the shape of `client.ScheduleUpdate`. Grep the vendored SDK: `grep -rn "ScheduleUpdateInput\|type ScheduleUpdate \|func.*Update(ctx" $(go env GOMODCACHE)/go.temporal.io/sdk@*/client/`.

- [ ] **Step 1: Verify the Temporal SDK update API shape**

Run: `cd stl-verify && grep -rn "ScheduleUpdateInput\|type ScheduleUpdate struct\|ScheduleUpdateOptions\|GetHandle" $(go env GOMODCACHE)/go.temporal.io/sdk@*/client/ | head -40`
Expected: confirms `ScheduleUpdateInput.Description.Schedule.Spec` and `client.ScheduleUpdate{Schedule: *Schedule}`. Adjust the field paths in Step 4 if the vendored version differs.

- [ ] **Step 2: Write the failing test for spec building**

Add to `stl-verify/internal/adapters/outbound/temporal/temporal_test.go`:

```go
func TestBuildScheduleSpec_Offset(t *testing.T) {
	tests := []struct {
		name       string
		cfg        CronjobConfig
		env        map[string]string
		wantEvery  time.Duration
		wantOffset time.Duration
		wantErr    bool
	}{
		{
			name:       "no offset env configured",
			cfg:        CronjobConfig{IntervalDefault: "1h"},
			wantEvery:  time.Hour,
			wantOffset: 0,
		},
		{
			name:       "offset env set",
			cfg:        CronjobConfig{IntervalDefault: "1h", IntervalOffsetEnv: "OFFSET"},
			env:        map[string]string{"OFFSET": "5m"},
			wantEvery:  time.Hour,
			wantOffset: 5 * time.Minute,
		},
		{
			name:       "offset env empty falls back to zero",
			cfg:        CronjobConfig{IntervalDefault: "1h", IntervalOffsetEnv: "OFFSET"},
			env:        map[string]string{},
			wantEvery:  time.Hour,
			wantOffset: 0,
		},
		{
			name:    "invalid offset errors",
			cfg:     CronjobConfig{IntervalDefault: "1h", IntervalOffsetEnv: "OFFSET"},
			env:     map[string]string{"OFFSET": "not-a-duration"},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			getenv := func(k string) string { return tc.env[k] }
			spec, err := buildScheduleSpec(tc.cfg, getenv)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got := spec.Intervals[0]
			if got.Every != tc.wantEvery || got.Offset != tc.wantOffset {
				t.Fatalf("got {Every:%s Offset:%s}, want {Every:%s Offset:%s}",
					got.Every, got.Offset, tc.wantEvery, tc.wantOffset)
			}
		})
	}
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd stl-verify && go test ./internal/adapters/outbound/temporal/ -run TestBuildScheduleSpec_Offset -v`
Expected: FAIL to compile ("undefined: buildScheduleSpec", "IntervalOffsetEnv").

- [ ] **Step 4: Implement the offset field, the spec builder, and reconciliation**

In `stl-verify/internal/adapters/outbound/temporal/temporal.go`:

Add the field to `CronjobConfig` (after `IntervalDefault`):

```go
	// IntervalDefault is the default schedule interval (e.g. "5m", "1h").
	IntervalDefault string
	// IntervalOffsetEnv is the env var name for a per-job schedule offset
	// (optional). An offset phases the interval boundary so jobs sharing an
	// external rate limit do not all fire at the same wall-clock instant.
	IntervalOffsetEnv string
```

Extract the spec builder (pure, testable) and rewrite `ensureSchedule`:

```go
func buildScheduleSpec(cfg CronjobConfig, getenv func(string) string) (client.ScheduleSpec, error) {
	interval := cfg.IntervalDefault
	if cfg.IntervalEnv != "" {
		if v := getenv(cfg.IntervalEnv); v != "" {
			interval = v
		}
	}
	every, err := time.ParseDuration(interval)
	if err != nil {
		return client.ScheduleSpec{}, fmt.Errorf("parsing %s %q: %w", cfg.IntervalEnv, interval, err)
	}

	var offset time.Duration
	if cfg.IntervalOffsetEnv != "" {
		if v := getenv(cfg.IntervalOffsetEnv); v != "" {
			offset, err = time.ParseDuration(v)
			if err != nil {
				return client.ScheduleSpec{}, fmt.Errorf("parsing %s %q: %w", cfg.IntervalOffsetEnv, v, err)
			}
		}
	}

	return client.ScheduleSpec{
		Intervals: []client.ScheduleIntervalSpec{{Every: every, Offset: offset}},
	}, nil
}

func ensureSchedule(ctx context.Context, c client.Client, logger *slog.Logger, taskQueue string, cfg CronjobConfig) error {
	spec, err := buildScheduleSpec(cfg, os.Getenv)
	if err != nil {
		return err
	}

	scheduleID := cfg.Name
	workflowID := "scheduled-" + cfg.Name

	_, err = c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID:   scheduleID,
		Spec: spec,
		Action: &client.ScheduleWorkflowAction{
			Workflow:  cronjobWorkflow,
			ID:        workflowID,
			TaskQueue: taskQueue,
		},
	})
	if err == nil {
		logger.Info("schedule created", "scheduleID", scheduleID, "spec", spec.Intervals[0])
		return nil
	}

	if grpcstatus.Code(err) == codes.AlreadyExists || strings.Contains(err.Error(), "already registered") {
		// Reconcile so a changed interval or offset takes effect on redeploy
		// without a manual schedule deletion.
		return reconcileScheduleSpec(ctx, c, logger, scheduleID, spec)
	}
	return fmt.Errorf("creating schedule %q: %w", scheduleID, err)
}

func reconcileScheduleSpec(ctx context.Context, c client.Client, logger *slog.Logger, scheduleID string, want client.ScheduleSpec) error {
	handle := c.ScheduleClient().GetHandle(ctx, scheduleID)
	err := handle.Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			in.Description.Schedule.Spec = &want
			return &client.ScheduleUpdate{Schedule: &in.Description.Schedule}, nil
		},
	})
	if err != nil {
		return fmt.Errorf("reconciling schedule %q: %w", scheduleID, err)
	}
	logger.Info("schedule reconciled", "scheduleID", scheduleID, "spec", want.Intervals[0])
	return nil
}
```

Add `"os"` to the imports if not already present. Remove the now-stale comment block about interval changes requiring manual deletion (lines ~236-238), since reconciliation handles it.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd stl-verify && go test ./internal/adapters/outbound/temporal/ -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add stl-verify/internal/adapters/outbound/temporal/temporal.go stl-verify/internal/adapters/outbound/temporal/temporal_test.go
git commit -m "feat(cronjobs): support per-job schedule offset and reconcile existing schedules"
```

---

### Task 6: Wire the offset env into the validator cronjob

**Files:**
- Modify: `stl-verify/cmd/cronjobs/watcher-data-validator/main.go` (the `CronjobConfig` literal ~line 39-49)
- Test: `stl-verify/cmd/cronjobs/watcher-data-validator/main_test.go`

**Interfaces:**
- Consumes: `CronjobConfig.IntervalOffsetEnv` (task 5).
- Produces: the validator now reads `DATA_VALIDATION_SCHEDULE_OFFSET` for its schedule offset.

- [ ] **Step 1: Add the offset env to the config literal**

In `stl-verify/cmd/cronjobs/watcher-data-validator/main.go`, add one line to the `CronjobConfig` literal (after `IntervalDefault: "1h",`):

```go
		Name:              env.Get("SERVICE_NAME", "watcher-data-validator"),
		IntervalEnv:       "DATA_VALIDATION_INTERVAL",
		IntervalDefault:   "1h",
		IntervalOffsetEnv: "DATA_VALIDATION_SCHEDULE_OFFSET",
```

(Re-gofmt the struct so field alignment is correct.)

- [ ] **Step 2: Build to verify it compiles**

Run: `cd stl-verify && go build ./cmd/cronjobs/watcher-data-validator/`
Expected: no output, exit 0.

- [ ] **Step 3: Confirm existing main tests still pass**

Run: `cd stl-verify && go test ./cmd/cronjobs/watcher-data-validator/ -v`
Expected: PASS. (If `main_test.go` asserts the `CronjobConfig` contents, extend it to assert `IntervalOffsetEnv == "DATA_VALIDATION_SCHEDULE_OFFSET"`.)

- [ ] **Step 4: Commit**

```bash
git add stl-verify/cmd/cronjobs/watcher-data-validator/main.go stl-verify/cmd/cronjobs/watcher-data-validator/main_test.go
git commit -m "feat(data-validator): read DATA_VALIDATION_SCHEDULE_OFFSET for schedule staggering"
```

---

### Task 7: Stagger the per-chain schedules in k8s config

**Files:**
- Modify: `k8s/overlays/staging/configmaps.yaml` (validator ConfigMaps, lines ~183-234)
- Modify: `k8s/overlays/prod/configmaps.yaml` (matching validator ConfigMaps)

**Interfaces:**
- Consumes: `DATA_VALIDATION_SCHEDULE_OFFSET` env (task 6).

Offsets are chosen so the longest job (mainnet, ~3 min run) never overlaps the next. Spacing of 5 min leaves headroom for the three currently-disabled chains when they are enabled later.

| ConfigMap | CHAIN_ID | Offset |
|-----------|----------|--------|
| watcher-data-validator (mainnet) | 1 | `0s` |
| unichain-watcher-data-validator | 130 | `5m` |
| arbitrum-watcher-data-validator | 42161 | `10m` |
| optimism-watcher-data-validator | 10 | `15m` |
| base-watcher-data-validator | 8453 | `20m` |
| avalanche-watcher-data-validator | 43114 | `25m` |

- [ ] **Step 1: Add the offset to each validator ConfigMap (staging)**

In `k8s/overlays/staging/configmaps.yaml`, add a `DATA_VALIDATION_SCHEDULE_OFFSET` line to each validator `data:` block. Mainnet example (leave at `0s` for an explicit, self-documenting value):

```yaml
kind: ConfigMap
metadata:
  name: watcher-data-validator
data:
  TEMPORAL_HOST_PORT: "temporal-server.temporal:7233"
  TEMPORAL_NAMESPACE: "vector"
  CHAIN_ID: "1"
  DATA_VALIDATION_SCHEDULE_OFFSET: "0s"
```

Apply the table's value to each of the six validator ConfigMaps.

- [ ] **Step 2: Mirror the offsets in prod**

Apply the identical six `DATA_VALIDATION_SCHEDULE_OFFSET` values to the matching ConfigMaps in `k8s/overlays/prod/configmaps.yaml`.

- [ ] **Step 3: Verify both overlays build**

Run: `cd /Users/timonfloriangodt/projects/stl && kubectl kustomize k8s/overlays/staging > /dev/null && kubectl kustomize k8s/overlays/prod > /dev/null && echo OK`
Expected: `OK` (no kustomize errors).

- [ ] **Step 4: Confirm the env reaches the pods**

Run: `cd /Users/timonfloriangodt/projects/stl && kubectl kustomize k8s/overlays/staging | grep -A2 "DATA_VALIDATION_SCHEDULE_OFFSET" | head -20`
Expected: the six offset values appear in the rendered ConfigMaps. (If the validator Deployments use `envFrom: configMapRef`, the key is injected automatically; if they enumerate env keys explicitly, add `DATA_VALIDATION_SCHEDULE_OFFSET` to each Deployment's env list in `k8s/base/*-watcher-data-validator/deployment.yaml` and re-run this check.)

- [ ] **Step 5: Commit**

```bash
git add k8s/overlays/staging/configmaps.yaml k8s/overlays/prod/configmaps.yaml
git commit -m "deploy(data-validator): stagger per-chain validator schedules"
```

---

### Task 8: Full verification and review

**Files:** none (verification only).

- [ ] **Step 1: Run the full unit suite with the race detector**

Run: `cd stl-verify && make test-race`
Expected: PASS.

- [ ] **Step 2: Run integration tests**

Run: `cd stl-verify && make test-integration`
Expected: PASS.

- [ ] **Step 3: Run CI checks**

Run: `cd stl-verify && make ci-checks`
Expected: PASS, except the known pre-existing go1.26.3 stdlib vulncheck finding (see memory `ci-vulncheck-toolchain`); do not treat that single finding as a regression.

- [ ] **Step 4: Spawn the CLAUDE.md review phase**

Spawn, in parallel (one message, multiple Agent calls): `pr-review-toolkit:code-reviewer`, `pr-review-toolkit:silent-failure-hunter`, a general-purpose architecture reviewer, and a general-purpose code-quality reviewer. Scope each to the files changed in tasks 1-7. Tailored checklist must include: (a) silent-failure-hunter verifies the skip path does not mask a genuine data discrepancy (only `ErrCanonicalSourceUnavailable` is skipped; mismatches and local errors still fail); (b) architecture reviewer verifies the sentinel lives in the port and the adapter (not the service) classifies, with no adapter import leaking into the service; (c) code-reviewer checks the reconcile path against the Temporal SDK and the no-em-dash rule. Apply blocking and should-fix items before declaring done; surface nice-to-haves to the user.

- [ ] **Step 5: Open the PR**

```bash
git push -u origin <branch>
gh pr create --title "VEC-208 follow-up: stop data-validator alert noise" --body "<summary of root cause, semantic fix, and staggering; link this plan>"
```

---

## Operational rollout notes (for the PR description)

- The semantic fix alone stops the spam: transient Etherscan throttles become `skipped`, so a run no longer goes red on a recovered throttle. Staggering removes the cross-job contention that produced most throttles in the first place.
- Schedule reconciliation (task 5) means the new offsets take effect on the next worker rollout with no manual Temporal schedule deletion. Watch the first post-deploy run of each validator in the Temporal UI (vector namespace) to confirm the new fire times.
- A total Etherscan outage now yields an all-`skipped`, still-green run rather than a red one. That is a deliberate trade to kill the false alerts. Detecting a sustained all-skipped state belongs to the deferred validator alerts/runbook follow-up (per #435), not this PR.
- Out of scope (tracked separately, per decision): reducing mainnet's ~194 calls/run by bounding reorg re-validation to a recent window.

## Self-Review

- Spec coverage: semantic fix (tasks 1-4), staggering with working deploy (tasks 5-7), verification + mandated review (task 8). The "would staggering alone solve it" finding is reflected by making the semantic fix the primary lever and staggering defence-in-depth. Covered.
- Placeholder scan: SDK field paths in task 5 are gated behind an explicit verification step (5.1) rather than assumed; mock names in task 4 are flagged as illustrative with instructions to match the existing file. No bare TODOs.
- Type consistency: `ErrCanonicalSourceUnavailable` (task 1) is used identically in tasks 2 and 4; `StatusSkipped` / `Report.Skipped` (task 3) used in task 4; `IntervalOffsetEnv` / `buildScheduleSpec` (task 5) used in task 6; `DATA_VALIDATION_SCHEDULE_OFFSET` consistent across tasks 6 and 7.
