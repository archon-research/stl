# Multi-chain Data Validator Implementation Plan (VEC-208)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the `watcher-data-validator` cronjob for every chain we ingest (Ethereum, Optimism, Unichain, Base, Arbitrum, Avalanche C-Chain), each emitting its own per-chain report, while leaving Ethereum behaviour unchanged.

**Architecture:** The validator service is already chain-agnostic (it depends only on the `BlockVerifier` and `BlockStateRepository` ports). We add a registry-driven verifier factory that routes a chain ID to the right canonical-source adapter (Etherscan V2 for all six chains today, with a clean seam for RPC or non-EVM adapters later), make the Temporal cronjob name chain-distinct so each chain gets its own schedule and task queue, and deploy one cronjob per chain following the repo's existing per-chain manifest pattern (`base-watcher`, `optimism-watcher`, etc.).

**Tech Stack:** Go 1.26, Etherscan V2 multichain API, Temporal cronjobs, Kustomize, External Secrets, testcontainers (Postgres).

---

## Background: why each piece exists

- **Service is already chain-agnostic.** `data_validator.NewService` (`stl-verify/internal/services/data_validator/service.go:58`) takes `outbound.BlockVerifier` and `outbound.BlockStateRepository`. No Etherscan-specific assumptions; the only cosmetic Etherscan references are doc comments.
- **Etherscan adapter is already multichain.** `etherscan.NewClient` (`stl-verify/internal/adapters/outbound/etherscan/client.go:91`) uses the V2 endpoint `https://api.etherscan.io/v2/api` and passes `chainid` on every call (`client.go:160,181,198`). One endpoint, one API key, chain selected by parameter. `ClientConfig.BaseURL` is overridable (used by the integration test).
- **Temporal name must be chain-distinct.** `temporal.RunCronjob` derives the task queue (`temporal.go:137`), schedule ID (`temporal.go:206`), and workflow ID (`temporal.go:207`) all from `cfg.Name`. Six pods sharing the hardcoded name `"watcher-data-validator"` would collide on one schedule and task queue. Each chain needs a distinct name; mainnet keeps `"watcher-data-validator"` so its schedule is byte-identical and unchanged.
- **Per-chain k8s pattern is established.** The repo deploys per-chain copies as separate base directories (`k8s/base/base-watcher`, `k8s/base/avalanche-watcher`, etc.), each with its own configmap (`CHAIN_ID`) and ExternalSecret, wired into both overlays. The shared image is referenced by a Kustomize images-transformer key equal to the deployment name. We follow this exactly.
- **No infra-repo dependency.** The validator uses no AWS data-plane resources (no S3/SNS/SQS). Etherscan V2 uses a single API key shared across chains, and the per-chain `ETHERSCAN_API_KEY`/`DATABASE_URL` ExternalSecrets reference secrets that already exist (`stl-sentinel{staging,prod}-etherscan-api-key`, `stl-sentinel{staging,prod}-tigerdata-app`). External Secrets are synced by the cluster operator, not by pod IAM, so no new IAM/Pod Identity is required. Confirm with infra, but the work proceeds independently.

## Chain reference table

| Chain | CHAIN_ID | k8s name prefix | Notes |
|---|---|---|---|
| Ethereum mainnet | 1 | (none) `watcher-data-validator` | exists today; unchanged |
| Optimism | 10 | `optimism-watcher-data-validator` | OP Stack L2 |
| Unichain | 130 | `unichain-watcher-data-validator` | OP Stack L2 |
| Base | 8453 | `base-watcher-data-validator` | OP Stack L2 |
| Arbitrum One | 42161 | `arbitrum-watcher-data-validator` | Arbitrum Nitro L2 |
| Avalanche C-Chain | 43114 | `avalanche-watcher-data-validator` | sovereign EVM L1 |

## File structure

**Go (new):**
- `stl-verify/internal/adapters/outbound/blockverifier/factory.go`, registry + factory `New(chainID, Options) (outbound.BlockVerifier, error)`.
- `stl-verify/internal/adapters/outbound/blockverifier/factory_test.go`, unit tests for routing.
- `stl-verify/internal/services/data_validator/service_integration_test.go`, integration test for a non-Ethereum chain (Base, 8453) against real Postgres with a mock Etherscan server.

**Go (modify):**
- `stl-verify/cmd/cronjobs/watcher-data-validator/main.go`, build the verifier via the factory; read the cronjob name from `SERVICE_NAME` (default `watcher-data-validator`).
- `stl-verify/internal/services/data_validator/service.go`, doc-comment cleanup to use chain-neutral language (no behaviour change).

**k8s (new base dirs, 3 files each):**
- `k8s/base/{optimism,unichain,base,arbitrum,avalanche}-watcher-data-validator/{deployment,serviceaccount,kustomization}.yaml`

**k8s (modify overlays, staging and prod):**
- `k8s/overlays/{staging,prod}/kustomization.yaml`, +5 resources, +5 images.
- `k8s/overlays/{staging,prod}/configmaps.yaml`, +5 ConfigMaps.
- `k8s/overlays/{staging,prod}/external-secrets.yaml`, +5 ExternalSecrets.

**Explicitly out of scope:** building an RPC or non-EVM verifier. All six chains we currently ingest are supported by Etherscan V2 (confirmed against the official chain list, see Task 0), so every chain uses the Etherscan path. A separate verifier adapter is future work, to be picked up only when we need to validate a chain that Etherscan does not cover (see "Deferred / future"). Also out of scope: changing the watcher or other services, new database tables (validator is read-only), and moving hash normalisation out of the service.

---

## Task 0: Confirm the Etherscan V2 proxy module answers for each chain

**Already established:** the official Etherscan V2 chain list (`https://api.etherscan.io/v2/chainlist`) lists all six chains we ingest as supported, including Avalanche C-Chain (43114) and Unichain (130). So no chain needs a separate adapter for this ticket; every chain uses `KindEtherscan`.

The only thing left to confirm is a runtime detail: that the `proxy` module (the exact calls the validator makes) returns a block for each chain, since "listed as supported" and "proxy module enabled" can differ. This is a cheap sanity check, not a blocker for starting Tasks 1 to 4.

**Files:** none (spike).

- [ ] **Step 1: Query the proxy module for each chain ID using the staging key**

Fetch the staging Etherscan key locally (or use any valid V2 key):

```bash
cd stl-verify
# Option A: pull from AWS if you have access
ETHERSCAN_API_KEY=$(aws secretsmanager get-secret-value \
  --secret-id stl-sentinelstaging-etherscan-api-key \
  --query SecretString --output text --region eu-west-1)

for CID in 1 10 130 8453 42161 43114; do
  echo "chain $CID:"
  curl -s "https://api.etherscan.io/v2/api?chainid=$CID&module=proxy&action=eth_blockNumber&apikey=$ETHERSCAN_API_KEY"
  echo
done
```

Expected: each chain returns `{"jsonrpc":"2.0","id":...,"result":"0x..."}` (a hex block number), not an error like `"Invalid chainid"`.

- [ ] **Step 2: Record the result**

All six are expected to return a block number; proceed with the plan as written (every chain uses `KindEtherscan`). In the unlikely event a chain's proxy module errors, do not build an RPC adapter inside this ticket: drop that single chain from scope, note it in the PR, and raise a follow-up ticket for it (see "Deferred / future"). The factory already returns a clear startup error for any chain not in its registry, so omitting a chain fails safe.

---

## Task 1: Verifier factory package

**Files:**
- Create: `stl-verify/internal/adapters/outbound/blockverifier/factory.go`
- Test: `stl-verify/internal/adapters/outbound/blockverifier/factory_test.go`

- [ ] **Step 1: Write the failing test**

Create `stl-verify/internal/adapters/outbound/blockverifier/factory_test.go`:

```go
package blockverifier

import (
	"strings"
	"testing"
)

func TestNew_KnownChainsReturnEtherscanVerifier(t *testing.T) {
	chains := []int64{1, 10, 130, 8453, 42161, 43114}
	for _, chainID := range chains {
		t.Run(strings.ReplaceAll(formatChain(chainID), " ", "_"), func(t *testing.T) {
			v, err := New(chainID, Options{EtherscanAPIKey: "test-key"})
			if err != nil {
				t.Fatalf("New(%d) returned error: %v", chainID, err)
			}
			if v == nil {
				t.Fatalf("New(%d) returned nil verifier", chainID)
			}
			if got := v.Name(); got != "etherscan" {
				t.Fatalf("New(%d) verifier Name() = %q, want %q", chainID, got, "etherscan")
			}
		})
	}
}

func TestNew_UnknownChainErrors(t *testing.T) {
	_, err := New(999999, Options{EtherscanAPIKey: "test-key"})
	if err == nil {
		t.Fatal("New(999999) expected error, got nil")
	}
	if !strings.Contains(err.Error(), "no block verifier configured") {
		t.Fatalf("New(999999) error = %q, want it to mention 'no block verifier configured'", err.Error())
	}
}

func TestNew_MissingEtherscanKeyErrors(t *testing.T) {
	_, err := New(1, Options{EtherscanAPIKey: ""})
	if err == nil {
		t.Fatal("New(1) with empty key expected error, got nil")
	}
	if !strings.Contains(err.Error(), "etherscan API key required") {
		t.Fatalf("New(1) error = %q, want it to mention 'etherscan API key required'", err.Error())
	}
}

func formatChain(chainID int64) string {
	return "chain " + itoa(chainID)
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd stl-verify && go test ./internal/adapters/outbound/blockverifier/...`
Expected: FAIL to compile with "undefined: New" / "undefined: Options".

- [ ] **Step 3: Write the factory implementation**

Create `stl-verify/internal/adapters/outbound/blockverifier/factory.go`:

```go
// Package blockverifier selects and constructs the right outbound.BlockVerifier
// for a given chain. It is the chain-routing seam for the data validator: the
// service depends only on the BlockVerifier port, and this factory decides which
// concrete adapter backs that port for each chain we ingest. Adding a future
// chain (including a non-EVM one) is a registry entry plus, if needed, a new
// adapter behind the same port, the service never changes.
package blockverifier

import (
	"fmt"
	"log/slog"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/etherscan"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Kind identifies which canonical-source adapter backs a chain.
type Kind string

const (
	// KindEtherscan uses the Etherscan V2 multichain API: one endpoint, one key,
	// chain selected via the chainid query parameter.
	KindEtherscan Kind = "etherscan"
)

// chainKind maps each chain we ingest to the adapter kind that verifies it.
// A chain not present here is rejected at startup so a misconfigured deployment
// fails loudly rather than silently validating nothing.
var chainKind = map[int64]Kind{
	1:     KindEtherscan, // Ethereum mainnet
	10:    KindEtherscan, // Optimism
	130:   KindEtherscan, // Unichain
	8453:  KindEtherscan, // Base
	42161: KindEtherscan, // Arbitrum One
	43114: KindEtherscan, // Avalanche C-Chain
}

// Options carries the credentials and overrides the factory needs to build a
// verifier. A field is used only by the adapter kinds that require it.
type Options struct {
	// EtherscanAPIKey is the Etherscan V2 API key (shared across all chains).
	EtherscanAPIKey string

	// EtherscanBaseURL overrides the Etherscan V2 endpoint. Empty uses the
	// adapter default. Tests set this to a mock server URL.
	EtherscanBaseURL string

	// Logger is the structured logger passed to the adapter.
	Logger *slog.Logger
}

// New returns the BlockVerifier for chainID, or an error if the chain is not
// configured or required credentials are missing.
func New(chainID int64, opts Options) (outbound.BlockVerifier, error) {
	kind, ok := chainKind[chainID]
	if !ok {
		return nil, fmt.Errorf("no block verifier configured for chain ID %d", chainID)
	}

	switch kind {
	case KindEtherscan:
		return newEtherscanVerifier(chainID, opts)
	default:
		return nil, fmt.Errorf("unsupported verifier kind %q for chain ID %d", kind, chainID)
	}
}

func newEtherscanVerifier(chainID int64, opts Options) (outbound.BlockVerifier, error) {
	if opts.EtherscanAPIKey == "" {
		return nil, fmt.Errorf("etherscan API key required for chain ID %d", chainID)
	}
	client, err := etherscan.NewClient(etherscan.ClientConfig{
		APIKey:  opts.EtherscanAPIKey,
		ChainID: chainID,
		BaseURL: opts.EtherscanBaseURL, // empty => adapter default
		Logger:  opts.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating etherscan verifier for chain ID %d: %w", chainID, err)
	}
	return client, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd stl-verify && go test ./internal/adapters/outbound/blockverifier/...`
Expected: PASS (all three tests).

- [ ] **Step 5: Commit**

```bash
git add stl-verify/internal/adapters/outbound/blockverifier/
git commit -m "VEC-208: add chain-routing block verifier factory"
```

---

## Task 2: Wire the factory and chain-distinct Temporal name into main.go

**Files:**
- Modify: `stl-verify/cmd/cronjobs/watcher-data-validator/main.go`

- [ ] **Step 1: Change the Temporal cronjob name to come from SERVICE_NAME**

In `main()`, replace the hardcoded `Name` field. Find:

```go
	}, temporal.CronjobConfig{
		Name:            "watcher-data-validator",
		IntervalEnv:     "DATA_VALIDATION_INTERVAL",
```

Replace with:

```go
	}, temporal.CronjobConfig{
		// SERVICE_NAME is injected per deployment from the pod's app label so each
		// per-chain validator registers its own Temporal schedule and task queue.
		// Defaults to "watcher-data-validator" so the Ethereum mainnet deployment
		// (which sets no SERVICE_NAME) keeps its existing schedule ID unchanged.
		Name:            env.Get("SERVICE_NAME", "watcher-data-validator"),
		IntervalEnv:     "DATA_VALIDATION_INTERVAL",
```

- [ ] **Step 2: Build the verifier via the factory in setupRunner**

Find the Etherscan construction in `setupRunner`:

```go
	etherscanClient, err := etherscan.NewClient(etherscan.ClientConfig{
		APIKey:  etherscanAPIKey,
		ChainID: int64(chainID),
		Logger:  deps.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating etherscan client: %w", err)
	}

	blockStateRepo := postgres.NewBlockStateRepository(deps.Pool, int64(chainID), deps.Logger)

	service, err := data_validator.NewService(
		data_validator.DefaultConfig(),
		blockStateRepo,
		etherscanClient,
	)
```

Replace with:

```go
	verifier, err := blockverifier.New(int64(chainID), blockverifier.Options{
		EtherscanAPIKey: etherscanAPIKey,
		Logger:          deps.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating block verifier: %w", err)
	}

	blockStateRepo := postgres.NewBlockStateRepository(deps.Pool, int64(chainID), deps.Logger)

	service, err := data_validator.NewService(
		data_validator.DefaultConfig(),
		blockStateRepo,
		verifier,
	)
```

- [ ] **Step 3: Fix imports**

In the import block, remove the now-unused etherscan import and add the factory:

Remove:
```go
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/etherscan"
```
Add:
```go
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/blockverifier"
```

- [ ] **Step 4: Verify it builds and vets**

Run: `cd stl-verify && go build ./... && go vet ./cmd/cronjobs/watcher-data-validator/...`
Expected: no output (success).

- [ ] **Step 5: Commit**

```bash
git add stl-verify/cmd/cronjobs/watcher-data-validator/main.go
git commit -m "VEC-208: route validator verifier via factory and per-chain Temporal name"
```

---

## Task 3: Chain-neutral language in the service

Cosmetic only; addresses the ticket's "comparison logic should not assume an Etherscan-specific source" by removing Etherscan-specific wording from the public surface. No behaviour change.

**Files:**
- Modify: `stl-verify/internal/services/data_validator/service.go`

- [ ] **Step 1: Update the package and method doc comments**

Find (line 1):
```go
// Package data_validator provides validation of block chain data stored by the watcher.
// It verifies reorg events against Etherscan's canonical chain and validates chain integrity.
```
Replace with:
```go
// Package data_validator provides validation of block chain data stored by the watcher.
// It verifies reorg events against a canonical chain source (provided via the
// BlockVerifier port) and validates chain integrity. The source is chain-specific
// and selected by the caller; the service itself is chain-agnostic.
```

Find (line 192):
```go
// validateReorgs validates each reorg event against Etherscan.
```
Replace with:
```go
// validateReorgs validates each reorg event against the canonical chain source.
```

Find (line 224):
```go
// validateSingleReorg validates a single reorg event against Etherscan.
```
Replace with:
```go
// validateSingleReorg validates a single reorg event against the canonical chain source.
```

Find (line 236):
```go
	// Fetch the canonical block from Etherscan
```
Replace with:
```go
	// Fetch the canonical block from the verifier source
```

Find (line 257) the message `"Block not found on Etherscan"` and replace with a source-neutral message that uses the verifier name:
```go
				Message:  fmt.Sprintf("Block not found on %s", s.blockVerifier.Name()),
```

Find (line 321):
```go
// spotCheckBlock verifies a single block's hash against Etherscan.
```
Replace with:
```go
// spotCheckBlock verifies a single block's hash against the canonical chain source.
```

- [ ] **Step 2: Run the existing service tests to confirm no regression**

Run: `cd stl-verify && go test ./internal/services/data_validator/...`
Expected: PASS (existing tests unchanged in behaviour).

- [ ] **Step 3: Commit**

```bash
git add stl-verify/internal/services/data_validator/service.go
git commit -m "VEC-208: make data validator wording chain-neutral"
```

---

## Task 4: Integration test for a non-Ethereum chain

Exercises factory routing (chain 8453 -> Etherscan) + the Etherscan adapter + the service + real Postgres, with only the third-party API mocked. Satisfies the acceptance criterion "integration tests cover at least one non-Ethereum chain".

**Files:**
- Create: `stl-verify/internal/services/data_validator/service_integration_test.go`

First inspect the existing in-package test helpers and the `BlockStateRepository` integration setup used elsewhere so this test reuses the established testcontainers Postgres harness rather than inventing one.

- [ ] **Step 1: Find the existing Postgres testcontainers helper and BlockStateRepository schema**

Run:
```bash
cd stl-verify
grep -rn "testcontainers\|postgres.NewBlockStateRepository\|func.*Postgres.*testing" internal/adapters/outbound/postgres/*_test.go | head
grep -rln "//go:build integration" internal/adapters/outbound/postgres/ internal/services/
```
Expected: identify the helper that starts a Postgres container and applies migrations (for example a `setupTestDB(t)` returning a `*pgxpool.Pool`), and the build tag string used for integration tests.

- [ ] **Step 2: Write the integration test**

Create `stl-verify/internal/services/data_validator/service_integration_test.go`. Use the same `//go:build` integration tag string discovered in Step 1 (shown here as `integration`). Replace `postgrestest.SetupPool` / migration helper with the actual helper found in Step 1; the structure below is the target:

```go
//go:build integration

package data_validator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/blockverifier"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/services/data_validator"
	// postgrestest "<module>/internal/adapters/outbound/postgres" or the shared
	// testcontainers helper package identified in Step 1.
)

const baseChainID int64 = 8453

// mockEtherscanV2 returns an httptest server that answers the V2 proxy calls the
// Etherscan adapter makes (eth_blockNumber, eth_getBlockByNumber) for any chainid,
// echoing back the hashes seeded into Postgres so the validator passes.
func mockEtherscanV2(t *testing.T, hashByNumber map[int64]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		action := q.Get("action")
		w.Header().Set("Content-Type", "application/json")
		switch action {
		case "eth_blockNumber":
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":"0x100"}`)
		case "eth_getBlockByNumber":
			var number int64
			fmt.Sscanf(q.Get("tag"), "0x%x", &number)
			hash, ok := hashByNumber[number]
			if !ok {
				fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":null}`)
				return
			}
			resp := map[string]any{
				"jsonrpc": "2.0", "id": 1,
				"result": map[string]string{
					"number":    fmt.Sprintf("0x%x", number),
					"hash":      hash,
					"timestamp": "0x65a1234",
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
		default:
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":null}`)
		}
	}))
}

func TestValidate_BaseChain_Integration(t *testing.T) {
	ctx := context.Background()

	// 1. Real Postgres via the shared testcontainers helper (from Step 1).
	pool := setupTestDB(t) // returns *pgxpool.Pool with migrations applied

	// 2. Seed block_state rows for chain 8453 with a known parent-linked chain.
	//    Use the same insert path the watcher uses, or direct SQL matching the
	//    block_state schema. Record the hashes so the mock can echo them.
	hashByNumber := seedLinkedBlocks(t, ctx, pool, baseChainID, 1, 50)

	// 3. Mock Etherscan V2 echoing those hashes.
	server := mockEtherscanV2(t, hashByNumber)
	defer server.Close()

	// 4. Build the verifier through the factory (proves routing for 8453) but
	//    point it at the mock server.
	verifier, err := blockverifier.New(baseChainID, blockverifier.Options{
		EtherscanAPIKey:  "test-key",
		EtherscanBaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("factory New(%d): %v", baseChainID, err)
	}

	repo := postgres.NewBlockStateRepository(pool, baseChainID, nil)

	svc, err := data_validator.NewService(data_validator.DefaultConfig(), repo, verifier)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	report, err := svc.Validate(ctx)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	report.Finalize()

	if !report.Success() {
		t.Fatalf("expected success, got %d failed / %d errors:\n%s",
			report.Failed, report.Errors, report.FormatText())
	}
}
```

Implement `seedLinkedBlocks` to insert `block_state` rows for the given chain ID with consistent `parent_hash` linkage and return a `map[int64]string` of block number to hash. Match the exact `block_state` columns the `BlockStateRepository` reads (inspect `internal/adapters/outbound/postgres/blockstate_repository.go` for `GetBlockByNumber`, `GetMinBlockNumber`, `GetMaxBlockNumber`, `VerifyChainIntegrity`, `GetReorgEventsByBlockRange`). Seed zero reorg events so the reorg check passes trivially.

- [ ] **Step 3: Run the integration test**

Run: `cd stl-verify && make test-integration` (or `go test -tags integration ./internal/services/data_validator/... -run TestValidate_BaseChain_Integration -v`)
Expected: PASS. Requires Docker.

- [ ] **Step 4: Commit**

```bash
git add stl-verify/internal/services/data_validator/service_integration_test.go
git commit -m "VEC-208: integration test validating a non-Ethereum chain (Base)"
```

---

## Task 5: Per-chain k8s base directories

Create five base directories following the `k8s/base/base-watcher` pattern. Each is identical except for the chain name in three places (metadata name, app label, serviceAccountName, container name, image key) and the configmap/secret names. The `CHAIN_ID` lives in the overlay configmaps (Task 6/7), not here.

**Files (create all 15):**
- `k8s/base/optimism-watcher-data-validator/{deployment,serviceaccount,kustomization}.yaml`
- `k8s/base/unichain-watcher-data-validator/{deployment,serviceaccount,kustomization}.yaml`
- `k8s/base/base-watcher-data-validator/{deployment,serviceaccount,kustomization}.yaml`
- `k8s/base/arbitrum-watcher-data-validator/{deployment,serviceaccount,kustomization}.yaml`
- `k8s/base/avalanche-watcher-data-validator/{deployment,serviceaccount,kustomization}.yaml`

- [ ] **Step 1: Create the five base directories with a generator loop**

Run this from the repo root. It writes all three files for each chain, substituting the name. SERVICE_NAME is injected from the pod app label so the Temporal schedule ID equals the deployment name.

```bash
cd /Users/timonfloriangodt/projects/stl
for NAME in optimism-watcher-data-validator unichain-watcher-data-validator base-watcher-data-validator arbitrum-watcher-data-validator avalanche-watcher-data-validator; do
  mkdir -p "k8s/base/$NAME"

  cat > "k8s/base/$NAME/deployment.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $NAME
spec:
  replicas: 1
  selector:
    matchLabels:
      app: $NAME
  template:
    metadata:
      labels:
        app: $NAME
    spec:
      serviceAccountName: $NAME
      containers:
        - name: $NAME
          # Image name used as the Kustomize images transformer key.
          # The overlay kustomization.yaml overrides newName + newTag.
          image: $NAME
          env:
            # Inject the deployment name so this validator registers its own
            # Temporal schedule/task queue (cfg.Name) distinct from other chains.
            - name: SERVICE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.labels['app']
          envFrom:
            - configMapRef:
                name: $NAME
            - secretRef:
                name: $NAME
          resources:
            requests:
              cpu: "50m"
              memory: "64Mi"
            limits:
              cpu: "200m"
              memory: "256Mi"
      terminationGracePeriodSeconds: 60
EOF

  cat > "k8s/base/$NAME/serviceaccount.yaml" <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: $NAME
EOF

  cat > "k8s/base/$NAME/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - serviceaccount.yaml
  - deployment.yaml
EOF
done
```

- [ ] **Step 2: Verify each base kustomization builds in isolation**

Run:
```bash
cd /Users/timonfloriangodt/projects/stl
for NAME in optimism unichain base arbitrum avalanche; do
  kubectl kustomize "k8s/base/${NAME}-watcher-data-validator" >/dev/null && echo "$NAME OK"
done
```
Expected: `optimism OK` ... `avalanche OK` (no YAML errors).

- [ ] **Step 3: Commit**

```bash
git add k8s/base/optimism-watcher-data-validator k8s/base/unichain-watcher-data-validator \
        k8s/base/base-watcher-data-validator k8s/base/arbitrum-watcher-data-validator \
        k8s/base/avalanche-watcher-data-validator
git commit -m "VEC-208: add per-chain watcher-data-validator k8s base manifests"
```

---

## Task 6: Staging overlay wiring

**Files:**
- Modify: `k8s/overlays/staging/kustomization.yaml`
- Modify: `k8s/overlays/staging/configmaps.yaml`
- Modify: `k8s/overlays/staging/external-secrets.yaml`

- [ ] **Step 1: Add the five base resources**

In `k8s/overlays/staging/kustomization.yaml`, find:
```yaml
  - ../../base/watcher-data-validator
```
Add immediately after it:
```yaml
  - ../../base/optimism-watcher-data-validator
  - ../../base/unichain-watcher-data-validator
  - ../../base/base-watcher-data-validator
  - ../../base/arbitrum-watcher-data-validator
  - ../../base/avalanche-watcher-data-validator
```

- [ ] **Step 2: Add the five image entries**

In `k8s/overlays/staging/kustomization.yaml`, find the existing block:
```yaml
  - name: watcher-data-validator
    newName: 579039992622.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelstaging-cronjob
    newTag: "watcher-data-validator-4504f179622d9ca4251a4c767f0f213cbecd70e0"
```
Add immediately after it (all five share the cronjob image and the same tag; CI manages the tag thereafter):
```yaml
  - name: optimism-watcher-data-validator
    # Shares the cronjob image, same binary, different chain config via env vars
    newName: 579039992622.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelstaging-cronjob
    newTag: "watcher-data-validator-4504f179622d9ca4251a4c767f0f213cbecd70e0"
  - name: unichain-watcher-data-validator
    # Shares the cronjob image, same binary, different chain config via env vars
    newName: 579039992622.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelstaging-cronjob
    newTag: "watcher-data-validator-4504f179622d9ca4251a4c767f0f213cbecd70e0"
  - name: base-watcher-data-validator
    # Shares the cronjob image, same binary, different chain config via env vars
    newName: 579039992622.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelstaging-cronjob
    newTag: "watcher-data-validator-4504f179622d9ca4251a4c767f0f213cbecd70e0"
  - name: arbitrum-watcher-data-validator
    # Shares the cronjob image, same binary, different chain config via env vars
    newName: 579039992622.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelstaging-cronjob
    newTag: "watcher-data-validator-4504f179622d9ca4251a4c767f0f213cbecd70e0"
  - name: avalanche-watcher-data-validator
    # Shares the cronjob image, same binary, different chain config via env vars
    newName: 579039992622.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelstaging-cronjob
    newTag: "watcher-data-validator-4504f179622d9ca4251a4c767f0f213cbecd70e0"
```

- [ ] **Step 3: Add the five ConfigMaps**

In `k8s/overlays/staging/configmaps.yaml`, find the existing `watcher-data-validator` ConfigMap block:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: watcher-data-validator
data:
  TEMPORAL_HOST_PORT: "temporal-server.temporal:7233"
  TEMPORAL_NAMESPACE: "vector"
  CHAIN_ID: "1"
```
Add immediately after it (note the `---` separators and each chain's ID):
```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: optimism-watcher-data-validator
data:
  TEMPORAL_HOST_PORT: "temporal-server.temporal:7233"
  TEMPORAL_NAMESPACE: "vector"
  CHAIN_ID: "10"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: unichain-watcher-data-validator
data:
  TEMPORAL_HOST_PORT: "temporal-server.temporal:7233"
  TEMPORAL_NAMESPACE: "vector"
  CHAIN_ID: "130"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: base-watcher-data-validator
data:
  TEMPORAL_HOST_PORT: "temporal-server.temporal:7233"
  TEMPORAL_NAMESPACE: "vector"
  CHAIN_ID: "8453"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: arbitrum-watcher-data-validator
data:
  TEMPORAL_HOST_PORT: "temporal-server.temporal:7233"
  TEMPORAL_NAMESPACE: "vector"
  CHAIN_ID: "42161"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: avalanche-watcher-data-validator
data:
  TEMPORAL_HOST_PORT: "temporal-server.temporal:7233"
  TEMPORAL_NAMESPACE: "vector"
  CHAIN_ID: "43114"
```

- [ ] **Step 4: Add the five ExternalSecrets**

In `k8s/overlays/staging/external-secrets.yaml`, find the existing `watcher-data-validator` ExternalSecret block (ends with the `ETHERSCAN_API_KEY` remoteRef referencing `stl-sentinelstaging-etherscan-api-key`). Add immediately after it, one block per chain. They are identical except for `metadata.name` and `target.name`. Repeat this block for each of: `optimism-watcher-data-validator`, `unichain-watcher-data-validator`, `base-watcher-data-validator`, `arbitrum-watcher-data-validator`, `avalanche-watcher-data-validator`:
```yaml
---
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: optimism-watcher-data-validator
spec:
  refreshInterval: 3m
  secretStoreRef:
    name: aws-secrets-manager
    kind: ClusterSecretStore
  target:
    name: optimism-watcher-data-validator
    creationPolicy: Owner
    deletionPolicy: Retain
  data:
    - secretKey: DATABASE_URL
      remoteRef:
        key: stl-sentinelstaging-tigerdata-app
        property: pooler_url
        conversionStrategy: Default
        decodingStrategy: None
        metadataPolicy: None
    - secretKey: ETHERSCAN_API_KEY
      remoteRef:
        key: stl-sentinelstaging-etherscan-api-key
        conversionStrategy: Default
        decodingStrategy: None
        metadataPolicy: None
```
(Then the same for `unichain-`, `base-`, `arbitrum-`, `avalanche-watcher-data-validator`: copy the block, change both `name:` and `target.name:` to the chain's service name. All five reference the same two staging secrets.)

- [ ] **Step 5: Verify the staging overlay builds**

Run: `cd /Users/timonfloriangodt/projects/stl && kubectl kustomize k8s/overlays/staging >/dev/null && echo "staging OK"`
Expected: `staging OK` (no errors; all image keys resolve, no dangling references).

- [ ] **Step 6: Commit**

```bash
git add k8s/overlays/staging/kustomization.yaml k8s/overlays/staging/configmaps.yaml k8s/overlays/staging/external-secrets.yaml
git commit -m "VEC-208: wire per-chain data validators into staging overlay"
```

---

## Task 7: Prod overlay wiring

Mirror Task 6 with prod values: ECR account `030797368798`, repo `stl-sentinelprod-cronjob`, tag `watcher-data-validator-7abeb809148bd7cf9698592bfb16a350597885fc`, secrets `stl-sentinelprod-tigerdata-app` and `stl-sentinelprod-etherscan-api-key`.

**Files:**
- Modify: `k8s/overlays/prod/kustomization.yaml`
- Modify: `k8s/overlays/prod/configmaps.yaml`
- Modify: `k8s/overlays/prod/external-secrets.yaml`

- [ ] **Step 1: Add the five base resources**

In `k8s/overlays/prod/kustomization.yaml`, find `- ../../base/watcher-data-validator` and add the same five `- ../../base/<chain>-watcher-data-validator` lines after it (identical to Task 6 Step 1).

- [ ] **Step 2: Add the five image entries (prod values)**

In `k8s/overlays/prod/kustomization.yaml`, find:
```yaml
  - name: watcher-data-validator
    newName: 030797368798.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelprod-cronjob
    newTag: "watcher-data-validator-7abeb809148bd7cf9698592bfb16a350597885fc"
```
Add five entries after it, one per chain name (`optimism-`, `unichain-`, `base-`, `arbitrum-`, `avalanche-watcher-data-validator`), each:
```yaml
  - name: <chain>-watcher-data-validator
    # Shares the cronjob image, same binary, different chain config via env vars
    newName: 030797368798.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelprod-cronjob
    newTag: "watcher-data-validator-7abeb809148bd7cf9698592bfb16a350597885fc"
```

- [ ] **Step 3: Add the five ConfigMaps**

In `k8s/overlays/prod/configmaps.yaml`, find the `watcher-data-validator` ConfigMap (CHAIN_ID "1") and add the same five ConfigMap blocks as Task 6 Step 3 (identical content: TEMPORAL_HOST_PORT, TEMPORAL_NAMESPACE, and the per-chain CHAIN_ID values 10, 130, 8453, 42161, 43114).

- [ ] **Step 4: Add the five ExternalSecrets (prod secrets)**

In `k8s/overlays/prod/external-secrets.yaml`, find the `watcher-data-validator` ExternalSecret and add five blocks after it, identical to Task 6 Step 4 but referencing `stl-sentinelprod-tigerdata-app` and `stl-sentinelprod-etherscan-api-key`.

- [ ] **Step 5: Verify the prod overlay builds**

Run: `cd /Users/timonfloriangodt/projects/stl && kubectl kustomize k8s/overlays/prod >/dev/null && echo "prod OK"`
Expected: `prod OK`.

- [ ] **Step 6: Commit**

```bash
git add k8s/overlays/prod/kustomization.yaml k8s/overlays/prod/configmaps.yaml k8s/overlays/prod/external-secrets.yaml
git commit -m "VEC-208: wire per-chain data validators into prod overlay"
```

---

## Task 8: Full CI and final verification

**Files:** none (verification).

- [ ] **Step 1: Run the full Go CI suite**

Run: `cd stl-verify && make ci`
Expected: PASS (test-race, vet, fmt-check, tidy-check, staticcheck, vulncheck, golangci-lint). If `vulncheck` fails on a stdlib CVE unrelated to this change, note it (a known toolchain item) but do not let unrelated failures mask new ones.

- [ ] **Step 2: Run integration tests**

Run: `cd stl-verify && make test-integration`
Expected: PASS, including `TestValidate_BaseChain_Integration`. Requires Docker.

- [ ] **Step 3: Build both overlays one more time**

Run:
```bash
cd /Users/timonfloriangodt/projects/stl
kubectl kustomize k8s/overlays/staging >/dev/null && echo "staging OK"
kubectl kustomize k8s/overlays/prod >/dev/null && echo "prod OK"
```
Expected: both `OK`.

- [ ] **Step 4: Confirm there is no stray formatting/tidy diff**

Run: `cd stl-verify && git status --porcelain`
Expected: empty (everything committed; CI fails on an uncommitted tidy/fmt diff).

---

## Review phase (CLAUDE.md mandated)

After the tasks above and before declaring done, spawn the reviewer subagents in parallel (single message, multiple Agent calls), each with the file list and a tailored checklist:

1. **pr-review-toolkit:code-reviewer**, project conventions, error wrapping, `-er`/`New` naming, snake_case files, no adapter import in service/domain.
2. **pr-review-toolkit:silent-failure-hunter**, the factory error paths, the mock-server test, the main.go wiring; confirm unknown-chain and missing-key cases fail loudly.
3. **General-purpose, architecture brief**, verify the factory sits at the adapter/composition layer, the service stays dependency-free of concrete adapters, and the BlockVerifier port remains the only chain seam.
4. **General-purpose, code-quality brief**, factory design, registry extensibility, DRY across the k8s manifests, premature-abstraction check on the `Kind` enum.
5. **pr-review-toolkit:pr-test-analyzer**, coverage of routing logic and the non-Ethereum integration test; missing edge cases (block-not-found, hash mismatch surfacing as failure).

Apply Blocking and Should-fix items before finishing. Surface Nice-to-have items to the user.

## PR

- Branch: `timon/vec-208-extend-data-validator-to-support-all-other-chains` (Linear-generated).
- Title: `VEC-208: Run data validator for every ingested chain`.
- In the PR description, include the CONTRIBUTING section 5 data-source justification: the validator needs an independent canonical source to cross-check our stored block data, Etherscan V2 is that source, used read-only and only for validation, sharing one key across chains.
- Optionally split the k8s overlay changes (Tasks 6 and 7) into a follow-up deploy PR to keep the code review narrow, per CONTRIBUTING section 8 tip.

## Deferred / future (not in this ticket)

- **Hash normalisation into adapters.** The service's `hashesMatch` (`service.go:420`) lowercases and strips `0x`, which is correct for all six EVM chains. A future non-EVM chain (for example a base58-hash chain) would make normalisation chain-specific; at that point each adapter should return an already-canonicalised hash and the service should switch to plain string equality. Not needed now (YAGNI) since every ingested chain is EVM and the port already isolates the service from chain specifics.
- **RPC / non-EVM verifier (future ticket, not VEC-208).** Every chain we ingest today is served by Etherscan V2, so no second verifier is built here. The factory's `Kind` switch and registry are the seam: when we first need to validate a chain that Etherscan does not cover, a follow-up ticket adds a new `Kind` plus an adapter implementing `outbound.BlockVerifier`, and points the affected chain's registry entry at it. No change to the service is required. The likely starting point is a thin wrapper over the existing Alchemy JSON-RPC client (`internal/adapters/outbound/alchemy/client.go`), which already implements `eth_getBlockByNumber` / `eth_blockNumber` but does not yet satisfy the `BlockVerifier` signatures. Independence caveat for that future work: the watcher already ingests from Alchemy, so a verifier pointed at the same provider only catches our own processing bugs, not bad upstream data. Point it at a node independent of the watcher's for that chain. See "Appendix A: future-work sketch".
- **Hash normalisation into adapters.** Already listed above; same future-work bucket, only relevant once a non-EVM chain arrives.

---

## Appendix A: Future-work sketch, a chain not covered by Etherscan V2

Not part of VEC-208. This is a sketch for the future ticket that adds the first non-Etherscan chain, so the seam's intended shape is on record. Do not implement it as part of this ticket; if Task 0's proxy check ever fails for a chain, drop that chain from VEC-208's scope and raise this as its own ticket.

- **Add an RPC-backed verifier adapter.** Create `stl-verify/internal/adapters/outbound/rpcverifier/client.go` implementing `outbound.BlockVerifier` by calling `eth_getBlockByNumber` / `eth_blockNumber` over the chain's JSON-RPC HTTP URL, returning `outbound.CanonicalBlock{Number, Hash, Timestamp}`. Reuse the Alchemy client or mirror the etherscan client's retry/rate-limit use of `internal/pkg/httpclient`, and prefer a node independent of the watcher's for genuine cross-checking.
- **Register the kind in the factory.** In `factory.go`, add `KindRPC Kind = "rpc"`, set the affected chain's `chainKind` entry to `KindRPC`, add an `RPCBaseURL` field to `Options`, and add a `case KindRPC:` to `New`. Add the RPC URL to that chain's overlay configmap and pass it through `main.go`.
- **Test and wire.** Add factory unit tests asserting the chain routes to a verifier whose `Name()` is the RPC verifier's name, and add an integration test for that chain.
