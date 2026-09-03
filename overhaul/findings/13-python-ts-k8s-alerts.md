Status: DRAFT — investigation in progress

# 13 — Python / TS / k8s / alerts survey

(Draft checkpoint: metrics gathered, findings 1-8 drafted below. Still to finish: final
open-questions pass, cross-area observations consolidation, word-count trim of summary.)

## 1. Area map

- **stl-verify/python** — FastAPI service (`app/`) serving reference/allocation/risk data,
  plus one Temporal cronjob (`cli/cronjobs/core_model_runner/`) and CLI scripts. Hexagonal
  layout mirrors Go: `domain/`, `ports/`, `services/`, `adapters/{postgres,onchain,temporal,parquet}/`,
  `api/v1/`. Three risk models live under `app/risk_engine/` (pure math, no I/O): `suraf`
  (asset-level rating, CSV inputs under top-level `suraf/inputs/ratings/`), `crypto_lending`
  (discriminator `gap_sweep`), `core_model` (Monte-Carlo liquidation simulator). No Python
  per-block worker exists yet (CONTRIBUTING.md §8 confirms). `app/risk_engine/_vendored_synome/`
  is a vendored entity-registry package; it is the source of truth for `contracts/axis-synome/*.json`,
  which Go also reads (`internal/pkg/axis_synome_contract/loader.go`) — a genuine generated
  cross-language contract.
- **stl-verify/ts** — React UI (npm workspaces: `ui`, `mocks`), frontend only per AGENTS.md.
  `mocks` is a well-built typed MSW layer generated off the UI's own `openapi-types.ts`.
- **k8s/** — Kustomize `base/` (56 dirs, one per binary-per-chain) + `overlays/{dev,staging,prod}`.
  No k8s `CronJob` objects anywhere — Temporal "cronjobs" are plain `Deployment`s that serve a
  task queue forever. `stl-verify/k8s/apps` is a different, unrelated tree (Argo CD Application
  manifests), not a duplicate of `k8s/`.
- **alerts/ + docs/runbooks/** — 6 Prometheus rule files / 79 rules, 8 runbook docs, paired by
  filename convention with one documented, deliberate exception (transform-worker).

```
Go workers (SQS/SNS/Temporal, cmd/) --writes--> Postgres <--reads-- stl-verify/python (FastAPI)
                                                                          |
                                                          make export-openapi-schema
                                                                          v
                                                                 stl-verify/ts (React UI)
```

## 2. Metrics

| Area | Files | Lines | Notes |
|---|---|---|---|
| python `app/` | 166 | 22,814 | domain 1740, ports 676, services 2310, adapters 6660, api 4432, risk_engine 6264 |
| python `tests/` | 144 | 29,228 | unit 94 files, integration 48 files — bigger than `app/` itself |
| python `cli/` + `scripts/` | 6 | 204 | one Temporal cronjob + one OpenAPI-export script |
| ts `ui/` | ~78 src + tests | 21,087 | 6 test files total in `ui/src` |
| ts `mocks/` | — | 5,612 | typed MSW fixtures/handlers |
| k8s `base/` | 56 dirs / 54 `deployment.yaml` | 2,729 (deployments only) | 34 of 56 dirs (61%) are per-chain copies of 5 binary shapes |
| alerts/ | 6 files | 79 rules (23 critical / 55 warning / 1 info) | |
| docs/runbooks/ | 8 docs | — | |

**Largest Python files/functions:**
| File | Lines | |
|---|---|---|
| `app/adapters/postgres/allocation_position_repository.py` | 1883 | 21 methods + ~8 trailing module-level SQL constants |
| `app/risk_engine/_vendored_synome/spec/entities/assets.py` | 1102 | vendored data table, not logic |
| `app/api/v1/allocations.py` | 1038 | |
| `app/risk_engine/core_model/liquidator.py` | 747 | contains the 446-line function below |

| Function (AST span) | Lines | File:start |
|---|---|---|
| `simulate_liquidations` | 446 | `app/risk_engine/core_model/liquidator.py:300` |
| `create_app` | 206 | `app/main.py:174` |
| `build_axis_synome_contract` | 160 | `app/risk_engine/_vendored_synome/export_entities.py:159` |
| `list_exposure_buckets` | 144 | `app/adapters/postgres/allocation_position_repository.py:984` |

**Largest TS files:** `ui/src/generated/openapi-types.ts` (3308, generated), `AllocationGrid.tsx` (1628,
no test file), `mocks/scripts/check-mock-api.ts` (1559), `ActivityFeed.tsx` (1262, no test file),
`dashboard.ts`/`dashboard.test.ts` (915/1077 — the one large module that *is* well tested).

Ports (Python): 15 files under `app/ports/`; adapters: `postgres`, `onchain`, `temporal`, `parquet`.

## 3. Findings

### F13.1 — Two independent definitions of `MAINNET_CHAIN_ID` in the same service
**Strength**: Strong
**Files**: `stl-verify/python/app/domain/chain_names.py:23`, `stl-verify/python/app/services/core_model_risk_service.py:44`, `stl-verify/python/app/main.py:45`
**Problem**: `chain_names.py` defines `MAINNET_CHAIN_ID = 1` with a docstring and is the file that
`tests/unit/test_chain_names.py` guards against drift from Go's `entity.ChainIDToName`. Separately,
`core_model_risk_service.py:44` redefines `MAINNET_CHAIN_ID = 1` locally instead of importing the
guarded constant, and `main.py:45` imports the constant from `core_model_risk_service`, not from
`chain_names`. Two sources of truth for the same value, only one of which is under test-guarded
lockstep with Go.
**Proposed change**: Delete the local redefinition in `core_model_risk_service.py`; import from
`app.domain.chain_names`.
**Benefits**: One definition, already covered by `test_chain_names.py`'s cross-language guard.
**Risk / migration**: Trivial, one import line change.
**Size**: S

### F13.2 — `simulate_liquidations` is a 446-line function (60% of its file)
**Strength**: Worth exploring
**Files**: `stl-verify/python/app/risk_engine/core_model/liquidator.py:300-746`
**Problem**: Single method covers margin-call triggering/curing, "count bad debt once" EAD
accounting, recovery accounting, feasibility/profitability checks, and slippage — each named as a
distinct concern in its own docstring, but not factored into separate functions. `Liquidator` overall
is 747 lines; this one method is 60% of it.
**Proposed change**: Split along the docstring's own seams (margin-call step, execution/feasibility
step, bad-debt/recovery accounting step) into named helpers taking/returning the shared vectorized
state; keep orchestration in `simulate_liquidations`.
**Benefits**: Each accounting rule becomes independently testable (today only the whole simulation
is testable); smaller diffs when one rule changes.
**Risk / migration**: Numerical code — any refactor needs before/after output-equality tests over
fixed seeds before splitting. Real risk of subtly changing vectorized semantics.
**Size**: M

### F13.3 — `AllocationRepository` is an 1883-line, 21-method god file
**Strength**: Worth exploring
**Files**: `stl-verify/python/app/adapters/postgres/allocation_position_repository.py:1-1883`
**Problem**: One repository class backs essentially every allocation/exposure/total-capital query
(chains, protocols, primes, receipt-token positions, direct holdings, custody holdings, usd
exposure, activity buckets, total-capital buckets, exposure buckets, proxy addresses...). ~1090
lines of class body plus ~790 lines of trailing module-level SQL-text constants referenced by the
methods above them (forward references, valid Python but non-obvious to a reader scanning top to
bottom).
**Proposed change**: Split by read-model (positions vs custody vs activity/exposure buckets vs
total-capital) into sibling repositories or at least sibling modules; co-locate each SQL constant
directly above/inside the method that uses it instead of a shared tail block.
**Benefits**: Locality — a change to activity-bucket SQL no longer requires scrolling past 20
unrelated methods; smaller PRs.
**Risk / migration**: Pure reorganization if done by cut-paste; watch for the module-level SQL
constants that are shared between two methods (check before splitting).
**Size**: M

### F13.4 — Shared `FakeRiskModel` test double was introduced once, then forked into two divergent copies
**Strength**: Strong
**Files**: `stl-verify/python/tests/unit/test_risk_model_port.py:22`, `stl-verify/python/tests/unit/test_model_registry.py:15`; historical: `stl-verify/python/tests/unit/fakes/fake_risk_model.py` (commit `c4ab2867`, "Introduce RiskModel port and shared RrcResult type")
**Problem**: Commit `c4ab2867` added `tests/unit/fakes/fake_risk_model.py` as *the* shared test
double for the `RiskModel` port. That file no longer exists in the tree (only stale
`tests/unit/fakes/__pycache__/fake_risk_model.cpython-312.pyc` remains — `git ls-files` returns
nothing for the directory). In its place, two test files each hand-roll their own `class
FakeRiskModel` with different constructors (one takes a pre-built `RrcResult`; the other builds one
inline per `risk_model` name via if/elif) — exactly the "test-double proliferation" pattern called
out as a general risk in the investigation brief.
**Proposed change**: Restore one shared fake under `tests/unit/fakes/`, parameterized to cover both
call shapes, and delete the two inline copies.
**Benefits**: One place to extend when a new model/discriminator is added to the port.
**Risk / migration**: Low; used only by two test files today.
**Size**: S

### F13.5 — 61% of k8s `base/` is copy-paste per-chain duplication with no Kustomize reuse mechanism
**Strength**: Strong
**Files**: `k8s/base/{watcher,arbitrum-watcher,optimism-watcher,unichain-watcher,...}/deployment.yaml`; `k8s/overlays/{prod,staging}/kustomization.yaml:6-75`
**Problem**: `watcher`/`backup-worker`/`allocation-tracker`/`watcher-data-validator`/`psm3-indexer`
each exist in 4-8 chain variants (34 of 56 base dirs total). Diffing `watcher` vs `arbitrum-watcher`
vs `optimism-watcher` shows the *only* differences are name substitution (`watcher` →
`arbitrum-watcher`) plus, occasionally, one extra arg (`--enable-traces=false`). Each overlay
(`prod`, `staging`) lists all 34 as separate `resources:` lines by hand. The repo already has and
uses a Kustomize `Component` mechanism (`k8s/overlays/dev/components/runtime/`) — but only for a
local-runtime env/imagePullPolicy patch, never to collapse the per-chain name-substitution
duplication.
**Proposed change**: Generate the per-chain bases with a Kustomize component/patch
(`namePrefix`/`nameSuffix` + label transformer + a JSON patch for the one differing arg) driven off
a short list of chain names, instead of 34 hand-copied directories; or a small script that renders
them (similar to `scripts/deploy/render-overlay-images.sh`'s relationship to `image-roster.txt`).
**Benefits**: Adding a 6th chain today means creating up to 5 new 3-file directories by hand and
adding 5 new `resources:` lines to 2-3 overlay files; with a generator/component it becomes one
list entry.
**Risk / migration**: L — many manifests, needs careful before/after `kubectl kustomize` diffing
per overlay to prove byte-identical output before cutover; ArgoCD SSA field-ownership history
(see `k8s/AGENTS.md` rollout-strategy notes on #640) makes this sensitive.
**Size**: L

### F13.6 — Alert CI has a hardcoded 6-file allowlist; a 7th file is silently dropped
**Strength**: Strong
**Files**: `.github/workflows/alerts.yml` (`namespaces-regex: '^vector-(watcher|backup-worker|indexers|psm3|cronjobs|database)$'`, both `staging` and `prod` jobs), `alerts/vector-cronjobs.yaml:338-450`
**Problem**: `alerts/AGENTS.md` says "Rules → a group in `alerts/vector-<service>.yaml`," implying
any new file is fine. In practice the sync workflow only pushes files matching a hardcoded regex of
6 names. `vector-cronjobs.yaml:338-347` has a comment explaining that transform-worker's rules had
to be folded into `vector-cronjobs.yaml` instead of their own file "because... `.github/workflows/alerts.yml`
does not accept a `vector-transform-worker` file — a file would be silently dropped." The matching
runbook (`docs/runbooks/vector-transform-worker.md`) *is* its own file, so the alert-runbook
pairing convention is asymmetric for this one service, for a CI reason not mentioned in
`alerts/AGENTS.md`.
**Proposed change**: Either generalize the regex (e.g. glob `vector-*` in `rules-dir`), or add an
explicit note to `alerts/AGENTS.md` that new alert files must also be added to this regex in two
places (staging + prod jobs).
**Benefits**: Removes a silent-drop footgun; one PR forgetting the regex update currently fails
open (rules just don't sync, no error).
**Risk / migration**: S — confirm the mimir-rules-sync action supports a glob before widening it.
**Size**: S

### F13.7 — `VEC-277-root-cause-findings.md` at repo root duplicates a properly-filed incident doc
**Strength**: Strong
**Files**: `/VEC-277-root-cause-findings.md` (340 lines, root), `docs/incidents/2026-06-02-arbitrum-backfill-loop.md`
**Problem**: Both files were added in the same commit (`05c758cb`, "VEC-277: Handle the case where
blocks are out of order (#377)") and describe the same incident (Arbitrum watcher backfill loop,
same root cause: reorg-as-blanket-orphan + unrecoverable orphan state). The root-level file is
explicitly written as an agent-session artifact ("Status: root cause re-verified... this document is
written so another agent can independently re-verify each claim") — i.e. it is a debugging
scratchpad that was committed instead of being discarded or folded into the incident doc that
already exists in the conventional location.
**Proposed change**: Delete the root file (or fold anything from it not already in the incident doc
into that doc), since `docs/incidents/` is the established location.
**Benefits**: Removes a stray, confusing root-level doc; one canonical incident record.
**Risk / migration**: None — it's dead documentation weight.
**Size**: S

### F13.8 — `docs/superpowers/plans/` is an untracked agent-planning doc unrelated to `docs/`'s stated purpose
**Strength**: Worth exploring
**Files**: `docs/superpowers/plans/2026-07-09-blockpin-statereader-seam.md` (1121 lines, untracked)
**Problem**: Root `AGENTS.md` defines `docs/` as "architecture diagrams and entity relations."
This file is a task-execution plan for an external Claude Code skill ("superpowers:subagent-driven-development"),
untracked in git, sitting inside `docs/`. It documents real, still-relevant design decisions
(BlockPin/StateReader seam) that overlap with what the Go-side agents are separately investigating
(see cross-area note below) but is not discoverable as an ADR or doc per repo convention.
**Proposed change**: If the design decisions in it are still current, promote the relevant parts to
`docs/adr/`; otherwise remove it from the tree (it is agent scratch, not committed, so no history is
lost by deleting the working file).
**Benefits**: Keeps `docs/` matching its own stated definition; avoids a second, informal home for
architecture decisions alongside `docs/adr/`.
**Risk / migration**: None, it isn't tracked.
**Size**: S

## 4. Cross-area observations

- The BlockPin/StateReader seam plan in `docs/superpowers/plans/2026-07-09-blockpin-statereader-seam.md`
  targets `fluid_vault_indexer`'s live reorg bug and explicitly defers curve/psm3/vat/aavelike/oracle/morpho/
  allocation_tracker migrations — directly relevant to whichever Go agent is covering indexer reorg
  handling.
- `stl-verify/data_quality/schemamaster/` is Go code (schema-conformance checker against
  `information_schema`) living outside the normal `cmd/`/`internal/` tree — worth the Go-service
  agent confirming it's intentionally exempt from that layout.
- `.claude/commands/pr-respond.md` and `.claude/workflows/pr-review-response.js` are untracked and
  appear to be repo-local copies of the same-named skills already available globally
  (`pr-respond`, `pr-review-response`) — unclear if intentional or accidental duplication; flagged,
  not analyzed in depth (outside this agent's remit).

## 5. Open questions

- Whether `docs/linear_issues_proposal.md` and the untracked `.claude/commands`/`.claude/workflows`
  are intentional in-progress work or stray — not yet determined (checkpoint cut investigation short;
  see final version).
- Whether `k8s/overlays/dev/workers/` and `data-validator/` sub-overlays (mentioned in `k8s/AGENTS.md`)
  have the same per-chain duplication as `base/` — not yet checked.
- Whether splitting `AllocationRepository` (F13.3) is worth the churn given it's all read-only
  queries behind one port today — need to check `app/ports/allocation_repository.py`'s shape first.
