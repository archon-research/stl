# k8s — Kubernetes manifests (Kustomize)

Root repo map and cross-cutting rules: [../AGENTS.md](../AGENTS.md).

- `k8s/base/` — one subdirectory per service: `Deployment`, `ServiceAccount` (reused by every overlay)
- `k8s/overlays/prod/` — prod-specific patches (namespace, images/image tags)
- `k8s/overlays/staging/` — staging-specific patches (namespace, images/image tags)
- `k8s/overlays/dev/` — local kind overlay (localhost/*:local images, shared stl-config/stl-secrets via a runtime Component); `workers/` sub-overlay for Alchemy-key workers
- `k8s/dev-infra/` — local-only artifacts with no EKS equivalent: infra (timescaledb, redis, localstack, temporal, jaeger, mock-blockchain-server), `jobs/`, and `kind.yaml` (the kind cluster definition); applied imperatively by `stl-verify/Makefile`

## Config / secret rollouts (EKS only)

- Every Deployment in the **staging + prod** overlays is opted into Stakater Reloader via one `kind: Deployment` patch per overlay (`reloader.stakater.com/auto: "true"`, ADR-013 / ORB-188). New services inherit it automatically — do not add per-service annotations.
- A merged ConfigMap change (or an ESO Secret refresh after an AWS rotation) rolls the referencing workloads on its own. No image bump, no manual restart.
- The **dev** overlay is intentionally not annotated — no controller runs locally, so local config changes still need `kubectl rollout restart`.
- Guard: `make check-reloader-opt-in` (in `stl-verify/`) asserts prod/staging are fully opted in and dev is not.

## Rollout strategy

- **Every Deployment under `k8s/base/` declares `spec.strategy` explicitly**, including
  values that merely restate the API-server default. That is not redundancy — do not delete
  it as cleanup. `k8s/dev-infra/` is exempt: it is applied client-side by the Makefile and
  is never in ArgoCD's apply set, so the trap below cannot reach it.
- Both stl Applications sync with `ServerSideApply=true`, and server-side apply only
  prunes fields the applier's own field manager already owns. A Deployment that goes live
  without declaring a strategy has `spec.strategy.rollingUpdate` defaulted onto it by the
  API server, owned by nobody in ArgoCD's apply set. A later change to `type: Recreate`
  then yields `Recreate` *plus* that stale block, which the API server rejects with
  `spec.strategy.rollingUpdate: Forbidden: may not be specified when strategy type is
  'Recreate'`. The sync fails, retries, and fails identically forever — and because it is
  one Application per environment, it blocks every other resource in the sync too, while
  app health stays green.
- Recovery is a repo change, but a two-step one, and knowing that is the difference between
  a five-minute fix and an afternoon: restore `type: RollingUpdate` with `maxSurge` and
  `maxUnavailable` spelled out — that applies cleanly, unwedges the sync, and hands ArgoCD
  ownership of the block — then flip to `Recreate` in a *second* commit, after the first is
  Synced. Reaching for `kubectl` is not required and not the fix.
- `rollingUpdate: null` does **not** fix that state: it survives kustomize into the
  rendered manifest, but SSA will not remove a field the applier does not own.
- Declaring `type:` on its own is **not** enough either — ArgoCD then owns `f:type` but
  not `f:rollingUpdate`, and the retrofit still fails. A `RollingUpdate` strategy must
  spell out `maxSurge` and `maxUnavailable` as well.
- Pick the value deliberately. The default `25%` rounds up to 1 at `replicas: 1`, so a
  single-consumer workload — one pod on an SQS queue or a Temporal task queue — runs a
  second *Ready* pod on the queue for the whole startup of the new one, on every rollout
  including a Reloader-triggered one.
- `Recreate` and `maxSurge: 0` are **not** interchangeable. `maxSurge` is evaluated against
  ReplicaSet `.spec.replicas`, not live pods, so at `maxSurge: 0` the new pod still starts
  while the old one is `Terminating` through its grace period — measured at ~2s of overlap.
  Only `Recreate` blocks on the old pods actually being gone. Use `maxSurge: 0` when you
  need ownership of the block first (see the recovery note above); use `Recreate` when two
  pods on the queue must never overlap at all.

## Deploy

- **Never hand-edit image tags** in `k8s/overlays/{staging,prod}/kustomization.yaml` — CI owns them (staging bumps on merge; prod via the gated `production` GitHub Environment approval).
  Guard: the `Manifests` CI job runs `scripts/deploy/check-overlay-tag-consistency.sh`,
  which fails a PR whose tags do not all name one commit — the bot stamps a whole file to a
  single deploy SHA, so a hand-written tag is the odd one out. This has to block the merge:
  ArgoCD syncs the merge commit before the bot stamps it, so a tag naming an image that does
  not exist yet reaches the cluster as ImagePullBackOff, fails the staging health gate, and
  skips the prod promotion (ORB-313). A brand-new service lands its image in a separate PR
  first (CONTRIBUTING.md section 14).
- Merging to `main` deploys to staging via ArgoCD, then prod after manual approval.
- AWS resources (SQS queues, SNS subscriptions, IAM, secrets) live in a separate private infrastructure repo and must land **before** the code that needs them.
