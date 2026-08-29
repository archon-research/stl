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
  values that only restate the API-server default — not redundancy, do not delete it.
  `RollingUpdate` must spell out `maxSurge` *and* `maxUnavailable`; `Recreate` must carry
  no `rollingUpdate` key. `k8s/dev-infra/` is exempt (client-side applied, never synced).
- Why: SSA prunes only fields ArgoCD already owns, so a strategy left to the API server's
  defaults can never be changed later — the retrofit is rejected on every sync, blocking
  every other resource in that Application while app health stays green (#640). Declaring
  `type:` alone is not enough, and `rollingUpdate: null` is not a fix.
- Recovery is a repo change, not a `kubectl` one, but takes two merges: restore an explicit
  `RollingUpdate`, then change it once that is Synced.
- Choose the value deliberately. `25%` rounds up to one extra pod at `replicas: 1`, so a
  single-consumer workload (one pod on an SQS or Temporal task queue) wants `Recreate` —
  `maxSurge: 0` is not equivalent, it only removes the *Ready*-pod overlap.

## Deploy

- **Never hand-edit image tags** in `k8s/overlays/{staging,prod}/kustomization.yaml` — CI owns them (staging bumps on merge; prod via the gated `production` GitHub Environment approval).
  The prod overlay's tag is also CI's record of the last promoted SHA: main change detection
  diffs each push against it (ORB-361), so even a uniform hand edit skews what the next merge deploys.
  Guard: the `Manifests` CI job runs `scripts/deploy/check-overlay-tag-consistency.sh`,
  which fails a PR whose tags do not all name one commit — the bot stamps a whole file to a
  single deploy SHA, so a hand-written tag is the odd one out. This has to block the merge:
  ArgoCD syncs the merge commit before the bot stamps it, so a tag naming an image that does
  not exist yet reaches the cluster as ImagePullBackOff, fails the staging health gate, and
  skips the prod promotion (ORB-313). A brand-new service lands its image in a separate PR
  first (CONTRIBUTING.md section 14) — also when it reuses an *existing* image whose content
  the same PR changes: the new Deployment starts on the previously-built image and
  crash-loops until the merge commit's own build is stamped.
- Merging to `main` deploys to staging via ArgoCD, then prod after manual approval.
- AWS resources (SQS queues, SNS subscriptions, IAM, secrets) live in a separate private infrastructure repo and must land **before** the code that needs them.
