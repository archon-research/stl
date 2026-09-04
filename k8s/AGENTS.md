# k8s — Kubernetes manifests (Kustomize)

Root repo map and cross-cutting rules: [../AGENTS.md](../AGENTS.md).

- `k8s/base/` — one subdirectory per service: `Deployment`, `ServiceAccount` (reused by every overlay)
- `k8s/overlays/prod/` — prod-specific patches (namespace, images/image tags)
- `k8s/overlays/staging/` — staging-specific patches (namespace, images/image tags)
- `k8s/overlays/dev/` — local kind overlay (localhost/*:local images, shared stl-config/stl-secrets via a runtime Component); `workers/` sub-overlay for Alchemy-key workers
- `k8s/dev-infra/` — local-only artifacts with no EKS equivalent: infra (timescaledb, redis, localstack, temporal, jaeger, mock-blockchain-server), `jobs/`, `sql/`, and `kind.yaml` (the kind cluster definition); applied imperatively by `stl-verify/Makefile`
- `k8s/dev-infra/sql/` holds SQL that repairs the **local kind database only**, run by a Job in `dev-infra/jobs/` and never by the migrator. Anything added here must state in its header why it is dev-only, and must be idempotent and safe to re-run. Today: `resync-sequences.sql` (`make kind-resync-sequences`, ARCT-399) — see [README.md](README.md#bulk-importing-rows-into-the-dev-database-staging-clone).

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

- **Never hand-edit the `images:` block** in `k8s/overlays/{staging,prod}/kustomization.yaml` — it is
  generated from `k8s/image-roster.txt` by `scripts/deploy/render-overlay-images.sh`, and the deploy bot
  rewrites it wholesale on every deploy (staging on merge; prod stamped by the same run, rolled
  out only after the `production` GitHub Environment approval). Adding an image = one roster line (kind, name, the `image:` aliases the bases
  use) plus the base dir under `resources:`; the next deploy pins it (ORB-362). Removing or
  re-homing an image: change the roster and delete the stale entry in the same PR — the one
  sanctioned hand edit, since the bot only rewrites the block after merge. The prod block's tag is
  also CI's record of the last promoted SHA — main change detection diffs each push against it (ORB-361).
  Guards: the `Manifests` CI job runs `scripts/deploy/check-overlay-tag-consistency.sh` (all tags name
  one commit) and `render-overlay-images.sh --check --allow-missing` (every entry is one the roster
  renders; roster lines the bot has not written yet are pending, not errors); locally: `make
  check-overlay-images` in `stl-verify/`. Both must block the merge:
  ArgoCD syncs the merge commit before the bot writes the block, so a hand-written tag naming an image
  that does not exist reaches the cluster as ImagePullBackOff, fails the staging health gate, and skips
  the prod promotion (ORB-313). A brand-new service lands its build + roster line in a separate PR first
  (CONTRIBUTING.md section 14).
- Merging to `main` deploys to staging via ArgoCD, then prod after manual approval.
- AWS resources (SQS queues, SNS subscriptions, IAM, secrets) live in a separate private infrastructure repo and must land **before** the code that needs them.
