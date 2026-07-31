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

## Deploy

- **Never hand-edit image tags** in `k8s/overlays/{staging,prod}/kustomization.yaml` — CI owns them (staging bumps on merge; prod via the gated `production` GitHub Environment approval).
- Merging to `main` deploys to staging via ArgoCD, then prod after manual approval.
- AWS resources (SQS queues, SNS subscriptions, IAM, secrets) live in a separate private infrastructure repo and must land **before** the code that needs them.
