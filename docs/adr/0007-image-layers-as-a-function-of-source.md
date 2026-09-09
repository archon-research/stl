# ADR-0007: Image Layers as a Function of Source

- **Status**: Proposed
- **Proposed**: @r0hitsharma
- **Date**: 2026-09-09
- **Deciders**: @vector, @infrastructure
- **Relates to**: [ADR-0006](0006-data-reproducibility-and-append-only-guarantees.md) §2 (code identity, writer runs and image retention)

## Context

Every merge to `main` rewrites every `newTag` in the ArgoCD-synced overlays to the deploy SHA, so every service's pods roll even when that service did not change. A documentation edit anywhere under `stl-verify/` sets the `go` group in `.github/changed-files.yml`, which deploys, which rolls all of them.

The reason a rebuild differs is not accidental. `Dockerfile.common` linked `GIT_COMMIT`, `GIT_BRANCH` and `BUILD_TIME` into every binary with `-ldflags -X`, and `BUILD_TIME` is `date -u` at build time. Two builds of byte-identical source, differing only in that value, produced different binaries — measured, not assumed:

| Build | `BUILD_TIME` | shipped binary sha256 |
|---|---|---|
| A | `2026-09-08T10:00:00Z` | `0697470f97d21fe3…` |
| B | `2026-09-08T11:30:00Z` | `e378f021484f2cae…` |

The Python image had the same property by deliberate design: `ARG`/`ENV GIT_COMMIT` sat *above* the `apt-get upgrade` and `dpkg --purge` hardening step precisely so its per-commit value would bust the cache and re-run OS updates on every release. That is also what made the layer, and everything below it, new on every commit.

So the pipeline had no way to tell "this service changed" from "this service was rebuilt", and OS patching depended on a side effect of that inability.

## Decision

**1. No per-build value may reach an image layer.** The versioning args reach the image only as `ARG`/`ENV` declared after the last `COPY` of the runtime stage. Image config is not a layer, so layers depend on the source alone.

**2. A released binary learns its identity at runtime, not at link time.** `internal/pkg/buildinfo` resolves commit, branch and build time from the first source that has one: an ldflags stamp, then Go's embedded VCS info, then `BUILD_GIT_HASH` / `BUILD_GIT_BRANCH` / `BUILD_TIME`. A released image reports from ENV; a local `go build` still reports from VCS.

**3. A pod reports the commit of the image it runs, not of the deploy that last ran.** These differ exactly when a deploy keeps an older tag because the layers matched, and the image's own commit is the honest answer — it is also the one `build_registry` is keyed on (ADR-0006 §2). Injecting the commit as a pod-level env var from the overlay was rejected for this reason: a kept tag would then report the deploy's commit while running an older binary, which is a provenance lie.

**4. OS patching is scheduled, not incidental.** A weekly workflow rebuilds every image with `--no-cache --pull` and re-exports the shared build cache, so the next deploy of any commit inherits the patched layers. It pushes no image: the SHA tag for an already-built commit exists, and re-pushing it fails under ECR tag immutability. `--pull` is not redundant with `--no-cache` — the latter discards the build cache while base images still resolve from the builder's local store.

**5. The property is asserted in CI, not trusted.** Three builds per image: identical source and a docs-only edit must produce identical layers, and a real source change must not. The third case is not decoration — without it, a comparison that always answers "identical" reads as a pass and would certify the exact failure the check exists to catch.

**6. `-trimpath` and `-buildvcs=false` are set.** Neither changes anything today: paths are already fixed at `/app`, and the build context carries no `.git`. They exist so a future context or WORKDIR change cannot silently put a per-commit value back into the binary. ADR-0006 §2 lists `-trimpath` as a step toward bit-for-bit rebuilds.

## Boundaries

What this ADR does **not** claim, stated because each has been mistaken for a claim it makes:

- **Layers, not manifests.** `BUILD_TIME` still varies per build; it moved into the image config rather than out of the image. A rebuild's manifest digest therefore still differs, so re-running a build for an already-deployed SHA remains unsupported under tag immutability. The conclusion in `infrastructure`'s deployment guide is unchanged; only its stated reason ("the Go build is not bit-reproducible") is now the wrong one.
- **Registry-side identity rests on cache carry-forward, not on bit-for-bit reproducibility.** The CI gate compares uncompressed diff IDs of a locally loaded image; the deploy comparison reads compressed blob digests from the ECR manifest. Identical content guarantees the first; it guarantees the second only while the blob is carried forward by cache import. This is why the weekly refresh produces one deploy where essentially every image differs.
- **Base images are not pinned by digest.** `alpine:3.21`, `golang:${GO_VERSION}-alpine`, `python:${PYTHON_VERSION}-slim` and `node:24-alpine` are floating tags, so a rebuild months later resolves a different base. ADR-0006 §2 names pinned base-image digests as a later step and retains the original image as the guaranteed fallback.

## Consequences

- A deploy rolls the pods whose service changed, instead of all of them.
- One deliberate full roll per week, when patches land, replaces a full roll per commit for nothing.
- Patches reach the cluster on the first deploy *after* a refresh, not on the refresh itself. If `main` is quiet for a week, patches wait that long.
- The week of shadow verdicts used to justify any deploy-behaviour cutover contains one day where nearly every image reports `CHANGED`. That is the refresh, not churn the comparison failed to suppress.
- Reproducibility currently rests on the rebuild path alone. ADR-0006 §2's retained-image fallback is not wired up: `build_registry.docker_sha` is `NULL` for every row, so the fallback cannot name the image that produced any governed row, and the conformance check that would verify those digests does not exist. This ADR improves the rebuild path; it does not substitute for that gap.

## Alternatives considered

**Keep stamping and accept the rolling.** The status quo. Rejected: it makes every deploy's blast radius the whole namespace, which removes the signal that a rollout problem belongs to the service that changed.

**Inject the commit as a pod env var from the overlay.** Would have avoided touching the Dockerfiles. Rejected under decision 3: it reports the deploy's commit rather than the running image's, so a kept tag misattributes provenance — and the pod spec would change on every deploy, rolling every pod and defeating the purpose.

**A targeted `--no-cache-filter` on the runtime stage** instead of `--no-cache`, to keep the weekly refresh cheap. Rejected: a filter that stops matching a renamed stage silently refreshes nothing, and a security job whose failure mode is silence is worse than a slow one.
