# ADR-0007: Image Layers as a Function of Source

- **Status**: Proposed
- **Proposed**: @r0hitsharma
- **Date**: 2026-09-09
- **Deciders**: @vector, @infrastructure
- **Relates to**: [ADR-0006](0006-data-reproducibility-and-append-only-guarantees.md) §2 (code identity, writer runs and image retention)

## Context

Every merge to `main` rewrites every `newTag` in staging's ArgoCD-synced overlay to the deploy SHA, so every service's pods roll immediately even when that service did not change. Prod's overlay is rewritten too, but `deploy-prod.yaml` gates the rollout behind an `environment: production` approval, so a merge alone does not roll prod pods unapproved. A documentation edit anywhere under `stl-verify/` sets the `go` group in `.github/changed-files.yml`, which deploys, which rolls all of staging's pods on every merge.

The reason a rebuild differs is not accidental. `Dockerfile.common` linked `GIT_COMMIT`, `GIT_BRANCH` and `BUILD_TIME` into every binary with `-ldflags -X`, and `BUILD_TIME` is `date -u` at build time. Two builds of byte-identical source, differing only in that value, produced different binaries — measured, not assumed:

| Build | `BUILD_TIME` | shipped binary sha256 |
|---|---|---|
| A | `2026-09-08T10:00:00Z` | `18e3290f264cc28d1ce90fcab79b0eec9717d2630268d33f7923daa5e65bf859` |
| B | `2026-09-08T11:30:00Z` | `86e475dbda179a5c9853273a9c458d7c8595a0fa6eb2c89ff307b75dc79fb4a1` |

Both binaries are 29,687,970 bytes and first diverge at offset 3977. Measured on
`linux/arm64` (native, no emulation) at commit `6328bfff`, the parent of this stack,
whose `Dockerfile.common` still carries the `-X` stamps. To re-derive, from `stl-verify/`:

```
make docker-build AWS_ACCOUNT_ID=000000000000 ENV=measure \
  GIT_COMMIT_FULL=6328bfff0fcf41bc689df9f3584e51d1168cc622 GIT_BRANCH=HEAD \
  BUILD_TIME=2026-09-08T10:00:00Z IMAGE_TAG=build-a
# repeat with BUILD_TIME=2026-09-08T11:30:00Z IMAGE_TAG=build-b, then for each:
CID=$(docker create <repo>:build-a); docker cp "$CID:/app/watcher" watcher-a; docker rm "$CID"
```

`GIT_COMMIT_FULL` and `GIT_BRANCH` are pinned so the two builds differ in `BUILD_TIME`
alone; `AWS_ACCOUNT_ID`/`ENV` only shape the local tag. The hashes are specific to this
platform and to the flag string below — a non-static or amd64 build yields different
values while showing the same inequality, which is the part that matters.

The Python image had the same property by deliberate design: `ARG`/`ENV GIT_COMMIT` sat *above* the `apt-get upgrade` and `dpkg --purge` hardening step precisely so its per-commit value would bust the cache and re-run OS updates on every release. That is also what made the layer, and everything below it, new on every commit.

So the pipeline had no way to tell "this service changed" from "this service was rebuilt", and OS patching depended on a side effect of that inability.

## Decision

**1. No per-build value may reach an image layer.** The versioning args reach the image only as `ARG`/`ENV` declared after the last `COPY` of the runtime stage. Image config is not a layer, so layers depend on the source alone.

**2. A released binary learns its identity at runtime, not at link time.** `internal/pkg/buildinfo` resolves commit, branch and build time from the first source that has one: Go's embedded VCS info, then `BUILD_GIT_HASH` / `BUILD_GIT_BRANCH` / `BUILD_TIME`; a value the caller has already set takes precedence over both. A released image reports from ENV; a local `go build` still reports from VCS.

**3. A pod reports the commit of the image it runs, not of the deploy that last ran.** These differ exactly when a deploy keeps an older tag because the layers matched, and the image's own commit is the honest answer — it is also the one `build_registry` is keyed on (ADR-0006 §2). Injecting the commit as a pod-level env var from the overlay was rejected for this reason: a kept tag would then report the deploy's commit while running an older binary, which is a provenance lie.

**4. OS patching is scheduled, not incidental.** A weekly workflow rebuilds every image with `--no-cache --pull` and re-exports the shared build cache, so the next deploy of any commit inherits the patched layers. It pushes no image, and the reason matters because the obvious one is not yet true. The SHA tag is what `build_registry`'s `(git_hash, service)` identity names, so re-pushing it would change the bytes an already-written governed row points at. ECR does not currently prevent that: every `stl-sentinel*` repository reports `MUTABLE` (checked 2026-09-09, `aws ecr describe-repositories --region eu-west-1 --query 'repositories[].imageTagMutability'` — 16 of 16). ARCT-420 changes this and is code-complete in the infrastructure repo, but **not yet applied**; its target state is `IMMUTABLE_WITH_EXCLUSION` with the alias tags `latest` / `*-latest` excluded, not blanket immutability. Until it lands, `DOCKER_PUSH=0` is the only thing preventing the re-push. `--pull` is not redundant with `--no-cache` — the latter discards the build cache while base images still resolve from the builder's local store.

**5. The property is asserted in CI, not trusted.** Four builds per image: a baseline, a rebuild of identical source with different build metadata, a docs-only edit, and a real source change — the first two and the docs-only build must produce identical layers, and the source change must not. The docs-only case is genuine only for the go image: `Dockerfile.common`'s `COPY . .` reaches `stl-verify/README.md`. `python/Dockerfile` copies only `pyproject.toml`, `uv.lock`, `.env.default`, `app/`, `suraf/` and (in the final stage) `cli/` — no `COPY` reaches `README.md` — so for the python image the docs-only build is a byte-identical rebuild of the same-source case, and that leg of the gate is vacuous there. The source-change case is not decoration regardless — without it, a comparison that always answers "identical" reads as a pass and would certify the exact failure the check exists to catch.

**6. `-trimpath` and `-buildvcs=false` are set.** Neither is *required* today: paths are already fixed at `/app`, and the build context carries no `.git`. They exist so a future context or WORKDIR change cannot silently put a per-commit value back into the binary. ADR-0006 §2 lists `-trimpath` as a step toward bit-for-bit rebuilds.

## Boundaries

What this ADR does **not** claim, stated because each has been mistaken for a claim it makes:

- **Layers, not manifests.** `BUILD_TIME` still varies per build; it moved into the image config rather than out of the image. A rebuild's manifest digest therefore still differs, so re-running a build for an already-deployed SHA remains unsupported — today by convention rather than by enforcement, and by enforcement once ARCT-420 is applied (see decision 4). The conclusion in `infrastructure`'s deployment guide is unchanged; only its stated reason ("the Go build is not bit-reproducible") is now the wrong one.
- **Registry-side identity rests on cache carry-forward, not on bit-for-bit reproducibility.** The CI gate compares uncompressed diff IDs of a locally loaded image; the deploy comparison reads compressed blob digests from the ECR manifest. Identical content guarantees the first; it guarantees the second only while the blob is carried forward by cache import. This is why the weekly refresh produces one deploy where essentially every image differs.
- **Base images are not pinned by digest.** `golang:${GO_VERSION}-alpine` and `python:${PYTHON_VERSION}-slim` resolve to `golang:1.26.6-alpine` and `python:3.12.14-slim` — patch-pinned via `.go-version`/`.python-version`, so a rebuild resolves the same base until one of those files bumps. `alpine:3.21` and `node:24-alpine` name only a minor version, so either can resolve a different patch on a rebuild months later with no file in this repo changing. None of the four is pinned by digest, so the tag is still mutable in the registry either way — the two pairs differ in how likely a rebuild is to actually land on a different base, not in whether one could. ADR-0006 §2 names pinned base-image digests as a later step and retains the original image as the guaranteed fallback.

## Consequences

- A deploy rolls the pods whose service changed, instead of all of them.
- One deliberate full roll per week, when patches land, replaces a full roll per commit for nothing.
- Patches reach the cluster on the first deploy *after* a refresh, not on the refresh itself. If `main` is quiet for a week, patches wait that long.
- The week of shadow verdicts used to justify any deploy-behaviour cutover — collected only against the staging overlay, since `compare-image-layers.sh` is wired into `deploy.yaml`'s staging job and never runs against prod — contains one day where nearly every image reports `CHANGED`. That is the refresh, not churn the comparison failed to suppress. A cutover decision for prod extrapolates from staging's verdicts; it has none of its own.
- Reproducibility currently rests on the rebuild path alone. ADR-0006 §2's retained-image fallback is not wired up: `build_registry.docker_sha` is `NULL` for every row, so the fallback cannot name the image that produced any governed row, and the conformance check that would verify those digests does not exist. This ADR improves the rebuild path; it does not substitute for that gap.
- **Cutover criteria (draft — needs sign-off, not yet approved).** Before the deploy trusts the comparator's verdict instead of rewriting every tag on every merge: two consecutive full weeks of staging shadow verdicts, with any week containing a security-refresh all-`CHANGED` day excluded from the count and re-run clean; zero *false* `CHANGED` verdicts in that window, where false `CHANGED` means the comparator reported `CHANGED` for an image whose source — restricted to the paths its Dockerfile actually `COPY`s — did not change between the pinned and candidate commits; and no `UNKNOWN`, `PINNED_GONE` or `NOT_BUILT` verdict left unexplained. Owner: unassigned — whoever signs off on this ADR needs to name one before the cutover, not after.

## Alternatives considered

**Keep stamping and accept the rolling.** The status quo. Rejected: it makes every deploy's blast radius the whole namespace, which removes the signal that a rollout problem belongs to the service that changed.

**Inject the commit as a pod env var from the overlay.** Would have avoided touching the Dockerfiles. Rejected under decision 3: it reports the deploy's commit rather than the running image's, so a kept tag misattributes provenance — and the pod spec would change on every deploy, rolling every pod and defeating the purpose.

**A targeted `--no-cache-filter` on the runtime stage** instead of `--no-cache`, to keep the weekly refresh cheap. Rejected: a filter that stops matching a renamed stage silently refreshes nothing, and a security job whose failure mode is silence is worse than a slow one.
