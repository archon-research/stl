# CLAUDE.md

Guidance for Claude Code (claude.ai/code) in this repo. This root stays small and loads on
every session; per-directory files and `.claude/rules/*` are designed to load on demand when
you read files in that subtree, so most guidance is paid for only when it is relevant.
(Lazy-load fires in interactive sessions; see the maintainer note below for the caveat.)

## Repository map

- **stl-verify/** — main Go service (block watcher, backfill, backup worker). Ports and Adapters (Hexagonal).
- **k8s/** — Kubernetes manifests (Kustomize) for all environments.
- **alerts/**, **docs/runbooks/** — Prometheus alert rules and their matching runbooks.
- **docs/** — architecture diagrams and entity relations.

Infrastructure code (Terraform/OpenTofu) lives in a separate repository for security reasons.

`CONTRIBUTING.md` is the canonical onboarding doc (repo layout; how to add a worker / cronjob / backfiller / risk model; PR workflow). Read it before adding a new pipeline or model. Protocol specs and ADRs are in `docs/`.

## Cross-cutting rules

- **Dependencies flow inward** (hexagonal): domain has no dependencies; adapters depend on ports; ports depend on domain. Detailed port/adapter conventions live in `stl-verify/CLAUDE.md`.
- **On-chain data comes from chain RPC or the cached block payload, never third-party indexers.** Off-chain feeds need maintainer approval, justified in the PR description.
- **Data pipelines and model pipelines stay separate**: ingest writes "what happened" to Postgres; models read from Postgres and write "what it means" to their own tables. Separate entry points, usually separate PRs.
- **Language policy**: APIs and risk models are Python; workers/cronjobs/backfillers are Go (preferred) or Python; `stl-verify/ts/` is frontend only.
- **Never commit generated files or binaries.**
- **Don't bypass git hooks** (lefthook). The CI workflows in `.github/workflows/` are the source of truth for linting and tests. The `stl-verify/Makefile` is the source of truth for workflows — grep it before inventing a command.
- **Git**: branch `VEC-123-short-slug`; PR title `VEC-123: <what it does>`; GitHub squash-merges, don't squash locally. Run `make ci` (and `make test-integration` if data-adjacent) before pushing.

## Where the rest lives (loads on demand)

- **[stl-verify/CLAUDE.md](stl-verify/CLAUDE.md)** — Go service: architecture, errors, testing, function composition, comments, libraries, registries, external-API lore, build/run, Go linting.
- **[stl-verify/python/CLAUDE.md](stl-verify/python/CLAUDE.md)** and **[stl-verify/ts/CLAUDE.md](stl-verify/ts/CLAUDE.md)** — per-language tooling/CI.
- **[k8s/CLAUDE.md](k8s/CLAUDE.md)** — Kustomize base/overlays/dev-infra conventions.
- **[.claude/rules/go-database.md](.claude/rules/go-database.md)** — DB schema, migrations, snapshot reads, advisory locks. Auto-loads on `stl-verify/db/migrations/**` and repository adapters.
- **[.claude/rules/observability.md](.claude/rules/observability.md)** — alerts + runbooks definition-of-done for new indexers. Auto-loads on `alerts/**`, `docs/runbooks/**`.
- **`review-phase` skill** — spawn the standard parallel reviewer subagents after a substantive change, before declaring work done.

<!--
Maintainer notes (stripped from context — free):
- Keep this root ~60 lines: repo map, cross-cutting rules, and pointers ONLY. Everything
  else belongs in a per-directory CLAUDE.md, a .claude/rules/ file (paths: glob), or a skill.
- No CLAUDE.md should exceed ~200 lines; longer files reduce adherence.
- Path-scoped rules use the `paths:` frontmatter field (NOT the stale `globs:`).
  Verified firing in Claude Code v2.1.202 via the InstructionsLoaded hook (nested_traversal
  + path_glob_match). Interactive REPL triggers lazy-load; a driven/agent session may not.
- Review CLAUDE.md / rule edits in PRs like any other docs so conventions track the code.
- Prune ruthlessly: if Claude already does the right thing without a rule, delete the rule.
  Re-check after major model releases — a workaround for an old model becomes pure overhead.
-->
