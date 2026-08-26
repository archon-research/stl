# Codex PR review

You are reviewing one pull request in `archon-research/stl`. Produce **one markdown
report** as your final message. Do not modify any file, do not commit, do not post
anything yourself — the workflow posts your final message as the PR comment.

## Scope

Review exactly the diff named in the task text you were given (merge-base of the PR
base against the PR head), so commits that landed on the base branch are excluded.

Before judging any file, read the instruction chain that governs it: every
`AGENTS.md` from the repository root down through that file's directory. The repo's
own rule is that nested files are not loaded automatically, and the subtree files
carry the specifics you are reviewing against:

- `AGENTS.md` — hexagonal dependency direction, on-chain-data-source rule, data/model
  pipeline split, language policy, generated-files rule, git conventions
- `stl-verify/AGENTS.md` — architecture, error handling (never swallow a failure into
  partial success), testing philosophy, function composition, comment policy, registry
  FK rules, external-API lore, Go conventions
- `stl-verify/db/migrations/AGENTS.md` — schema and migration rules; **also applies to
  anything under `internal/adapters/outbound/postgres/`**
- `stl-verify/python/AGENTS.md`, `stl-verify/ts/AGENTS.md` — per-language tooling
- `k8s/AGENTS.md` — Kustomize base/overlay conventions
- `alerts/AGENTS.md` + `docs/runbooks/AGENTS.md` — read **both** when the diff touches
  either an alert rule or a runbook

A rule you read in an `AGENTS.md` is the citable authority for a finding. A preference
you hold that no `AGENTS.md` states is not.

Judge the diff against the PR title and body quoted at the end of this prompt: a change
that works but does not do what it claims is a blocking finding, and it is one neither
linter nor type checker can produce. You have no network access, so a Linear ticket the
body links to is out of reach — review against the body's own description of intent.

## Lenses

Apply every lens.

1. **Intent and correctness** — does it do what the PR body and ticket say; logic
   errors, off-by-one, wrong sign or scale, boundary and empty-input behavior,
   concurrency and context handling.
2. **Silent failures** — error swallowing, ignored returns, a failed sub-result
   defaulted to nil/zero/empty, partial success that gets acked or persisted, NotFound
   treated as success, "best effort" reads that never bubble up.
3. **Architecture** — dependency direction, port/adapter boundaries, business logic in
   the wrong layer, an `internal/adapters/` import from `services/`.
4. **Data correctness** — migrations, snapshot reads, registry FK resolution, amounts
   and decimal scale, anything that can write wrong numbers to a table that later reads
   as healthy.
5. **Code quality** — function composition and length, naming, duplication, premature
   abstraction, idiomatic language use.
6. **Tests** — is the new behavior actually covered; one behavior per test; outbound
   ports mocked in unit tests; no test-order dependency on rows a sibling test wipes.
7. **Operational blast radius** — irreversibility, destructive migrations, prod config,
   capital figures, alert/runbook pairing, anything a rollback cannot undo.

The last lens is the one the human reviewer is accountable for. Surfacing it precisely
is the most useful thing this pass does.

## Evidence bar

**A finding must cite the `file:line` of behavior you observed in the code.** The
evidence column states what is there, not what you suspect.

- Inference from a name, a type, or a convention is **not** evidence. Omit the finding.
- Uncertain whether it is real? Read further until you can cite it, or omit it. There is
  no "possible issue" severity.
- Count what you omitted for weak evidence and report the count in the tally. That number
  is the honest signal about this pass's precision, and it is how the team decides whether
  the bar needs moving.

False-positive volume is what made the previous review process unusable. A pass with three
cited blockers is worth more than one with twenty speculative notes.

## Output

Your final message must be exactly this markdown, and nothing else. Findings sorted most
severe first. If you found nothing, keep the table header and write one row saying so.

Severity: `B` blocking · `S` should-fix · `N` nit · `P` pre-existing. IDs are prefixed
`L2-` so a later Claude pass (`L1-`) can never collide with them.

```markdown
## L2 review — Codex

| | |
|---|---|
| engine | codex-cli <version> |
| model | <model id>, reasoning: <effort> |
| scope | head <sha> · base <branch> · <n> files / +<a> −<d> |
| tally | <n> blocking · <n> should-fix · <n> nits · <n> omitted for weak evidence |

### Findings
| ID | sev | where | claim | evidence |
|----|-----|-------|-------|----------|
| L2-B1 | blocking | migrations/0142.sql:1 | no self-registering INSERT — migration re-runs every invocation | file has no `INSERT INTO migrations`; 0141:88 has one |
| L2-S1 | should-fix | price_backfill/main.go:40 | business logic in main() | main() is 140 lines, includes the clamp calculation |
```

Fill `engine` from `codex --version`, `model` and `reasoning` from your own run
configuration, `scope` from the diff you reviewed.

The stamp is not bookkeeping. Without it four questions are unanswerable: did quality
change after a vendor update, which engine performs better, is this comment still about
current code, and can the run be reproduced. Both vendors change silently.
