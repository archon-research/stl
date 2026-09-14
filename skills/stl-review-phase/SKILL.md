---
name: stl-review-phase
description: Repo-specific review lenses for stl. Use after a substantive change (new feature, refactor, or multi-file bug fix) and before declaring work done.
---

# Review phase

Use the host's dedicated review workflow before applying these lenses:

- **Claude Code:** run `/code-review` (add `--fix` to apply findings to the
  working tree). If it is unavailable, perform a separate manual pass over
  the four lenses below and state that no dedicated review workflow was
  available.
- **Codex:** run `codex review --uncommitted` for working-tree changes. For a
  branch or commit review, use `codex review --base <branch>` or
  `codex review --commit <sha>`. Then verify the four lenses below in a
  follow-up pass; do not treat the absence of `/code-review` as the absence of
  a dedicated review workflow.
- **Other harnesses:** perform one combined review pass covering all four
  lenses and state that no dedicated review workflow was available.

Ask for `xhigh` or `max` when the host supports reasoning-level selection and
you want the multi-agent fan-out and verify pass — on some models `high` and
below are a single inline pass.

Then verify these repo lenses were actually covered — add them to the review
target prompt if not:

1. **Hexagonal layering** — dependency direction (domain depends on nothing,
   ports depend on domain, adapters depend on ports), port/adapter boundary leaks.
2. **Silent failures** — error swallowing, ignored errors, inadequate fallbacks,
   partial success, and NotFound-treated-as-success.
3. **Append-only database** — no `UPDATE`/`DELETE`/`DO UPDATE` on a converted
   table (see `stl-verify/db/migrations/AGENTS.md`).
4. **Pipeline separation** — ingest writes "what happened" to Postgres; models
   read from Postgres and write "what it means" to their own tables.

Tag findings with short IDs (`B1`/`S1`/`N1`) grouped by severity so they can be
referenced. Apply blocking and should-fix items before declaring the work done;
surface nice-to-have items to the user for an explicit decision.
