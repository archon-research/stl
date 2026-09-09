---
name: stl-review-phase
description: Repo-specific review lenses for stl. Use with /code-review after a substantive change (new feature, refactor, or multi-file bug fix) and before declaring work done.
---

# Review phase

Run `/code-review` (add `--fix` to apply findings to the working tree). Ask for
`xhigh` or `max` when you want the multi-agent fan-out and the verify pass —
on some models `high` and below are a single inline pass.

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

If `/code-review` is unavailable (a harness without it), apply all four lenses
yourself in one combined pass and say plainly that this was a single-pass review.
