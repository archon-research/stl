---
name: stl-review-phase
description: Run the standard parallel review after a substantive code change (new feature, refactor, or multi-file bug fix) and before declaring work done. Uses delegation when available and an in-agent fallback otherwise.
---

# Review phase

After completing a substantive code change (new feature, refactor, or bug fix
touching multiple files), and before declaring the work done, launch the review
lenses below concurrently using the platform's subagent or delegation tools.
Use a specialized reviewer when the platform provides one; otherwise give the
same brief to a general-purpose reviewer. Never skip a lens merely because a
named reviewer is unavailable. If the platform's concurrency limit is lower
than the number of lenses, combine adjacent lenses in one reviewer prompt.
If the platform has no delegation tools, the current agent must apply every
lens itself in one combined review before declaring the work done.

Always run:

1. **Guidelines and correctness** — adherence to project instructions, behavior,
   style, and conventions. Prefer `pr-review-toolkit:code-reviewer` when available.
2. **Silent failures** — error swallowing, ignored errors, inadequate fallbacks,
   partial success, and NotFound-treated-as-success. Prefer
   `pr-review-toolkit:silent-failure-hunter` when available.
3. **Architecture** — hexagonal layering, dependency direction, port/adapter
   boundaries, single responsibility, separation of concerns, and coupling.
4. **Code quality and patterns** — function size, naming, idiomatic language use,
   DRY, premature abstraction, test design, and SOLID concerns.

Run additionally when applicable:

5. **Tests and coverage** — when new tests are added or coverage is at risk.
   Prefer `pr-review-toolkit:pr-test-analyzer` when available.
6. **Type design** — when new types or interfaces are introduced. Prefer
   `pr-review-toolkit:type-design-analyzer` when available.
7. **Comments and documentation** — when substantive comments or docstrings are
   added. Prefer `pr-review-toolkit:comment-analyzer` when available.

Each reviewer prompt must include:

- `Review only; do not modify files.` Reviewers share a worktree and concurrent
  edits can race.
- The plan file path (if a plan exists) and a precise list of files in scope.
- A specific audit checklist tailored to that reviewer's lens — don't ask reviewers to "review the diff"; tell them what to look for.
- The expected output format: **Blocking** / **Should-fix** / **Nice-to-have** / **Verified correct**, with file:line citations.

Apply blocking and should-fix items before declaring the work done. Nice-to-have items are surfaced to the user for an explicit decision.
