---
name: review-phase
description: Spawn the standard parallel reviewer subagents after a substantive code change (new feature, refactor, or multi-file bug fix) and before declaring work done. Use when you finish implementing a change in this repo and need the project's mandated review pass.
---

# Review phase

After completing a substantive code change (new feature, refactor, bug fix touching multiple files), and before declaring the work done, spawn the following reviewer subagents **in parallel** — a single message containing multiple `Agent` tool calls. Sequential review wastes wall-clock time and starves later reviewers of attention.

Always spawn:

1. **`pr-review-toolkit:code-reviewer`** — adherence to project guidelines, style, conventions.
2. **`pr-review-toolkit:silent-failure-hunter`** — error swallowing, ignored errors, inadequate fallback, NotFound-treated-as-success.
3. **General-purpose with architecture brief** — hexagonal layering, dependency direction, port/adapter boundary, single responsibility, separation of concerns, coupling.
4. **General-purpose with code-quality/patterns brief** — function size, naming, idiomatic Go, DRY, premature abstraction, test design, SOLID-ish concerns.

Spawn additionally when applicable:

5. **`pr-review-toolkit:pr-test-analyzer`** — when new tests are added or coverage is at risk.
6. **`pr-review-toolkit:type-design-analyzer`** — when new types or interfaces are introduced.
7. **`pr-review-toolkit:comment-analyzer`** — when substantive new docstrings or comments are added.

Each reviewer prompt must include:

- The plan file path (if a plan exists) and a precise list of files in scope.
- A specific audit checklist tailored to that reviewer's lens — don't ask reviewers to "review the diff"; tell them what to look for.
- The expected output format: **Blocking** / **Should-fix** / **Nice-to-have** / **Verified correct**, with file:line citations.

Apply blocking and should-fix items before declaring the work done. Nice-to-have items are surfaced to the user for an explicit decision.
