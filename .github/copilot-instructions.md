---
description: 'Code review instructions for STL Verify'
applyTo: '**'
---

# STL Verify: Copilot code review

Project architecture, conventions, and tech stack are defined canonically in the repo's
`AGENTS.md` files (root and per-directory), which Copilot reads automatically. This file
adds only the review-specific framing: how to prioritise and format review comments. Do
not restate architecture or style rules here; cite `AGENTS.md` so the two never drift.

## Review priorities

### 🔴 CRITICAL: block merge
- Architecture / dependency-rule violations (the hexagonal rules in `AGENTS.md`: domain has no deps; adapters depend on ports; no business logic in adapters).
- Security: exposed secrets, missing error handling on external calls.
- Data integrity: chain-reorg handling errors, race conditions, and swallowed failures (a partial failure must stop the whole event/block, never persist a hole).
- Resource leaks: unclosed DB connections, HTTP bodies, file handles.

### 🟡 IMPORTANT: discuss before merge
- Missing tests on critical paths, or non-table-driven tests where the convention applies.
- Error handling: unwrapped/contextless errors, "best effort" reads that silently drop failures.
- Business logic leaking into adapters; a missing port.
- Performance: N+1 queries, missing pagination on large datasets.

### 🟢 SUGGESTION: non-blocking
- Unclear names, missing godoc on exported symbols, inconsistent formatting.

## Review comment format

Use this structure for each review comment:

**[PRIORITY] Category: Brief title**

Description of the issue.

**Why:** Impact and reasoning.

**Fix:**
```go
// Corrected code here
```
