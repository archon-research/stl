<!--
PR template for archon-research/stl.

Two halves. The first is written by a human, before or right after the PR
opens. The second may be filled in by an agent from the diff. Delete any
section that does not apply.
-->

## Human Intent

<!-- 2-4 lines in your own words: the problem, the trigger (ticket, alert that
fired, prior PR, staging observation), the outcome. What a reviewer needs to
know before reading the diff. -->

---

<!-- Agent-filled from here down. -->

## Why

<!-- The trigger, rationale and the reasoning: ticket, prior PR, alert that fired, staging
observation with timestamp. Evidence over assertion. -->

## What changed

### TLDR

<!-- in simple plain words -->

### Summary

<!-- Bullet list of the meaningful changes, bolded lead phrase per bullet. Skip trivial refactors. -->

## Testing

<!-- Manual checks CI does not run: local run against staging, numbers eyeballed,
before/after query timing. Bug fixes: state the test was observed red first.
No code change: say so. Describe and tick off the bullets below; add more as needed. -->

- [ ] `End-to-end`
- [ ] `Performance testing on new queries or endpoint`
  - [ ] `Local backend`
  - [ ] `Staging backend`

## Rollout

<!-- What to watch after merge: expected log line, metric that should appear or
stay flat, deploy stamp. Any post-merge step goes here as an unchecked box. -->

## Not in this PR

<!-- What is deliberately left out and where it is tracked (ticket / PR). -->

## Worth knowing

<!-- Reviewer-facing gotchas not visible from the diff: a behaviour change, a
design choice and the alternative rejected, anything important for future readers. -->
