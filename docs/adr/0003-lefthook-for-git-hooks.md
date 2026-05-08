# ADR-0003: Lefthook for better dev experience via git-hooks

**Status**: Proposed  
**Proposed**: @r0hitsharma  
**Date**: 2026-08-08  
**Deciders**: @vector

## Context

This repository spans multiple languages (Go, Python, and TypeScript), each with its own linting and formatting toolchain. Without a shared pre-commit mechanism, contributors can easily miss language-specific checks locally, leading to avoidable CI failures.

In addition to language code, we also maintain configuration-heavy files like GitHub Actions workflows and other YAML/JSON metadata. These files benefit from lightweight syntax and hygiene checks before push.

Running hooks locally helps catch and auto-fix issues early, so we avoid separate "lint-only" follow-up commits. That reduces noise in PR history and prevents unnecessary extra CI runs triggered only by formatting/lint fixes.

## Decision

We will standardize on Lefthook for git hooks in this repository.

Why Lefthook:

- It is a widely used, Go-native, zero-dependency binary maintained by Evil Martians.
- It fits our mixed-language stack while keeping local setup lightweight.
- It supports fast staged-file checks and parallel execution, which aligns with our developer workflow goals.

Alternatives considered:

| Tool | Language | Backed By / Creator | Stars | Popularity | Stability / Reliability | Zero-dependency binary | Notable trade-off |
|---|---|---|---:|---|---|---|---|
| Lefthook | Go | Evil Martians, a well-known consultancy with a strong OSS track record across frontend and developer tooling | ~8k+ | High | High: mature project, many releases, broad adoption, large contributor base, and active maintenance | Yes | Smaller hook-sharing ecosystem than pre-commit |
| pre-commit | Python | Maintained by Anthony Sottile and the broader pre-commit community; heavily used across major OSS projects | ~15k+ | Very high | Very high: long-lived, broadly adopted, low issue count relative to usage, and highly battle-tested | No | Requires Python runtime and managed hook environments |
| Hk | Rust | Klaus Post, a highly respected performance-focused systems engineer in the Go ecosystem | ~500+ | Medium | Medium: strong author reputation, but smaller ecosystem and lower adoption than Lefthook/pre-commit | Yes | Lower adoption and smaller integration footprint |
| GetHooky | Go | Community-maintained lightweight Go alternative | Small | Low | Lower: simple and promising, but with much less evidence of long-term adoption and maintenance at scale | Yes | Less battle-tested at large OSS scale |
| Husky | JavaScript | typicode / JavaScript ecosystem; widely used in frontend projects | ~35k+ | Very high | High: very mature and broadly adopted, but centered on Node/npm workflows rather than language-agnostic binaries | No | Pulls npm workflow assumptions and package-manager surface into hook setup |

These comparisons are intended as a practical engineering snapshot rather than a formal benchmark. Stars are only one signal; creator reputation, contributor breadth, release cadence, issue volume, and time in production use all inform the stability assessment.

Why this is preferable here:

- We need a single orchestrator over Go, Python, and TypeScript checks plus YAML/JSON workflow hygiene checks.
- We want to reduce "lint-fix-only" commits that add noise and retrigger CI.
- A standalone binary model keeps operational complexity lower than toolchains that bootstrap larger runtimes.

Security and supply-chain posture:

- The binary-install change introduced in commit 0c454c3 reduces dependency expansion versus package-manager-based hook tooling.
- We use a pinned Lefthook release version in local setup.
- We verify the downloaded binary against the release SHA-256 checksums before execution in commit ff31041.
- A standalone binary avoids postinstall script exposure common in npm-style installs.

Why pinned binary download over go install in this setup:

- It avoids build-time variability and transitive source fetch during local install.
- Checksum verification gives artifact-integrity validation at install time.
- It provides a deterministic, frozen artifact that is easier to reason about in constrained or security-sensitive environments.
