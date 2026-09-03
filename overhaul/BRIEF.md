# Overhaul investigation brief

Shared brief for every investigation agent working on the stl repo overhaul. Read fully before starting.

## Purpose

The repo has grown to ~260k lines of Go across 34 binaries, and PRs have grown with it (median 7
files per commit, p90 31, max 96 since March 2026). We are looking for refactorings that make the
system cleaner and easier to maintain. **Big changes are on the table.** Comments, READMEs and
AGENTS.md files describe *intent* and history; treat them as hypotheses about the code, not as
truth, and never as a reason to leave something alone.

## Ground rules

- **Read-only.** Do not edit code, do not run `git` commands that change state, do not commit.
  The only files you write are your own report under `overhaul/findings/`.
- Do not start Docker or run `make` targets. `go build ./...`, `go vet`, `go list`, and unit
  tests (`go test ./path/ -run X`) are fine; integration tests are not.
- Verify with evidence: line numbers, counts, quoted signatures. "Feels big" is not a finding;
  "12 handlers repeat the same 40-line decode/count-check/unpack skeleton, e.g. a.go:120-160,
  b.go:88-130" is.
- **Compare siblings.** The most valuable findings come from putting two packages that do the
  same job side by side (two indexers, two repositories, two main.go files) and listing where
  they diverge for no reason.
- Report only your assigned area in depth, but record anything you notice that belongs to another
  area under "Cross-area observations". The orchestrator stitches those together.

## Vocabulary (use these words exactly)

- **Module** — anything with an interface and an implementation (function, type, package).
- **Interface** — everything a caller must know to use the module: types, invariants, error
  modes, ordering, config. Not just the Go signature.
- **Depth** — a lot of behaviour behind a small interface is *deep*; an interface nearly as
  complex as the implementation is *shallow*.
- **Seam** — where an interface lives; a place behaviour can be swapped without editing in place.
- **Adapter** — a concrete thing satisfying an interface at a seam.
  One adapter = hypothetical seam. Two adapters = real seam.
- **Leverage** — what callers gain from depth. **Locality** — what maintainers gain: change,
  bugs and knowledge concentrated in one place.
- **Deletion test** — imagine deleting the module. If complexity vanishes, it was a
  pass-through. If complexity reappears across N callers, it was earning its keep.

Use the repo's own domain names (block event, reorg, backfill, indexer, position, allocation,
oracle price, vault, market, snapshot, registry tables, etc.). Domain vocabulary sources:
`docs/data_entities.md`, `docs/entity_relation.md`, `docs/*_spec.md`, `CONTRIBUTING.md`,
`stl-verify/AGENTS.md`, `stl-verify/db/migrations/AGENTS.md`, `docs/adr/`.

## What to look for

- Shallow modules and pass-throughs (apply the deletion test).
- Duplication across siblings: repeated skeletons, hand-rolled helpers that exist in `pkg/`
  already, N copies of the same struct or decoder.
- Invariants enforced by convention at many call sites that should be enforced once by a seam.
- God files and god functions (list the largest, with line counts).
- Leaky seams: adapters knowing about services, services knowing about SQL/HTTP shapes, domain
  entities importing infrastructure.
- Ports with exactly one adapter and one caller (hypothetical seams), ports that are near-copies
  of each other, and "interface segregation" taken to the point of 46 tiny files.
- Error handling that swallows failures into partial success, or that retries/acks inconsistently
  between siblings.
- Test-double proliferation (hand-rolled mocks per package for the same port), tests that test
  internals, test infrastructure that every package re-implements.
- Configuration and wiring sprawl in composition roots.
- Anything that makes a "small" change fan out across many files.

## Report format

Write your report to `overhaul/findings/<NN>-<area>.md` (the orchestrator gives you NN and area).
Structure:

1. **Area map** (≤ 300 words) — what lives here, the main flow, how it is wired to the rest.
   A small Mermaid or ASCII dependency sketch is welcome.
2. **Metrics** — a table: packages, files, lines, largest files, largest functions (a quick awk
   or `gofmt`-based scan is fine), hand-rolled test doubles, ports consumed/implemented.
3. **Findings**, ranked by impact. Each finding:
   - `F<NN>.<k>` id and a one-line title
   - **Strength**: `Strong` / `Worth exploring` / `Speculative`
   - **Files**: paths with line ranges
   - **Problem**: what the friction is, with evidence
   - **Proposed change**: plain English; a sketch of the deepened module's interface is fine
   - **Benefits**: in terms of locality, leverage, and how tests get better
   - **Risk / migration**: how to land it incrementally, what could break
   - **Size**: S (one PR, < 300 lines), M (one PR, < 1000), L (2–4 PRs), XL (an epic)
   - **Depends on / enables**: other finding ids, if any
4. **Cross-area observations** — things you noticed outside your area, one line each.
5. **Open questions** — what you could not determine from the code alone.

Then return to the orchestrator a summary of **at most 350 words**: the metrics headline, your
top three to five findings with strength and size, and your single most important cross-area
observation. The full detail lives in the file, not in the summary.
