# Overhaul

A repo-wide refactoring investigation of `stl`, started 2026-09-03, and the log of the work that
follows from it. Everything here is evidence-based: numbers come from the code and the git
history on `main` at `c4e0a8f2`, not from documentation.

| File | What it is | Read it when |
|---|---|---|
| `CANDIDATES.md` | The 14 programs of work, each stitched from several area reports, with the finding ids that feed it, strength, size and dependencies | you want to know *what* to change and why |
| `ROADMAP.md` | The candidates sequenced into phases of PR-sized slices, with the metric each phase should move | you want to know *in which order* |
| `PROGRESS.md` | Status of the investigation and, later, of each slice as it lands; resume instructions | you are picking the work up |
| `SYSTEM-MAP.md` | The shape of the repo in numbers: layers, binaries, largest files, churn | you need the raw counts |
| `BRIEF.md` | The method and vocabulary every report uses (module, seam, depth, deletion test) | you are writing or reading a report |
| `findings/NN-*.md` | Thirteen per-area reports, each with an area map, metrics, ranked findings `F<NN>.<k>`, cross-area observations and open questions | you want the evidence behind a candidate |

Conventions: finding ids are `F<area>.<n>`, candidate ids are `C<n>`. A candidate lists the
findings that feed it; a finding never repeats what its candidate says. Work lands on
PR-sized branches referenced from `PROGRESS.md`; this folder is updated in the same PR when a
slice changes a metric or closes a finding.
