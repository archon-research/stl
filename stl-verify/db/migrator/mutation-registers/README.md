# Mutation sweep — VEC-616 registers

`./run.sh` breaks one thing at a time in
`db/migrations/20260915_120000_secstore_registers_and_id_vocabularies.sql`, runs the register
tests, and records whether anything noticed. Current state is in [RESULTS.md](RESULTS.md).

A mutation that **survives** is a behaviour nothing is testing. That is the only interesting
outcome; a green suite looks identical whether or not it is checking anything.

## Why this exists, and what replaces it

Review found 18 of 20 mutations surviving against the register reads. The tests that close that
are in `secstore_registers_reads_integration_test.go`; this directory is the evidence that they
do, in a form a reviewer can re-run rather than take on trust.

**It is superseded by #984 (VEC-786)**, which builds the real harness in `../mutation/`:
`mutate.py` derives mutations from the SQL and emits them as JSON line-patches with stable ids,
`run.sh` applies each in a scratch worktree. This is the same idea with `sed` and no
dependencies, written because #984 had not landed and the evidence should not live only in a
review comment.

Folding in is mechanical, and should happen when #984 merges:

1. add this migration to `mutate.py`'s file list;
2. add the operators from `run.sh`'s `CATALOGUE` to the matching families — `read_order`,
   `tiebreak`, `read_window`, `guard`, `constraint`;
3. widen `run.sh`'s `TEST_PATTERN` to cover the register tests;
4. delete this directory.

## RESULTS.md is committed on purpose

`../mutation/.gitignore` currently ignores its own `RESULTS.md`. A sweep whose output is not in
the tree cannot be checked by a reviewer, which is most of the value of having run it — so this
one is committed, and regenerating it is part of changing the resolution reads.

## A caution from writing it

The first run reported two survivors. Both were bugs in this script, not gaps in the tests: the
test pattern was `TestRegisters`, which does not match `TestRegisterReads…`, so the valid-time
bound test never ran; and BSD `sed` silently ignores `\|` alternation in a basic regex, so the
index mutation matched nothing and the migration was never actually broken.

A mutation harness fails quietly in exactly this way — a mutation that was never applied is
indistinguishable from one nothing caught. Treat a survivor as a question about the harness
before treating it as a question about the tests, and check the mutation really landed.
