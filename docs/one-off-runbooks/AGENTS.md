# One-off runbooks

A procedure that runs a fixed number of times and is then finished: a data repair, a one-time
backfill of a column, a migration that has to run out of band. It names the ticket that owns it,
and it is deleted in the PR that records the run as complete — a spent procedure that stays is one
someone will follow again.

Not here:

- **Recurring operations** — an operator guide for a job that can be run again on demand, such as
  [../backfilling-offchain-prices.md](../backfilling-offchain-prices.md), lives in `docs/`.
- **Alert responses** — [../runbooks/](../runbooks/) is paired with `alerts/*.yaml`, one `##`
  section per alert rule. See [../runbooks/AGENTS.md](../runbooks/AGENTS.md).
- **What happened after the fact** — [../incidents/](../incidents/).

Measured figures (row counts, block counts, wall-clock) belong in the procedure, each one next to
the query that reproduces it and marked as the measurement it was, never as a constant to trust.
