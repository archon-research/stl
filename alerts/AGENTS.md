# Observability — alerts & runbooks (required for new indexers)

A new indexer / data service that emits metrics ships its alert rules **and**
runbook sections in the same PR — same definition-of-done as tests.
Before modifying either side, read both this file and
[`docs/runbooks/AGENTS.md`](../docs/runbooks/AGENTS.md); they are sibling scopes.

- Rules → a group in `alerts/vector-<service>.yaml`; runbooks → matching
  `## AlertName` sections in `docs/runbooks/vector-<service>.md`. Copy an
  existing pair (`vector-indexers.yaml` + `.md`) — their header comments carry
  the label, severity→routing, and window conventions; follow them.
- Cover at minimum: liveness/stall, error rate, silent-empty / data-quality
  holes the error path won't catch, and latency.
- A counter an alert reads with an absence shape (`increase()`/`rate()` `== 0`)
  needs its series to exist from process start. An unseeded OTel counter series
  first appears at 1, so `increase()` misses the 0->1 after every pod rollover
  (false page on error+success pairs) and a worker that dies before its first
  unit of work emits no series at all, so the stalled alert can never fire.
  Three ways to satisfy that, in order of preference: seed at construction via
  `telemetry.SeedStatusCounter` (status-labelled) or `telemetry.SeedCounter`
  (fixed labels) — Python emitters must export the equivalent 0 themselves;
  write the rule as `A > 0 unless B > 0`, which fires on an absent *or* zero
  series; or zero-fill from kube-state (`or (… ) * 0`). Open-ended label sets
  (per-operation error counters) cannot be seeded — pair those with a
  kube-state `Down` companion instead.
- Seeding starts the rule's clock at boot, so check the `for:` window before
  adding one. Unseeded, a fresh pod produced no series and the rule stayed
  silent; seeded, `rate()` reads 0 from the first export and `for:` becomes the
  deadline for the first unit of work. A counter whose work cadence approaches
  that window needs a cold-start gate — see the `offset` on
  `VectorCronjobCoreModelRunnerNoCompletions`. Seeding also only reaches
  Prometheus if the process survives one export interval, so it covers the
  Running-but-wedged pod, not a fast CrashLoopBackOff.
- Not yet seeded, and known: `reference_capital_sync_*`, `archive_writes_total`
  and `alchemy_client_requests_total` are all read with `== 0`. Treat the list
  as an audit, not as licence to add another.
- `critical` must have a `runbook_url` + runbook section; `warning`/`info` must
  have a runbook section.

## Alert ownership

- **You create it, you own it** (for now). Every alert must be actionable and
  require action when it fires. An alert that fires without needing action is
  a bug in the alert — fix it (tighten the threshold/window, add the missing
  condition, or delete it); don't leave it firing. If deletion is the choice,
  explicitly flag it to the human reviewing.
- **Silence while you work.** If you are working on an alert, silence it until
  you are finished so it stops paging/posting and doesn't cause alert fatigue.
- **Be explicit.** When silencing or deleting an alert, say so explicitly (in
  the PR / thread) and get human approval first.
