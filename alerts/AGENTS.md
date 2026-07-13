# Observability — alerts & runbooks (required for new indexers)

A new indexer / data service that emits metrics ships its alert rules **and**
runbook sections in the same PR — same definition-of-done as tests.

- Rules → a group in `alerts/vector-<service>.yaml`; runbooks → matching
  `## AlertName` sections in `docs/runbooks/vector-<service>.md`. Copy an
  existing pair (`vector-indexers.yaml` + `.md`) — their header comments carry
  the label, severity→routing, and window conventions; follow them.
- Cover at minimum: liveness/stall, error rate, silent-empty / data-quality
  holes the error path won't catch, and latency.
- `critical` must have a `runbook_url` + runbook section; `warning`/`info` must
  have a runbook section.
