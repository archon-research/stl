# Vector — Temporal cronjobs runbook

Owner: vector team · Source rules: [alerts/vector-cronjobs.yaml](../../alerts/vector-cronjobs.yaml)

These alerts cover every Temporal cronjob built on the shared `cronjobWorkflow`
(`stl-verify/internal/adapters/outbound/temporal/`). Each cronjob runs on a
schedule in the `vector` Temporal namespace and snapshots an external source
into TimescaleDB (or validates stored data). Current cronjobs:

| Cronjob (`service_name`) | Deployment | Interval | Does |
|---|---|---|---|
| `anchorage-indexer` | `spark-anchorage-indexer` | 15m | Anchorage collateral / operations snapshots |
| `offchain-price-indexer` | `offchain-price-indexer` | 5m | CoinGecko token prices |
| `watcher-data-validator` | `watcher-data-validator` | 1h | Validates stored chain data vs Etherscan |
| `transform-worker` | `transform-worker` | 10m | Drains the transformed-layer change queues and refreshes the parity ledger |

> `maple-graphql-indexer` is also a cronjob but has its own richer rules — see
> [vector-indexers.md](vector-indexers.md), not this runbook.

The shared activity records `cronjob_runs_total{status="success"|"error"}` and
`cronjob_run_duration_seconds` per run, labelled `service_name=<cronjob>`. New
cronjobs are covered automatically; only `VectorCronjobWorkerDown` needs the new
Deployment name added to its regex.

> `transform-worker` ships at `replicas: 0` and is enabled (scaled to 1) only after
> the one-off bootstrap has run. `VectorCronjobWorkerDown` is guarded on
> `kube_deployment_spec_replicas > 0`, so a deliberately scaled-to-zero deployment
> does not page (see that section).

General triage:

```bash
kubectl -n vector get pods -l app=<deployment>      # pod health
kubectl -n vector logs deploy/<deployment> --tail=200
```

Temporal UI (vector namespace) → Schedules → `<cronjob>` shows recent runs and
the failure stack for each.

---

## VectorCronjobRunFailing

**Severity:** warning · **For:** 0m

### What it means

The labelled cronjob recorded at least one failed run in the last 15m. Recorded
once per activity execution, so a transient failure the retry policy later
recovers also fires this — that is intentional (you want to see flapping). Data
may be partial or briefly stale.

### First checks (≤5 min)

1. **Logs** — `kubectl -n vector logs deploy/<deployment> --tail=200`; find the
   error class (upstream API 4xx/5xx, DB error, decode/parse failure).
2. **Upstream API** — most cronjobs pull an external API (Anchorage,
   CoinGecko, Etherscan). Check for outage / rate limiting / auth failure.
3. **Temporal UI** — inspect the failed run's stack trace.

### Common causes

- External API rate limit or transient 5xx → usually self-heals on the next
  cycle; confirm `status="success"` returns.
- Credential expiry (API key rotated) → update the secret and restart.
- DB connection issue → check the Postgres dashboard.

### Verify recovery

`increase(cronjob_runs_total{status="error", service_name="<cronjob>"}[15m]) == 0`
and a fresh `status="success"` run.

---

## VectorCronjobAllRunsFailing

**Severity:** critical (pages) · **For:** 15m

### What it means

Zero successful runs **and** at least one failure over the last 1h for the
labelled cronjob. The worker is up and emitting metrics, but every run errors —
its TimescaleDB output is going stale. This is the sustained-failure escalation
of `VectorCronjobRunFailing`.

### First checks

1. Everything under `VectorCronjobRunFailing` — but the failure is now
   persistent, so look for a hard, non-transient cause.
2. **Recent deploys** — `kubectl -n vector rollout history deploy/<deployment>`;
   a bad release is the most common persistent cause. Roll back if so.
3. **Upstream contract / schema change** — a changed external API response or
   on-chain contract upgrade can break every run until the code is updated.
4. **Credentials / config** — a wrong or expired secret fails 100% of runs.

### Verify recovery

A `status="success"` run appears and the rule clears (errors-with-no-success
no longer holds over 1h).

---

## VectorCronjobWorkerDown

**Severity:** critical (pages) · **For:** 10m

### What it means

The labelled Deployment has <1 available replica for >10m. The cron worker is
not running, so its Temporal schedule fires with no worker to pick it up — no
snapshots/validations are produced. Sourced from kube-state-metrics, so it fires
even when the OTLP/metrics pipeline is the thing that broke (a dead worker emits
no `cronjob_runs_total`, so the metric-based alerts can't see it).

The alert is guarded on `kube_deployment_spec_replicas > 0`, so a deployment
deliberately scaled to zero does not page. This matters for triage: a
scaled-to-zero deployment (e.g. `transform-worker` before it is enabled) is
fully silent — no page and no metrics — which is expected, not an outage.
`transform-worker` must only be scaled to 1 after the one-off bootstrap has run;
if it is enabled first, the first tick runs the full parity verify inline and can
run long. Keep that rollout order explicit in the deploy notes.

### First checks

1. **Pod status** — `kubectl -n vector get pods -l app=<deployment>`. Look for
   `CrashLoopBackOff`, `OOMKilled`, `ImagePullBackOff`, `Pending`.
2. **Describe** — `kubectl -n vector describe pod <pod>` for the failure reason
   (scheduling, image, resource limits).
3. **Logs of the crashing container** —
   `kubectl -n vector logs <pod> --previous`.

### Common causes

- Crash on startup after a bad deploy → roll back.
- OOM → raise the memory limit.
- Bad image ref / registry auth → fix the tag / pull secret.
- Node pressure / unschedulable → check Karpenter and node capacity.

### Verify recovery

`kube_deployment_status_replicas_available{deployment="<deployment>", namespace="vector"} >= 1`
and a fresh `status="success"` run in `cronjob_runs_total`.

---

## Adding a new cronjob

Failure + all-failing alerts are automatic (they group by `service_name` and
exclude only maple). Two manual steps:

1. Add the new **Deployment name** to the `deployment=~"..."` regex in
   `VectorCronjobWorkerDown` (the kube-state-metrics label is the Deployment
   name, which may differ from `service_name` — e.g. `spark-anchorage-indexer`).
2. Add a row to the table at the top of this runbook.
