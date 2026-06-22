# Vector — Postgres / TimescaleDB runbook

Owner: vector team · Source rules: [alerts/vector-postgres.yaml](../../alerts/vector-postgres.yaml)

The Vector indexers write product state into a shared TimescaleDB. A session
left `idle in transaction` holds locks that block TimescaleDB compression
policies and, transitively (via Postgres lock-queue semantics), the indexers'
reads and writes — the 2026-06-18 lock-convoy incident that stalled every
indexer and backed up the SQS queues. These alerts surface that condition
before it cascades.

---

## Metrics coverage (staging vs prod)

These alerts read `pg_stat_activity_*` from the Timescale Cloud exporter
(`job="tigerdata"`). Each rule is pinned to its environment's Timescale service:

| Environment | Timescale service | `service_id` |
| --- | --- | --- |
| prod | `stl-sentinelprod-db` | `ucpymqz73b` |
| staging | `stl-sentinelstaging-db` | `xd7na17213` |

As of 2026-06-19 the exporter topology is wrong, and the rules are pinned per
`(cluster, service_id)` to work around it:

- **Staging is not yet covered.** The staging DB (`xd7na17213`) exports no
  metrics to Grafana Cloud, so `PostgresLongIdleInTransaction` cannot fire for
  staging. Coverage activates automatically once infra enables that service's
  metrics export labelled `cluster=archon-staging`.
- **Prod metrics also land in the staging stack** mislabelled
  `cluster=archon-staging`. The prod rule is pinned to `cluster="archon-prod"` so
  that leak cannot page as a staging alert; the staging rule is pinned to
  `service_id="xd7na17213"` so it stays immune to the leak.

Infra action plan: [staging-db-metrics-export-plan.md](../staging-db-metrics-export-plan.md).
Verify coverage with `count by (cluster, service_id) ({job="tigerdata"})` in each
Grafana stack: staging should show `xd7na17213` (not `ucpymqz73b`); prod should
show `ucpymqz73b`.

---

## PostgresLongIdleInTransaction

**Severity:** warning · **For:** 2m (idle-in-transaction > 5m)

Early warning of the condition described below. Follow the same checks and
remediation; it escalates to critical at 15m.

## PostgresLongIdleInTransactionCritical

**Severity:** critical · **For:** 1m (idle-in-transaction > 15m)

### What it means

A database session has held a transaction open and idle for 15+ minutes on the
labelled `cluster` / `service_id` / `role`. Its locks block TimescaleDB
compression jobs, and Postgres then queues the indexers' reads/writes behind the
compression job's exclusive-lock request — the indexers stall and their SQS
queues grow.

### First checks (≤5 min)

1. **Find the offending session(s):**
   ```sql
   SELECT pid, usename, client_addr, state,
          now() - xact_start AS age, left(query, 200) AS query
   FROM pg_stat_activity
   WHERE state LIKE 'idle in transaction%'
   ORDER BY xact_start;
   ```
2. **See what is blocked behind it:**
   ```sql
   SELECT pid, usename, wait_event_type, pg_blocking_pids(pid) AS blocked_by,
          left(query, 80) AS query
   FROM pg_stat_activity
   WHERE cardinality(pg_blocking_pids(pid)) > 0;
   ```

### Remediation

1. **Unblock now** (needs a role with `pg_signal_backend`, e.g. `tsdbadmin`):
   terminate the idle-in-transaction holders, scoped to the offending
   client/role:
   ```sql
   SELECT pid, pg_terminate_backend(pid)
   FROM pg_stat_activity
   WHERE state LIKE 'idle in transaction%'
     AND xact_start < now() - interval '1 minute'
     AND client_addr = '<offending host>';
   ```
   The indexers self-recover once the locks release; no pod restart is needed.
2. **Prevent recurrence:** set `idle_in_transaction_session_timeout` on the role
   the offending client uses (the original incident was an external data-catalog
   tool leaving transactions open). Where possible, point read-only/analytics
   tools at a replica so their long transactions cannot collide with compression
   and writes on the primary.

### Common causes

- An external tool (BI / data catalog / a stuck `psql` session) parks a
  transaction and never commits.
- A long-running migration or backup holding locks.
- An indexer transaction itself blocked on another lock (cascade).
