# Vector — backup worker runbook

Owner: vector team · Source rules: [alerts/vector-backup-worker.yaml](../../alerts/vector-backup-worker.yaml)

The backup worker (`stl-backup-worker`) consumes SQS messages produced by the
chain watcher and writes raw block data to S3 for long-term archive and
replay. It runs per-chain.

---

## VectorBackupWorkerStalled

**Severity:** critical · **For:** 15m

### What it means

`stl-backup-worker` on the labelled `chain` has not advanced a block in
15 minutes **while the watcher is still producing on the same chain**. The
`and on(chain)` clause prevents this firing during a watcher outage (that's
`VectorWatcherNoBlocks`'s job).

### First checks (≤5 min)

1. **Pod status** — `kubectl -n vector get pods -l app=stl-backup-worker`.
   Look for crash/restart loops or `OOMKilled`.
2. **Recent logs** — `kubectl -n vector logs <pod> --tail=200`. Common
   patterns: SQS `ReceiveMessage` failures, S3 `PutObject` failures, or
   downstream decode panics.
3. **SQS depth** — check `ApproximateNumberOfMessages` and the DLQ via the
   AWS console. A growing DLQ points at a poison message.

### Common causes

- Pod stuck on a poison message → drain DLQ, redrive after fixing the
  decoder.
- S3 IAM permission drift after an account move → verify the IRSA service
  account.
- SQS visibility-timeout misconfiguration → messages reappear before
  processing finishes; symptom is sustained re-deliveries in logs.

### Verify recovery

`rate(blocks_processed_total{service_name="stl-backup-worker"}) > 0` for
the affected chain. SQS queue depth trends back down.

---

## VectorBackupWorkerLatencyHigh

**Severity:** warning · **For:** 15m

### What it means

p99 SQS→S3 processing time is over 30 seconds sustained for 15 minutes.
Approaching the SQS visibility timeout means messages may start being
redelivered and the queue depth will grow.

### First checks

- Pod CPU / memory throttling (`kubectl top pod -n vector`).
- S3 write latency from the AWS S3 console / CloudWatch.
- Recent deploy that may have increased per-block work.

### Verify recovery

p99 processing duration trends back under 30s.

---

## See also

- Watcher runbook: [vector-watcher.md](vector-watcher.md)
- Indexers runbook: [vector-indexers.md](vector-indexers.md)
