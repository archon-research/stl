# Vector — CEX order book indexer runbook

Owner: vector team · Source rules: [alerts/vector-cex-orderbook.yaml](../../alerts/vector-cex-orderbook.yaml)

The `cex-orderbook-indexer` streams L2 order books from an exchange over a
WebSocket and persists periodic top-N snapshots into TimescaleDB
(`cex_orderbook_snapshots`). One pod per exchange
(`service_name=cex-orderbook-indexer-<exchange>`, label `exchange`).

There is no SQS queue behind it: a failed write is dropped and the next tick
re-samples the live book, so transient blips self-heal on their own. These
alerts fire on the failure modes that do **not** self-heal.

---

## VectorCexOrderbookPersistFailing

**Severity:** critical · **For:** 15m

### What it means

Every snapshot write for the labelled `exchange` has failed for >15 minutes.
The WebSocket is probably still up (books are fresh in memory) but nothing is
reaching TimescaleDB — a silent data hole. Because the indexer drops failed
ticks by design, it will not recover on its own from a permanent cause.

### First checks (≤5 min)

1. **Pod status & logs** — `kubectl -n vector logs -l app=cex-orderbook-indexer-{{exchange}} --tail=200`.
   Look for the `failed to persist order book snapshots` error and its wrapped
   cause.
2. **Classify the cause from the error:**
   - `password authentication failed` / permission denied → DB credential or
     grant problem (auth). The secret or role is wrong — fix and restart.
   - `relation "cex_orderbook_snapshots" does not exist` → the migration did not
     run in this environment. Run the migrate job.
   - `timeout` / `too many connections` / pool exhausted → DB under load or pool
     too small; check the Postgres dashboard.
3. **TimescaleDB health** — connection pool exhaustion or a stuck node stalls
   writes; check the Postgres dashboard.

### Common causes

- DB credentials rotated without updating the pod's secret → restart after the
  ExternalSecret syncs.
- Migration not applied in the target environment → run migrate.
- DB pool saturated → restart the pod; longer-term raise the pool limit.

### Verify recovery

`rate(orderbook_persist_failures_total{exchange="<exchange>"}[10m]) == 0` and
fresh rows landing: `SELECT max(persisted_at) FROM cex_orderbook_snapshots WHERE exchange = '<exchange>'`.

---

## VectorCexOrderbookStreamStalled

**Severity:** critical · **For:** 10m

### What it means

The oldest symbol on the labelled `exchange` has had no order book update for
>120s sustained over 10m. The upstream feed has silently gone dead; snapshots
are stale and stale symbols stop being written, so the series flat-lines.

### First checks (≤5 min)

1. **Pod status & logs** — `kubectl -n vector logs -l app=cex-orderbook-indexer-{{exchange}} --tail=200`.
   Look for reconnect churn (`orderbook.reconnections.total`) or
   `skipping stale order books`.
2. **Exchange status** — check the exchange's status page / API health; a venue
   outage or a symbol delisting stops updates.
3. **Symbol config** — a bad/renamed symbol can wedge the feed (see the Kraken
   `XBT`/`XDG` aliasing case). Confirm `SYMBOLS` matches the venue's current
   pairs.
4. **Network egress** — confirm the pod can reach the exchange WebSocket
   endpoint.

### Common causes

- Exchange-side outage or rate-limit / IP ban → wait it out or rotate egress.
- Misconfigured/renamed symbol reconnect-looping the feed → fix `SYMBOLS`.
- WebSocket wedged without a clean close → restart the pod.

### Verify recovery

`max(orderbook_last_update_age{exchange="<exchange>"}) < 120` and
`rate(orderbook_updates_emitted_total{exchange="<exchange>"}[5m]) > 0`.
