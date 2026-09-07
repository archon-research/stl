---
title: Backfilling Off-Chain Prices - Operator Guide
audience: [developers, operators, ai-agents]
repo: stl
applies_to: stl-verify
job: stl-verify/cmd/backfillers/offchain-price-backfill
task_queue: offchain-price-backfill
workflow_type: OffchainPriceBackfill
related_docs:
  - docs/temporal_guide.md              # how on-demand Temporal jobs work, and how to add one
  - docs/price-sources.md               # which assets exist and their CoinGecko IDs
  - docs/runbooks/vector-cronjobs.md    # what to do when the worker pod is down
---

# Backfilling Off-Chain Prices

How to load historical CoinGecko prices into `offchain_token_price` for a date range
you choose. This is the operator's view — for how the job is built, or how to add
another on-demand job, see [temporal_guide.md](temporal_guide.md).

The live `offchain-price-indexer` cronjob already writes **current** prices every 5
minutes. This job fills in **history**, backwards, into the same rows. You only need
it when you want data older than whatever the indexer has already collected.

## Before you start

The worker is a long-running pod with **no schedule** — it idles until you trigger
it. Confirm it is up, or your workflow will queue with nothing to pick it up:

```bash
kubectl --context archon-staging -n vector get pods -l app=offchain-price-backfill
# want: 1/1 Running
```

## Trigger it

Open the Temporal UI and **switch the namespace to `vector`** — the UI lands on
`default`, which is empty for us:

```
http://temporal-staging:8080/namespaces/vector/workflows
```

Click **Start Workflow** and fill in:

| Field | Value |
|---|---|
| Task Queue | `offchain-price-backfill` |
| Workflow Type | `OffchainPriceBackfill` |
| Workflow ID | anything unique and descriptive, e.g. `backfill-weth-wbtc-2020` |
| Input | the JSON below |

```json
{"assets":["weth","wrapped-bitcoin"],"from":"2020-01-01T00:00:00Z","to":"2026-03-18T00:00:00Z"}
```

Then **Start**. That example is ~162 activities and takes about **2 minutes**.

The **Workflow ID is the concurrency guard**: Temporal refuses a duplicate while a
run with that ID is in flight, so a double-click cannot launch the same backfill
twice. To re-run later, use the same form with a new ID.

### Equivalent CLI

The UI is the intended path (the gRPC port is not exposed outside the cluster). If
you would rather script it, port-forward first:

```bash
kubectl --context archon-staging -n temporal port-forward svc/temporal-server 7233:7233

temporal workflow start --address 127.0.0.1:7233 --namespace vector \
  --task-queue offchain-price-backfill --type OffchainPriceBackfill \
  --workflow-id backfill-weth-wbtc-2020 \
  --input '{"assets":["weth","wrapped-bitcoin"],"from":"2020-01-01T00:00:00Z","to":"2026-03-18T00:00:00Z"}'
```

## What to put in `assets`

**CoinGecko IDs, not token symbols.** `weth` works; `WETH` fails immediately with
`unknown source asset IDs`. Valid values are the `source_asset_id` values registered
in `offchain_price_asset` — currently these 18:

| CoinGecko ID | Symbol | | CoinGecko ID | Symbol |
|---|---|---|---|---|
| `dai` | DAI | | `usds` | USDS |
| `savings-dai` | sDAI | | `susds` | sUSDS |
| `usd-coin` | USDC | | `wrapped-bitcoin` | WBTC |
| `tether` | USDT | | `coinbase-wrapped-btc` | cbBTC |
| `paypal-usd` | PYUSD | | `lombard-staked-btc` | LBTC |
| `weth` | WETH | | `tbtc` | tBTC |
| `wrapped-steth` | wstETH | | `gnosis` | GNO |
| `wrapped-eeth` | weETH | | `rocket-pool-eth` | rETH |
| `renzo-restaked-eth` | ezETH | | `kelp-dao-restaked-eth` | rsETH |

To confirm the current set:

```sql
SELECT a.source_asset_id, t.symbol
FROM offchain_price_asset a
JOIN offchain_price_source s ON s.id = a.source_id
LEFT JOIN token t ON t.id = a.token_id
WHERE s.name = 'coingecko' AND a.token_id IS NOT NULL
ORDER BY 1;
```

Assets with no `token_id` cannot be backfilled — there is nowhere to store them. That
is why native BTC and ETH are absent (see VEC-539).

## Watching a run

- **Query tab → `progress`** gives `chunksDone / chunksTotal` live. Most useful on
  long runs.
- **Event History** shows one activity per chunk as it completes.

## Reading the result

The Result panel returns per-asset coverage. This is the important part — a run can
succeed while covering less than you asked for:

```json
{
  "chunksRun": 162,
  "coverage": {
    "weth": {
      "points": 57094, "chunks": 81,
      "emptyLeading": 0, "emptyAfterData": 0,
      "coveredFrom": "2020-01-01T00:00:00Z"
    }
  }
}
```

| Field | Meaning |
|---|---|
| `points` | Points the **provider returned**, not rows written. A re-run over filled data reports the same number having inserted nothing. |
| `coveredFrom` | Where data actually starts. **Compare this against your requested `from`.** |
| `emptyLeading` | Windows before the first data. Ambiguous — the asset may post-date your `from`, or the API plan may not reach that far back. Reported, not failed. |
| `emptyAfterData` | Windows with no data *after* data began. Always a real hole — the run **fails** if any occur. |

So `emptyLeading: 4` with `coveredFrom` four months after your `from` means the range
was truncated. That is usually correct behaviour (WBTC did not exist before January
2019), but it is worth noticing rather than assuming you got everything.

## Verifying afterwards

```sql
SELECT t.symbol, COUNT(*) AS rows,
       MIN(p.timestamp) AS earliest, MAX(p.timestamp) AS latest
FROM offchain_token_price p
JOIN token t ON t.id = p.token_id
JOIN offchain_price_source s ON s.id = p.source_id
WHERE t.chain_id = 1 AND s.name = 'coingecko'
  AND t.symbol IN ('WETH','WBTC')
GROUP BY t.symbol;
```

Roughly **8,760 rows per asset per year** (hourly). Spot-check a known value — WETH
was ~$128.63 and WBTC ~$7,157.53 at 2020-01-01T00:00Z.

## Two data characteristics that surprise people

**`market_cap_usd` is 0 for most history on wrapped assets.** That is CoinGecko's own
data, not a bug here — verified 0 at 2020, 2021, 2022 and 2023 while current values
read billions. `price_usd` and `volume_usd` are good throughout. Do not chart market
cap across the full range without checking.

**Small gaps are normal and usually upstream.** A local audit of 68 gaps found 67 were
CoinGecko holes — the API returns nothing for those hours. Before treating a gap as a
defect, ask the API directly for that window; if it returns no points, there is
nothing to fetch. Note the run's guard detects *empty windows*, not *sparse* ones, so
a window returning 19 of 721 points passes as served.

## Troubleshooting

| Symptom | Cause |
|---|---|
| Workflow queues, nothing happens | Worker pod not running — see the prerequisite above |
| `unknown source asset IDs [...]` | Symbol instead of CoinGecko ID, or an unregistered asset. Fails on attempt 1 |
| `returned no price points across the whole range` | Wrong ID, or the range is outside the API plan's history. Fails on attempt 1 |
| `invalid backfill parameters` | Bad JSON input — missing `from`/`to`, `from` ≥ `to`, duplicate assets, or a range so large it exceeds the chunk ceiling. No activity runs |
| `empty window(s) after data began` | A genuine hole mid-series. Re-run that sub-range; if it recurs, check the API for that window |
| An activity on **Attempt 2+** | A real error, not a timeout. Read the message — deterministic failures fail on attempt 1 by design |

## Local development

```bash
cd stl-verify
make dev-up                                        # kind cluster incl. Temporal + TimescaleDB
make dev-env                                       # writes the .env files dev-env-files names
make run-backfiller-offchain-price-backfill        # run the worker on the host
```

Local Temporal UI: `http://127.0.0.1:8233/namespaces/vector/workflows`.

`dev-env` covers only the jobs the `dev-env-files` target names — `offchain-price-backfill`
is one of them. For a backfiller it does not cover, copy a covered job's `.env` and edit
the job-specific keys.

Note `make dev-env` fetches the `coingecko_api_key` secret, which has been observed
deactivated; if requests come back HTTP 401, put a working key in
`cmd/backfillers/offchain-price-backfill/.env.local`, which the run target loads after
`.env`.

## Facts worth knowing

| | |
|---|---|
| Chunk size requested | 30 days per activity |
| Provider's hourly ceiling | **90 days** — past that CoinGecko silently returns daily data |
| Full 2020→now, both assets | ~162 activities, ~2 minutes, ~57k rows per asset |
| Per-chunk timeout | 10 min, with a 30-min envelope including retries |
| Re-running a filled range | Safe and additive — `ON CONFLICT DO NOTHING` |
| Rows are never deleted | Corrections append a new `processing_version` |
