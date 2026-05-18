# Handoff: CEX feed watcher / orderbook indexer split

Branch: `feature/cex-orderbook-indexer`
Status: code committed and pushed (HEAD `0d98b20`).

This doc captures the design decisions behind that commit so a fresh session
can pick up without re-deriving them. Read the commit + this doc and you're
caught up.

## What was done

Split the previous monolithic `orderbook-watcher` into two binaries:

```
cex-feed-watcher (1 pod per CEX)        orderbook-indexer (N pods, source-agnostic)
  WS connect/reconnect                   SQS consume
  per-CEX subscribe + ping               envelope → source → parser dispatch
  forward raw frames → SNS               write-through cache (Redis)
                                         batched DB flush (10s)
```

- **Watcher**: `cmd/base/cex-feed-watcher` + `internal/services/cex_feed_watcher`.
  One binary, selects exchange via `CEX_NAME` env. Imports `cex.WSProtocol`
  (per-CEX subscribe/ping) — does NOT parse payloads.
- **Indexer**: `cmd/workers/orderbook-indexer` + `internal/services/orderbook_indexer`.
  One binary, holds `map[source]cex.Parser`, dispatches per message.

## Key design decisions (non-obvious from the diff)

1. **Watcher is payload-agnostic.** Forwards raw bytes including heartbeats.
   The indexer's parser returns `(nil, nil)` on non-data frames — cheap. Trade:
   small SNS waste for a fully generic watcher. The watcher could be the
   foundation of a future generic `pkg/wsclient`.

2. **One binary per CEX deployment, not one binary per role.** k8s gets one
   Deployment base + one overlay per exchange setting `CEX_NAME`. If Binance's
   pod crashes it has zero blast radius on others or on indexers. Code stays
   trivially generic — just config selection.

3. **Standard SNS, not FIFO.** Orderbook snapshots are point-in-time values,
   not deltas. Strict ordering and dedup don't pay for themselves. The
   block-watcher path stays FIFO because per-chain order and rolling-deploy
   dedup do matter there. Different topics.

4. **Cache (Redis) writes happen in the indexer, not the watcher.** For
   blocks, the cache deduplicates expensive Alchemy fetches (1 watcher → many
   workers reading from cache). For CEX, the WS payload arrives free — there
   is no upstream cost to deduplicate. Cache is purely a read-side latency
   helper for "latest" snapshot lookups, which the indexer can write just as
   well. This keeps the watcher zero-state and never needing to parse.

5. **Parser interface split.** `cex.WSProtocol` (Exchange + SubscribeMessage +
   PingMessage) is watcher-side. `cex.Parser` (Exchange + ParseMessage) is
   worker-side. Each concrete CEX struct satisfies both. The watcher binary
   never calls ParseMessage; it's just dead-code-eliminated.

6. **Per-message handling on the worker:**
   - Malformed envelope → log + delete (poison pill, no retry)
   - Unknown source → log + delete (config drift, no retry)
   - Parse error → log + DO NOT delete (transient, let SQS visibility timeout
     redeliver, eventually DLQ)
   - Success → cache write-through + buffer + delete

7. **Long-term abstractions** (not extracted yet, but the shapes are
   positioned for it): generic `pkg/wsclient` (the current WSConnection minus
   the per-CEX WSProtocol coupling), generic `RawEventPublisher` port (the
   current CEXFeedPublisher rebranded), generic SQS-driven worker shell. The
   block watcher is explicitly NOT being migrated — its reorg + cache-pointer
   semantics are genuinely different.

## Required env vars (gitignored .env files DO NOT transfer)

Recreate these on the laptop after pulling.

`stl-verify/cmd/base/cex-feed-watcher/.env`:
```
CEX_NAME=binance
AWS_REGION=us-east-1
AWS_SNS_ENDPOINT=http://localhost:4566
AWS_SNS_CEX_FEED_TOPIC_ARN=arn:aws:sns:us-east-1:000000000000:stl-cex-feed
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
```

`stl-verify/cmd/workers/orderbook-indexer/.env`:
```
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/stl_verify?sslmode=disable
REDIS_ADDR=localhost:6379
AWS_REGION=us-east-1
AWS_SQS_ENDPOINT=http://localhost:4566
AWS_SQS_CEX_FEED_QUEUE_URL=http://localhost:4566/000000000000/stl-cex-feed
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
DB_FLUSH_INTERVAL=10s
```

## Out of scope (intentional, do these next)

- **k8s manifests**: one Deployment base for watcher + per-CEX overlay
  (`CEX_NAME` patch); one Deployment for indexer. Mirror existing watcher
  patterns in `k8s/base/` and `k8s/overlays/`.
- **Dockerfiles** for `cex-feed-watcher` and `orderbook-indexer` (mirror
  `stl-verify/Dockerfile`).
- **Makefile** targets: `run-cex-feed-watcher`, `run-orderbook-indexer`,
  `docker-release-cex-feed-watcher`, `docker-release-orderbook-indexer`.
- **Infra (separate repo)**: SNS topic `stl-cex-feed` (standard), SQS queue
  `stl-cex-feed` subscribed to it, IAM grants for watcher (sns:Publish) and
  indexer (sqs:Receive/Delete).
- **LocalStack wiring** in `make dev-up` (create the topic + queue + binding
  on cluster bring-up so local end-to-end works).

## Verification at commit time

- `go build ./...` ✓
- `go vet ./...` ✓
- `gofmt -l .` clean ✓
- `go test -race` on all affected packages ✓ (cex, sns, cex_feed_watcher,
  orderbook_indexer, domain/entity)
- Integration tests for postgres/redis still need Docker — unchanged.
