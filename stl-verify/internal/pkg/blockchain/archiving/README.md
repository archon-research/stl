# archiving — raw smart-contract call audit decorator (VEC-81)

This package decorates any `outbound.Multicaller` so every batch of contract
calls is mirrored to an `outbound.CallArchiver`. The S3 implementation lives
at `internal/adapters/outbound/s3.CallArchiver` and writes one zstd-compressed
JSONL object per `Multicaller.Execute` invocation, under the version-aware key
produced by `internal/pkg/rawsckey.Build`.

## Why a decorator?

- Zero changes to service / domain code.
- Failures are fail-open: a broken S3 cannot break ingest.
- Captures raw bytes _before_ ABI decoding — the archive is the audit truth.

## Properties

| Property      | Choice                                                                  |
|---------------|-------------------------------------------------------------------------|
| Granularity   | One JSONL file per `Multicaller.Execute` call                           |
| Format        | newline-delimited JSON, zstd-compressed                                 |
| Idempotency   | `IfNoneMatch=*` PUT; key includes UTC timestamp                         |
| Failure mode  | Log + swallow (fail-open)                                               |
| Bucket        | Single shared `RAW_SC_BUCKET` (NOT per-chain)                           |
| `bv` plumbing | `archiving.WithBlockVersion(ctx, ev.Version)` in each per-block handler |
| `build_id`    | Resolved once at startup via `buildregistry.New(...)`                   |

> **Divergence from VEC-81 ticket**: the key uses `build_id=` instead of the
> ticket's `pv=` (processing_version). `pv` is per-row, assigned by a Postgres
> trigger, and not knowable at archive time without a DB round-trip. `build_id`
> is its upstream cause and is 1:1 with `pv` for any (natural_key, block, bv)
> tuple, so audit joins remain trivial. Pending team confirmation.

## Wire-up recipe (per service)

### 1. Add the feature flag + bucket env vars to the service's k8s manifest

```yaml
env:
  - name: ARCHIVE_SC_CALLS
    value: "true"
  - name: RAW_SC_BUCKET
    value: "stl-raw-sc-calls-prod"
```

### 2. In `cmd/.../main.go`, build the archiver and wrap the multicaller

See `cmd/backfillers/oracle-pricing-backfill/main.go` (reference impl). The
pattern is a ~25-line helper that the service `run()` calls only when
`ARCHIVE_SC_CALLS=true`:

```go
if env.Get("ARCHIVE_SC_CALLS", "") == "true" {
    wrap, err := newArchivingWrap(ctx, logger, chainID, int(buildReg.BuildID()), "<source>")
    if err != nil { return fmt.Errorf("init SC-call archiver: %w", err) }
    inner := newMulticaller
    newMulticaller = func(args...) (outbound.Multicaller, error) {
        mc, err := inner(args...)
        if err != nil { return nil, err }
        return wrap(mc), nil
    }
}
```

Where `<source>` is the indexer name with the `-indexer` suffix stripped:

| Binary                              | Source name        |
|-------------------------------------|--------------------|
| `oracle-price-indexer`              | `oracle-price`     |
| `sparklend-indexer`                 | `sparklend`        |
| `morpho-indexer`                    | `morpho`           |
| `prime-debt-indexer`                | `prime-debt`       |
| `prime-allocation-indexer`          | `prime-allocation` |
| `oracle-pricing-backfill`           | `oracle-price`     |
| `sparklend-backfill`                | `sparklend`        |
| `morpho-vault-indexer` (backfill)   | `morpho`           |
| `aave-like-user-snapshot-indexer`   | `sparklend`        |

### 3. In each live (non-backfill) service, plumb the block version

Backfills are canonical and `bv=0` is correct. Live workers MUST tell the
decorator the actual reorg version on every block handler entry:

```go
func (s *Service) handleBlock(ctx context.Context, ev BlockEvent) error {
    ctx = archiving.WithBlockVersion(ctx, ev.Version)
    // ... existing per-block logic ...
}
```

Without this, reorged blocks (bv > 0) would silently archive under `bv=0`,
breaking audit reconciliation for the reorged versions.

## TODOs flagged for the team

1. **`build_id` vs `pv`** — confirm divergence from ticket is acceptable, or
   add a per-call DB lookup (not recommended — high write-amplification).
2. **Bucket lifecycle** — confirm 365-day Glacier transition (and any
   shorter-tier requirements) and codify in Terraform.
3. **Metrics** — add a `sc_call_archive_failures_total` counter once the
   service-side OTel meter pattern is standardised.
