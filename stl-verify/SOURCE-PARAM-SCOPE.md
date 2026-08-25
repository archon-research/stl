# Scope: replace `reference=<bool>` with `source=indexed|reference|both`

Branch `rohit/source-param-both-mode`, off `rohit/prime-collateral-cards` (PR #729).
Working document — not committed.

**Status:** all decisions settled (§2). **Every planned PR is implemented** — 18 commits on `rohit/source-param-both-mode`, nothing pushed, no PR opened. Revised after the upstream probe —
see `UPSTREAM-DATA-CATALOGUE.md`, which changes §3 and §5 materially: a
**proxy-scoped** upstream allocations endpoint exists, and switching the reference
side to it takes the join from 6/12 rows to 25/44.

---

## 1. What was asked

1. Replace the boolean `reference` query param with `source`, taking `indexed`,
   `reference` or `both`, where **`both` is the default** (an absent/empty param).
2. In `both`, show allocations tracked in *both* places, with every metric adapting.
3. Deduplicate the allocation rows — expected to be "trivial using the underlying
   or balance address".

Point 3 is where the work actually lives, and the measurements below say it is not
trivial. Everything else is mechanical. **Read §3 before §5** — two findings there
change what `both` can honestly mean, and one of them contradicts an assumption in
the request.

---

## 2. Decisions

All settled. D2a and D5 came out of the upstream probe and are recorded below with
the implementation consequences each carries.

### D1 — `both` is the default ✅ *decided: yes*

`source` absent means `both`, so the default flips from indexed-only to a merged
view: a third-party feed moves onto the critical path of every page load, and every
existing consumer of these endpoints changes provenance.

**Decided: `both` is the default.** So D5 is no longer an edge case — it is the
first paint of every page, and the work below treats it that way.

Two consequences to build around, both now measured:

- The grove 502 is **our bug, not upstream absence** (catalogue §6): grove's
  reference data is served fine on every upstream endpoint, and STL drops all 15
  rows because 2 sit on `plume` / `robinhood`, which `CHAIN_ID_TO_NAME` has never
  been taught. **Fixing that is a prerequisite of a `both` default**, otherwise half
  the primes default to a view whose reference half is an error.
- The `Cache-Control` reasoning in `exposure.py:96` needs revisiting: a merged
  response's reference half is a live upstream read, not an immutable observation.

### D2 — Union ✅ *decided*

"Allocations tracked both places" can mean the union of the two sources (dedup'd), or
only positions appearing in both. The mention of dedup implies union.

**Decided: union**, with each row carrying which provenance(s) produced it, so
"tracked in both" is visible and filterable rather than a hidden filter.

Row counts for spark, against the better upstream endpoint (catalogue §5): 27 indexed
+ 44 upstream-on-indexed-chains → **45 rows after dedup**, of which 25 are matched
pairs, 19 upstream-only and 1 STL-only. A further 15 upstream rows sit on chains STL
has no proxy for (arbitrum, unichain, optimism, robinhood) — see D2a.

### D2a — Include rows on chains STL does not index ✅ *decided: include*

Indexing support for those chains gets added gradually, so the union covers everything
Sky reports from day one and fills in from STL's side over time.

**This forces a response-model change.** `AllocationResponse.chain_id` is today a
required, non-nullable `int` (`allocations.py:130`), and **`0` is already the off-chain
custody sentinel** — so an unmappable network has no representable value. That is
exactly why `_reference_allocation_row` raises today rather than emitting a row.

So D2a and PR 0 are the same change, and it needs:

- **`chain_id: int | None`** — null meaning "a chain this vocabulary has not been
  taught", distinct from `0` meaning off-chain. The docstring at `allocations.py:121`
  has to say so, since it currently documents `0` as the only special case.
- **A new `network: str` field** carrying the upstream name verbatim (`plume`,
  `robinhood`, `xlayer`), because with a null `chain_id` there is otherwise nothing to
  label the row with. STL's `chain` registry has no row for these, so the label cannot
  be resolved the usual way.
- **UI: a chain label fallback.** `getChainLabel(chain_id, chainLabels)` cannot resolve
  a null id; it needs to fall back to the `network` string.
- **`getAllocationKey` must not collide.** Its direct-holding branch keys on
  `chain_id`, so two null-chain rows with the same symbol on different networks would
  share a key. The key needs `network` in it once `chain_id` can be null.

Rows on unindexed chains are, by definition, reference-only — there is no indexed half
to merge them with, and they must never be counted as "tracked in both".

### D3 — Align the response vocabulary ✅ *decided: agreed*

`source` is already a **response** field on four envelopes, with a different
vocabulary: `Literal["self", "reference"]` — `self`, not `indexed`.

- `app/api/v1/exposure.py:46`
- `app/api/v1/prime_risk_capital.py:160`
- `app/api/v1/total_capital.py:91`
- `app/api/v1/prime_debts.py:82`

So `?source=indexed` would return `{"source": "self"}`. There is no existing `source`
*query* param anywhere, so the name is free — it just reads inconsistently.

**Decided: keep the param name `source`, and align the response vocabulary to
`indexed | reference | both`.** Breaking response change, consumed by the TS typegen
(`ts/ui/src/generated/openapi-schema.json`), so it lands in PR A rather than a
follow-up. `make export-openapi-schema` must be re-run and committed (CI gate).

### D4 — Break the scalars down per allocation ✅ *decided: yes, and it reconciles*

You asked whether the scalar endpoints can be broken down by allocation. **Yes for
four of six figures, and the parts sum to the whole almost exactly** (catalogue §4,
measured on spark):

| Figure | Per-allocation source | Reconciles to |
|---|---|---|
| Exposure | `rrc/primes/{star}/allocations` → `exposure` | −0.0000% |
| Required risk capital | same → `rrc` (and `crr` per row) | −0.0002% |
| Assets | `allocations/?prime=` → `assets` | **0.0000%** |
| Allocated / idle assets | same → `allocated_assets`, `idle_assets` | **0.0000%** |
| **Total risk capital** | — | **not decomposable** |
| **Encumbrance ratio** | numerator only | **partly** |

`total_rc` is the treasury balance (`total_rc == prime.treasury_balance` to the cent)
and has no per-allocation column — risk capital is held by the prime, not by a
position. Encumbrance is `total_rrc / total_rc` (verified), so its numerator
decomposes and its denominator does not: each position can carry a share of
encumbrance as `rrc_i / total_rc`, which is a real breakdown even though the ratio is
prime-level.

**Decided:** a per-allocation breakdown replaces the 422 I originally proposed. So
`/risk-capital?source=both` becomes well-defined — it returns both provenances'
totals **plus** the per-allocation attribution — and the encumbrance and exposure
cards can be drilled into rather than being opaque prime-level numbers.

One constraint survives: the two provenances populate **disjoint scalar field sets** —
reference-only `junior`/`senior` splits, the internal/external/tokenized breakdown and
the utilizations; indexed-only `model` and `unpriced_reason`
(`prime_risk_capital.py:348-390`, `:479`). So the two *totals* sit side by side, never
merged into one object. What merges is the per-allocation table beneath them.

### D5 — Degrade silently by redirecting ✅ *decided*

**Decided: no notices and no error states.** When a source is unavailable, that option
*and* `both` are disabled in the settings menu, the URL is rewritten to whichever
source is available, and the page redirects there. The rest of the UI just renders the
source it was given.

This is a presentation decision, not a data one — the response still states its own
provenance in `source`, and the disabled radio plus the rewritten URL are the signal.
Nothing is served as a figure it is not. So it does not conflict with the "never
swallow a failure into partial success" rule in `AGENTS.md`, which is about persisting
holes that look healthy.

Four things it needs, all consequences rather than open questions:

1. **Availability has to be known before render, and it is per prime, not global.**
   Grove's reference half fails while spark's works. Cheapest place to answer it is the
   prime list the UI already loads first: an `available_sources: ["indexed",
   "reference"]` per prime on `/v1/primes`. The alternative — attempt, fail, redirect —
   pays a failed upstream round trip on every page load and makes the redirect
   observable as a flash.
2. **The redirect must be a full document load**, not a client-side navigation.
   `lib/referenceMode.ts` reads the mode once per session on purpose, so that a cached
   series and its refresh cannot disagree about provenance. A document load restarts
   the session and preserves that invariant; the settings menu already does exactly
   this via `globalThis.location.assign` (`SettingsMenu.tsx:211`).
3. **Guard against a redirect loop.** Availability can flap, and the check itself can
   fail. Redirect at most once per document load, and treat "cannot determine
   availability" as `indexed` rather than retrying.
4. **Sticky degradation is the one wrinkle worth a decision later.** Land on grove with
   the default, get rewritten to `?source=indexed`, then switch to spark — the URL now
   pins `indexed`, so you stay there even though `both` is available again. Options:
   re-evaluate on every prime switch (a second redirect, but the mode always matches
   what the prime supports), or leave it sticky and let the menu re-enable the option.
   I lean **re-evaluate**, since the default is meant to be `both`. Not blocking: it
   only bites after PR 0, which makes genuinely-unavailable primes rare.

One consequence to accept knowingly: a shared `?source=reference` link can render as
`indexed` for the recipient if the feed is down for them. The URL they end up on says
`indexed`, so it is recoverable, but the link no longer means what its sender intended.

---

## 3. What the data says about dedup

Measured against staging and the live upstream feed, not assumed.

> **Revised after the probe.** §3.1–3.3 describe the *Star monitor* list, which is what
> reference mode reads today — they are why dedup is not trivial on the current feed.
> §3.5 supersedes them: a better upstream endpoint exists, and the recommendation is
> now to switch the reference side to it rather than to work around the limits below.

### 3.1 The proposed keys do not exist in the response

Every reference allocation row, all 12 for `spark`:

| Field | Populated |
|---|---|
| `underlying_token_address` | **0/12** |
| `balance` | **0/12** |
| `receipt_token_address` | 10/12 |
| `receipt_token_id` | 7/12 |

`underlying_token_address` is null *by construction*, not by accident:
`_reference_allocation_row` (`allocations.py:765`) nulls it deliberately, because the
response model requires the token id and address together ("both or neither") and
upstream carries no registry id for the loan token. `balance` is null because the
monitor reports USD exposure only.

So dedup **cannot** be done client-side from today's response shape. It either happens
server-side, where the underlying address is in hand, or the response has to start
carrying it.

### 3.2 The underlying address exists upstream — but is not a unique key

Good news: `ReferenceAllocation.loan_token_address` is parsed
(`adapters/sky/reference_risk_capital_client.py:229`) and currently consumed by
nothing. I probed the live feed: it is a real 20-byte address on **12/12** rows.

Bad news, and this is the finding that changes the design — it is not unique per
position. For `spark`, 12 positions collapse to **6** distinct `(network,
loan_token_address)` pairs:

```
4 rows share USDC  0xa0b86991c6…: ANCHORAGE, sparkPrimeUSDC1, sparkUSDCbc, spUSDC
3 rows share USDT  0xdac17f958d…: spUSDT, UNI-V4-USDT-USDS, sparkUSDTbc
2 rows share PYUSD 0x6c3ea90364…: UNI-V4-PYUSD-USDS, spPYUSD
```

Keying on the underlying would merge a SparkLend position with a Morpho vault with a
Uniswap V4 LP because they all lend USDT. That is a false merge, and it would silently
understate the row count by half.

**The key has to be the position token, not the underlying:** `receipt_token_id` when
the registry join resolved, else `(chain_id, token_address)`.

### 3.3 That key covers 10/12 rows; the remaining two need explicit handling

- **Uniswap V4 (2 rows)** — `token_address` is a 66-char pool id, not an address
  (`token_address` lengths across the 12 rows: 42×10, 66×2). The entity docstring
  already warns about this. No address key is possible; these are reference-only rows.
- **Anchorage custody (1 row)** — the same real-world position is described
  differently by each side, so no automatic key can ever match it:

  | | indexed | reference |
  |---|---|---|
  | `chain_id` | **0** (off-chain sentinel) | **1** (ethereum) |
  | `symbol` | `BTC` | `ANCHORAGE` |
  | `receipt_token_address` | `null` | `0x49506c3aa0…` |

  It needs a named special case, or it shows up twice in every merged view.

### 3.4 The two sides have different scopes

This is the structural constraint on where the merge can live.

- **Indexed rows are per-proxy.** A prime allocates through one ALM proxy per chain,
  so its positions are spread across addresses; the UI fans out and concatenates
  (`lib/api.ts:145` `getAllocationsForProxies`).
- **Reference rows are per-prime.** Every proxy of a prime answers with the same
  prime-wide list — which is why `getAllocationsForProxies` deliberately calls only
  the *first* proxy in reference mode (`lib/api.ts:152`), and why the endpoint sets
  `scope="prime"`.

Measured consequence: querying spark's *mainnet* proxy matches 6 of 12 reference rows;
querying its *Base* proxy matches 0 of 12 — the reference list is mainnet-heavy and the
Base proxy holds 3 indexed rows. **A per-proxy merge is therefore meaningless.** The
merge is only well-defined against the prime's whole indexed fan-out.

Two ways to satisfy that:

- **(a) Client-side merge in `getAllocationsForProxies`** — it is the only place that
  already holds the full fan-out. Needs the API to start exposing the reference row's
  underlying address and a stable position key (§3.1).
- **(b) Server-side `both`** — `/primes/{id}/allocations?source=both` resolves the
  prime's proxy set (`list_prime_proxy_addresses` already exists, added in #729),
  unions and dedups server-side, returns rows tagged with provenance.

**Recommendation: (b).** It puts the dedup next to the data that makes it possible
(`loan_token_id` resolution, the registry join, the custody special case), keeps one
implementation for every consumer, and avoids widening the response purely to let a
client do the join. Cost: the endpoint stops being purely proxy-scoped in this mode,
which needs saying plainly in its description.

---

### 3.5 The better source — switch the reference side to it

`sky.data.blockanalitica.com/internal/allocations/?prime={star}` (catalogue §2.1)
answers every objection above:

| | Star monitor (today) | `/internal/allocations/` |
|---|---|---|
| rows (spark) | 12 | **59** |
| scope | prime-wide | **per-proxy** (`wallet_address`) |
| position token address | 10/12 | **59/59** (57 are 42-char) |
| unique key | — | **`(network, address, wallet_address)`, 59/59 distinct** |
| category vocabulary | — | **`allocation` / `asset` / `pol` / `psm3`** — STL's own |
| per-position assets | exposure only | `assets`, `allocated_assets`, `idle_assets`, `apy` |
| join vs STL indexed | 6/12 matched | **25/44 matched, 1 STL-only** |

Because it is per-proxy, the §3.4 scope problem disappears: the merge key becomes
`(chain_id, token address, proxy address)`, the same grain as `allocation_position`.
And `allocation_type` maps straight onto STL's `category`.

Still needs explicit handling, now a much shorter list:

- **Two Uniswap V4 rows** carry 66-char pool ids instead of addresses (2 of 59).
- **Anchorage** — `network=ethereum` + a token address upstream vs `chain_id=0` +
  null address in STL. A named special case either way.
- **`sparkUSDTbc` appears twice** on ethereum with different addresses: two Morpho v2
  vaults sharing a symbol. Pin this in a test — it is the case that punishes any
  symbol-keyed dedup.
- **15 rows on chains STL has no proxy for** — D2a.

**Cost:** this endpoint is on Host B, whose sign-off is still pending on #729. The
Star-monitor host stays in use for the risk-capital *totals*, which are richer there
(14 fields vs 5). So `both` would read from both hosts.

---

## 4. Where `reference` lives today

Complete inventory — 6 endpoints, 12 UI call sites.

### Python (`stl-verify/python`)

| File | What |
|---|---|
| `app/api/v1/allocations.py:661` | `list_allocations` — the list that needs merging |
| `app/api/v1/exposure.py:79` | exposure series |
| `app/api/v1/prime_debts.py:117` | debt series |
| `app/api/v1/prime_risk_capital.py:348` | risk-capital snapshot (see D4) |
| `app/api/v1/total_capital.py:127` | capital series + `assets_usd` / `encumbrance_ratio` |
| four `source:` response fields | listed in D3 |

`/v1/allocations/activity` has **no** reference mode at all (verified) — see §5.

### TypeScript (`stl-verify/ts/ui`)

| File | What |
|---|---|
| `src/lib/referenceMode.ts` | the whole module becomes `sourceMode.ts`; `REFERENCE_MODE` boolean → a 3-valued union |
| `src/router/search-params.ts:110-142` | `toReferenceFlag` / `referenceParam` → a `source` enum param on `sharedSearchSchema` |
| `src/lib/api.ts:134,152,169,189,440` | `referenceQuery` spread; `getAllocationsForProxies`' reference special-case |
| `src/components/shared/SettingsMenu.tsx:186-213` | radio group gains a third option (already uses `indexed`/`reference` as its values) |
| `src/App.tsx:611,619,857,866,876` | mode-conditional collateral/observed-at reads |
| `src/components/allocations/AllocationGrid.tsx:668,675` | mode-conditional debt reads |
| `src/hooks/usePrimeChartData.ts:94` | series fetch |
| `src/components/allocations/metricCards.tsx:39-48` | `MetricChartSpec` is **single-series** (`data`, `stroke`) — needs to become a series list for overlays |

**Back-compat:** keep accepting `reference=true` as a deprecated alias for
`source=reference` for one release, or break it now? It is consumed only by our own UI
and any hand-built links. **Recommendation: accept and ignore-with-deprecation for one
release** — it costs ~5 lines and the links exist in Slack scrollback.

---

## 5. What each metric means in `both`

The honest answer for a chart is **two lines, never one merged number** — the two
provenances describe overlapping-but-different position sets, so summing double-counts
and averaging is meaningless.

| Card | Endpoint | `both` behaviour |
|---|---|---|
| TOTAL ALLOCATION | `/allocations/activity` | No reference *series* exists (upstream `/primes/{star}/events/` is an event feed, and on-chain data must come from chain per `AGENTS.md`). Stays indexed; label it so it does not read as merged |
| EXPOSURE | `/exposure` | Two series overlaid |
| TOTAL RISK CAPITAL | `/risk-capital` + `/total-capital` | Two series; scalars side by side (D4) |
| PRIME COLLATERAL | `/total-capital` `assets_usd` | **Reference-only field.** Renders in `both` from the reference half, marked as such |
| ENCUMBRANCE | `/total-capital` `encumbrance_ratio` | Same — reference-only |
| PRIME DEBT | `/debt` | Two series overlaid |
| RISK CAPITAL column | `/risk-capital` per-allocation | Per row, from whichever provenance produced it; both shown on a merged row |

Two of six cards have no indexed counterpart and one has no reference counterpart, so
`both` is genuinely partial. That needs to be visible in the UI, not implied.

---

## 6. Work breakdown

Sized in PRs, each independently reviewable.

**PR 0 — nullable `chain_id` + `network`, and stop dropping unmapped rows.** ✅ **done**
(uncommitted, in this working tree). Carried D2a and the catalogue §6 fix together,
since they were one change: rows on chains STL has no id for became representable
instead of failing the list.

Measured before/after, same staging database:

| | `main` | this branch |
|---|---|---|
| grove reference allocations | **HTTP 502**, 0 rows | **HTTP 200**, 15 rows |
| — of which unmapped | — | 2 (`ACRDX`/plume, `groveUSDG`/robinhood) |
| spark reference allocations | 200, 12 rows | 200, 12 rows (unchanged) |

What it touched beyond the response model: `getChainLabel` falls back to the upstream
network name (title-cased); a new `allocationNetworkKey` keys the network filter and
`getAllocationKey` so two unindexed chains cannot collapse into one; `TokenAddress`
stops defaulting `chainId` to 1, which would have linked a Plume address to Etherscan;
`RiskBreakdownTab` and `ActivityFeed` skip lookups they cannot scope rather than
issuing a request that would answer about a different chain.

Gated by a new `test:unindexed-chains` check in `ts-ci`, pinning the collapse bug, the
label fallback, the three states of `chain_id` (id / `0` off-chain / `null` unknown),
the suppressed explorer link, and the agreement between the filter's option values and
the key its predicate computes. Verified falsifiable: reverting `allocationNetworkKey`
fails it.

**What the review pass caught** — three defects the gates did not:

- **`BottomPanel` wrote the literal `"null"`** into the `network` search param for an
  unmapped row, and `parseNetworkChainId` read `Number('null')` → `NaN` → *no chain
  filter*, so "view in activities" answered with every chain's flows for that symbol.
  Exactly the bleed the drawer guard prevents, through a path that bypassed it. The
  same weakness made the page-mode filter leaky once the dropdown could emit
  `net:plume`, so an unparseable value now suppresses the fetch instead of widening it.
- **A guard that could never run.** The null-chain check added to `RiskBreakdownTab`
  sat behind an existing `receiptTokenId === null` guard, and `receipt_token_id` is
  structurally null for these rows (the service skips the registry join when
  `chain_id is None`). Folded into the guard that does run, which also supplies the
  narrowing.
- **Three empty states named a false cause** — "no events match your filters", "direct
  asset holdings have no risk model". A reader would conclude the position was inert
  rather than unindexed. One shared `unindexedChainMessage` now states it: *"STL does
  not index Plume yet, so activity is unavailable for this position."* Worth noting the
  visible one was not where it looked: `ActivityResults` passes `isEmpty={false}`, so
  `AsyncStateRenderer`'s `emptyView` is unreachable and the rendered state is
  `ActivityTable`'s own.

Also from the review: a dead `isResolvableChain` export, a `network: None` that landed
in a `params=` dict rather than an expected row, the `TokenAddress` default that
contradicted its own doc, and the comment ratio (19% → 14.7%, of which ~19 lines are
exported-API docs the guideline keeps; inline comments are ~7.5%).

**One correction to the review, worth recording:** it proposed sorting network options
by label, which reorders the *indexed* chains for every prime — mainnet stops leading.
Options now sort indexed-chains-first in chain-id order, then unmapped by name, so a
prime with nothing unmapped is byte-identical to `main`. Verified: Ethereum Mainnet →
Base → Avalanche C-Chain → Plume → Robinhood.

**PR A — rename the param, no behaviour change.** ✅ **done** (uncommitted).
`source=indexed|reference|both` across the five endpoints that take it,
`reference=true|false` kept as a deprecated alias, response vocabulary aligned (D3),
contracts regenerated, UI switched over.

Verified against the live API, uniform across all five endpoints:

| request | result |
|---|---|
| *(absent)* | `200 source=indexed` — today's behaviour, unchanged |
| `source=indexed` / `source=reference` | `200` with that provenance |
| `source=both` | `422 source=both is not available here; this resource serves: indexed, reference` |
| `reference=true` / `reference=false` | `200 source=reference` / `source=indexed` |
| `source=indexed&reference=true` | `422 … conflicts with the deprecated reference=true` |
| `source=sky` | `422 Input should be 'indexed', 'reference' or 'both'` |

`both` is deliberately refused for now and the default stays `indexed`: the merged
answer does not exist until PR B, and a default that 422s would be worse than a
default that works. **PR B flips both** — adds `BOTH` to each endpoint's `available`
set and changes `default=Provenance.INDEXED` to `BOTH` at the five call sites.

**The enum is `Provenance`, not `DataSource`.** `DataSource` was already taken: the
UI binds it to `components['schemas']['DataSourceResponse']`, a `/v1/data-sources`
registry row describing which upstream feeds STL reads. Two unrelated things named
"data source" in one OpenAPI namespace would have read as singular-and-plural of each
other. Caught when the TS import collided.

**The superseded param is translated on entry, not stripped.** `?reference=true`
rewrites to `?source=reference` in the address bar, following the `withoutLegacyPrime`
precedent already in `router/routes.ts`. Without it the param was dropped while
`lib/provenance` had already read it — a URL that disagreed with the page it produced,
and a shared link that silently changed meaning when copied back out. `?reference=false`
becomes `?source=indexed` rather than an absent param, so it cannot inherit a default
that is about to move.

Also: the UI now **always states the provenance** rather than omitting it for the
default, so the page does not change what it shows the moment the API's default moves.

Gated by the existing `test:routes` check, updated to pin the translation in both
directions and to assert an unknown provenance is dropped rather than carried.

**PR B — `both` for allocations.** ✅ **done.** The dedup: prime-wide proxy resolution, position
key with the V4 and custody cases handled (§3.3), provenance tags on rows, degradation
(D5). This is where the risk is, and it wants integration tests seeded with the exact
shapes in §3.3.

**PR B2 — source availability and the silent redirect (D5).** ✅ **done.** `available_sources` per
prime on `/v1/primes`, disabled menu options, the entry-time rewrite and its loop
guard. Depends on PR A for the vocabulary; independent of B.

**PR C — `both` for the metrics.** ✅ **done.** `MetricChartSpec` multi-series, overlay rendering,
legend, the "no counterpart" labelling from §5.

**PR D — per-allocation breakdown (D4).** ✅ **done.** The reconciliation table in catalogue §4,
served per position, with the `rrc_i / total_rc` encumbrance attribution. Independent
of A–C; could ship before or after.

Rough sizing: PR 0 a day (model change touches more than it looks), A a day, B the real
work (2–3 days with tests), B2 a day, C a day plus design iteration on the overlay, D a
day. PR 0 and PR A are done.

---

## 6a. What the build turned up

Findings worth carrying forward, each measured rather than inferred.

**A single identity key was not enough.** The provenances do not carry the same
*kind* of identifier. Sky's allocation rows have a token address and often no
registry id; STL's risk-capital breakdown rows have the registry id and no
address. An address-first key sent the same position to two buckets, and the
breakdown merged **zero of 25** positions. Identity now offers candidate keys and
a match on any one counts — 6 positions pair up, and the allocation union is
unchanged (grove 16+14−8=22, spark 28+11−8=31, no duplicates).

**A union of rows is not a sum of rows.** Sky and STL sometimes describe the same
money differently — a position in an Arkis vault against the plain USDC beneath
it — and those rows do not match on identity, so adding them counts it twice.
Totals therefore cover STL's rows only, with the count naming what is left out.
The encumbrance column follows the same rule and reconciles to +0.005% even in
merged mode; including Sky-only rows stopped it summing to anything.

**The two models disagree, and that is the point.** For spark: total risk capital
agrees to the cent, exposure differs by STL's chain coverage ($1.77bn vs
$2.16bn), and encumbrance reads 100.6% against Sky's 39.3%. STL's model is known
to be immature, so parity is not expected — which is the argument for keeping
every figure in its own field rather than reconciling any of them.

**Merged mode is slow.** It fans out across a prime's proxies *and* calls
upstream, so it 500s under the dashboard's concurrent load against the staging
database over a tunnel; sequentially the same calls return in 6–13s. Read
alongside §7.0 — this is the same query cost, now multiplied.

---

## 7. Risks and open questions

0. **The prime-wide activity query may be too heavy for real data.** Reading the
   staging database directly, `/v1/allocations/activity` takes **~5s** per call for
   either prime, and under the dashboard's concurrent page load Postgres returns
   `OutOfMemoryError: out of memory` (110 occurrences in one session). PR 0 does not
   touch that query — #729 widened it from one proxy to the prime's whole proxy set,
   which is the obvious suspect. Measured over an SSH tunnel from a laptop against a
   shared read-only pooler, so not representative of in-cluster performance, and
   single requests always succeed. Worth an `EXPLAIN` against the widened predicate
   before the `both` default multiplies the traffic.

1. **`both` puts a third-party feed on the default path** (D1, decided). Both hosts
   are unauthenticated and carry no SLA, and `both` reads from *both* of them (§3.5).
   Worth deciding whether the reference half gets a short server-side cache so a page
   load does not fan out to two external hosts per prime.
2. **There is already a duplicate-key bug in the indexed list.** The allocations view
   logs `Encountered two children with the same key: direct:1:3` on every render —
   two direct holdings on chain 1 sharing an `underlying_token_id`. `getAllocationKey`
   even documents that it "gives the copies identical keys, so nothing downstream would
   catch it". Worth fixing inside PR B, where row identity is the subject anyway;
   dedup across provenances cannot be trusted while identity within one is ambiguous.

3. **A false merge is worse than a missed one.** Merging two positions understates the
   row count and sums two exposures into one row. The §3.2 numbers show the obvious
   key does exactly this. Tests should assert the *non*-merges (spUSDT vs sparkUSDTbc
   vs UNI-V4-USDT-USDS stay three rows).
4. **`AGENTS.md` off-chain-feed rule.** `both` as a default materially widens where
   the Sky feed is read. PR #729 already carries a pending Host B sign-off for this
   feed; making it a default probably needs that resolved first.
5. **Provenance mixing within a page.** `referenceMode.ts` reads the flag once per
   session on purpose, so a cached series and its refresh cannot disagree. A 3-valued
   mode keeps that property; a mid-session client-side switch would break it.
6. **The two hosts disagree on the same figures** — spark exposure 2.1476bn (BA) vs
   2.1238bn (SM), ~1.1%; encumbrance 0.4042 vs 0.4010. Separately computed live
   snapshots, so a merged row must not present them as one number, and whichever host
   a figure comes from has to be recorded with it. Per-row indexed-vs-reference
   disagreement is still unmeasured; worth doing before PR B, as it may justify a
   "disagreement" affordance.

---

## 8. How I would verify it

- Unit tests per endpoint for all three values plus the deprecated alias, and the 422
  for `source=both` on `/risk-capital`.
- Integration tests seeded with the four hard shapes from §3: a clean match, a V4 pool
  id, the Anchorage chain-0/chain-1 pair, and a shared-underlying trio that must
  **not** merge.
- A degradation test: reference unavailable for a prime → `available_sources` omits it,
  the redirect lands on `indexed`, and it happens once (no loop).
- A D2a test: an upstream row on `plume` renders with `chain_id: null` and
  `network: "plume"` rather than failing the list, and does not collide in
  `getAllocationKey` with a same-symbol row on `robinhood`.
- `make ci` + `make test-integration`; `ts-ci` including `npm run doctor` and
  `test:metrics-grid`.
- Manual: staging Vite server against `spark` and `grove` in all three modes. Post-PR 0
  both should work, so the D5 path needs a prime that is genuinely untracked upstream —
  `nova` returns 0 rows and 404/500 on the detail endpoints (catalogue §6) and is the
  natural fixture.
