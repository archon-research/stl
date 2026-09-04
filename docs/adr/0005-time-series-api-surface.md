# ADR-0005: Time-Series Data API Surface

**Status**: Proposed
**Proposed**: @andrius-senulis
**Date**: 2026-08-28
**Deciders**: @vector

## Context

The STL Time-Series Data API PRD asks for a coherent, deliberately designed surface exposing
STL's time series to programmatic consumers — API clients, AI agents, and a UI — rather than
an accretion of one-off endpoints. Its first-release datasets are prices, position exposures
and capital held — the last of which is a phrase rather than a dataset, and resolves below. Its stated non-functional priorities are consistency across the whole
surface, query patterns that do not overload the database under fan-out, and a pattern
extensible to the full set of STL time series over time.

Two things about the situation shape everything below.

**This is not greenfield.** A shared time-series query policy already exists and is in
production use across several routers: `app/domain/time_series.py` holds the policy and
`app/api/time_series.py` the HTTP adapter, with UTC enforced as a type invariant, a
window-to-minimum-frequency floor, window ceilings, and a cache policy. `time_bucket_gapfill`
with `locf(last(...))` in the Postgres adapters is already the PRD's **end-period** method with
forward fill. The remaining resampling work is exposing the method as a parameter and adding
the other three methods — not building a subsystem.

**The shipped surface is inconsistent in ways that are structural, not cosmetic.** A
conformance audit of the V1 surface against the convention the repo itself established found
the same computation reachable by several addressing schemes across different HTTP surfaces;
one path-parameter annotation carrying two different token meanings; prime-scoped routes keyed
on an ALM proxy address while a newer route keys on the prime name; and time-series routes that
accept a parameter and never read it. These are the failures a "consistent conventions"
requirement has to prevent, and they recur unless the structure prevents them.

Three further facts constrain the design.

*There are no live consumers yet.* Changing existing contracts is cheap today and will not be
cheap once GovOps or any other consumer integrates. Breaking-change work is therefore
front-loaded deliberately, and the whole V1 surface is brought onto one pattern rather than
only the three new datasets — otherwise "consistency" is an empty claim about a surface where
most endpoints predate the convention.

*The meaning of a timestamp was decided per table at ingest.* Two meanings exist in the schema:
event time (when the fact happened) and observation time (when we looked). An audit of the six
feeds backing the three first-release datasets found that only one — Sky's
Star-monitoring risk-capital payload behind `prime_capital_stack` — publishes no event time in
any form. One feed (`prime_debt`) stores poll time while already storing the block number
needed to resolve a real one. Two feeds are event-time but documented as observation-time:
`offchain_token_price.timestamp` carries CoinGecko's own `last_updated_at`, and
`allocation_position.created_at` carries a block timestamp. Taking the column comments at face
value would have had us declare prices an observation-time series — wrong, and wrong about the
flagship dataset.

That audit had a hole of its own, found later and recorded in decision 3: reading a column comment
establishes what a value *is* and not where it *comes from*. `allocation_position.created_at` does
hold a block timestamp, but of the block a periodic sweep sampled at rather than of the block the
value changed — which only the write path reveals.

*No on-chain value is a durable prime identity.* Verified by `eth_call` against mainnet and
Sky's chainlog: the vault we call "grove" reports an ilk of `ALLOCATOR-BLOOM-A`; a separate,
live `ALLOCATOR-GROVE-A` vault exists that appears nowhere in our data; and the `-A` suffix is a
version slot in Maker's ilk convention, so more than one vault per prime is anticipated by
design. Not the name, not the ilk, not the vault address.

This ADR records the decisions that are architecturally significant — hard to reverse,
structural, or certain to be questioned later. The conventions that follow from them
(parameter names, point encoding, decimal serialization, cache policy, empty-result semantics,
the `/latest`–`/asof` split) are real decisions but do not shape the structure; they belong in
the endpoint reference circulated alongside this document.

## Decision

### 1. Identity: prime-grained results, names over addresses, one internal identifier

Prime-scoped responses are always whole-prime. The API never returns data scoped to a single
proxy and offers no proxy filter. A prime is a set of wallets — a vault, one ALM proxy per chain
it operates on, and a SubProxy treasury — and serving partial totals is what makes a caller able
to receive a number that is wrong and looks right. Removing the ability to ask for one removes
the failure at the source.

The preferred external identifier is the prime's name, in the path. The API additionally accepts
the vault address and any proxy address and resolves all of them deterministically to the same
prime, silently, with no redirect. Resolution is safe because `prime.name` and
`prime.vault_address` both carry UNIQUE constraints, every address in the `axis_synome` contract
is distinct, and names and addresses cannot collide with each other by format.

`prime.id` is the canonical internal identifier and is stable across renames and vault
migrations; the name and the vault address are time-varying attributes of it. It never leaves
the API, so its environment-specific numbering is not a problem.

Prime names are permanent. A rebrand adds a name; it never frees the old one for another entity.
Because the name sits in the URL, reuse would mean a URL silently returning a different entity's
figures while still answering 200 — the worst failure shape available. This is enforced by
policy plus a CI assertion that no name ever maps to a different `prime.id` than before, not by
machinery: the failure needs two events, a rename and then a reuse, and both go through review.

Prime-scoped responses echo the prime's *current* name and carry `prime_key`, an opaque
identifier that never changes. The echoed name matches what the entity is called; `prime_key` is
the durable handle a client keys on. Retired names stay listed as aliases in the catalogue.
`prime_key` is a field, never a URL segment.

The key sits on the envelope rather than on every row because no response spans entities: the
unscoped route for a dataset returns the list of identifiers that have data, not the data itself
(decision 2), so every response carrying points is already scoped to one entity and the envelope
is an unambiguous place to name it. This was decided the other way for a time, on the assumption
that unscoped history returned rows for several primes — an assumption the shape no longer
supports.

Where two upstreams report the same quantity for the same entity, they are two series
discriminated by source — not one series with duplicate points. A response never contains two
points at the same timestamp. Two points at one timestamp is not a time series; it means two
series were conflated. Source is therefore part of series identity, which the licensing gate
needs anyway: the same token has a CoinGecko price and an on-chain price, and only one of them
is redistributable. If a single merged series across feeds is ever wanted, the merge rule is a
declared, deterministic source priority — never `last()` with arbitrary ordering among
same-timestamp rows.

The rule has one instance on the shipped surface and it is prices: several oracles publish a
price for the same token on the same chain, and they are several series. The discriminator is part
of the identifier — the feed is a path segment, so a request resolves to exactly one series and
there is no way to spell a request that returns two. It is deliberately not a parameter with a
default. A parameter would make the feed a *selector* over one series rather than a name for one of
several, and a default would mean re-pointing it silently changes what an unchanged URL returns,
which is the failure shape the name-permanence rule above exists to prevent.

A second upstream was considered and is not served: a third-party reference feed carried beside
our own figures on several routes. It was withdrawn from the series surface for a reason that is a
property of the feed rather than a preference — the payload behind most of it publishes no
timestamp of any kind, so it could not be served on any axis but our own polling clock. It remains
where it already was, on the snapshot routes, where no axis is claimed. The licensing example
resolves the same way: on the first-release surface prices are on-chain only, so the
redistributable and non-redistributable series of one token are two catalogue entries of which we
list one.

### 2. Dataset-oriented paths, uniform in shape

Each dataset gets its own path family rather than one generic series endpoint serving
everything. A single endpoint for every dataset would have to return the narrowest common
response, and the datasets genuinely differ: prices carry currency and source, exposures one USD
figure, capital-held components. Typed per-dataset responses serve the PRD's first-named
consumer better than a lowest common denominator.

Every family carries the same shapes: the dataset route, history for one entity, and one
single-observation route. The dataset route lists the identifiers that have data rather than
returning the data across every entity — a cross-entity history response answers no stated
requirement, and the max-points rule in decision 5 would reject most of the cases where it would
have been interesting. The consequence that matters elsewhere: no response carrying points ever
spans *keyed* entities, which is what puts `prime_key` on the envelope in decision 1.

One dataset keeps a bare data route, and the exception is principled rather than tolerated: the
protocol-event feed has no entity a client keys on, and forcing one into the path would make "what
happened across all protocols" unaskable — a caller would have to enumerate and fan out, which is
the pattern the catalogue exists to prevent. Its unscoped route therefore returns history, bound by
the window ceilings and the rejection rule in decision 5 rather than by the pagination that governs
identifier lists, and it has no single-observation route because there is nothing to be latest
*for*. Where a dataset's filter was standing in for an entity a client actually keys on, that
filter becomes the identifier instead.

The single-observation route takes an optional upper bound and returns the newest observation at
or before it, defaulting to now. An earlier revision split that into two routes — one with no
argument, one with a required one — on the ground that an optional bound can be forgotten, and
forgetting it returns *now* silently. That risk is real and is accepted for a first release: with
no live consumers there is no usage evidence to justify a second route per dataset, and the
envelope echoes the window that answered, so a caller who omitted the bound can see it.

The path shape, parameter names and response envelope are uniform across every dataset. This is
the load-bearing half: without it, dataset-oriented paths are just the status quo that produced
the inconsistencies in the Context. Uniformity is not self-enforcing today — the only mechanism
is code review, which is what produced them — so it needs a conformance test over the generated
OpenAPI schema asserting that every route carries the envelope, the agreed parameter names and
the declared error shapes. Those error shapes have to be declared somewhere before the assertion
means anything, which is what the rejection contract in decision 5 does.

Where "consistent with the current API format" conflicts with bringing the whole surface onto
one pattern, the consistent surface wins.

### 3. Event time is the required axis; observation time is a labelled exception

Every series the API serves is served on event time wherever the event time exists or can be
recovered from the upstream payload. A feed without one is a gap to close at ingest, not a
permanent second class.

Observation time is not a supported second axis we design around. It survives only where the
upstream publishes no event time in any form, is explicitly labelled as such in the catalogue,
and carries a tracked gap. For such a series, declared cadence is required rather than optional,
because cadence is what bounds the staleness of a polled value; without it the caller cannot
size the error.

Event time and observation time are never silently interleaved. Every series declares its axis,
and a single response never mixes the two. This is the correctness rule the whole axis policy
exists for: ask two series for "the value on 21 August" and an event-time series answers with
the value at that moment while an observation-time series answers with whatever the last poll
happened to see. Divide one by the other and the error is unbounded and invisible.

The strictness is deliberate. A tolerated fallback becomes the default, and a surface with two
first-class time axes cannot honestly promise that its series are comparable.

**One first-release series invokes the exception, and it is worth being precise about why.** The
feed the audit identified as the only one publishing no event time in any form is not served as a
series at all, once the reference provenance comes off this surface. The exception is claimed
instead by a series the audit misread: the on-chain treasury balance behind Total risk capital is
written by sampling `balanceOf` at a fixed block interval, so its timestamp is the block we looked
at rather than the block the balance moved. The audit classified that column as event time on the
strength of it being a block timestamp, which is true and is not the same thing.

That series ships on the sample timestamp, declared `observation` with its sampling interval as
the required cadence — per chain, since the interval is configured per tracker and block times
differ. What makes this tolerable where a polled third-party feed would not be:
`balanceOf` at a block is *exactly true* at that block, so no point is stale against its own
timestamp — what is lost is resolution, not accuracy, and the loss is bounded and expressed in
chain time. The never-interleave rule still binds, so the catalogue declares the axis and the
series is not comparable with an event-time one in a single response. The event-time path — index
the token's transfer events for the wallets in question, keeping the periodic sample as a
reconciliation guardrail — is being costed in parallel, and if it lands it lands before the
endpoint is built rather than as a migration after it.

### 4. The latest version only; the version axis is not exposed

The API serves the latest processing version of every point. Callers cannot request an earlier
one, and the version axis appears nowhere in the request grammar or the response.

Two grounds. The product requirement is that the latest version is always the right answer for
this API. And the mechanism does not carry what exposing it would appear to offer:
`processing_version` is in practice a reprocessing log rather than a correction log, so a caller
walking it would see mostly byte-identical duplicates. Anything useful — distinct value changes
— would have to be derived, and nothing derives it.

Reproducibility of a past answer remains a real requirement; it is served by the append-only
guarantees in ADR-0002 and ADR-0006 against the database, not by this API.

### 5. History is never paginated; oversized requests are rejected

Resampled responses are bounded by construction, because the frequency floor scales with the
window. Default-frequency responses are not bounded by anything today — the floor constrains
bucketing, not row counts — and a single well-formed request against a dense price series can
ask for millions of points. Those responses are bounded by a max-points rule that returns 422
naming the actual count and a window that would fit.

Rejecting, not truncating and not paginating.

Rejections are structured. The 422 body carries a stable error code, the actual point count, the
max-points limit, and a suggested `from_timestamp`/`to_timestamp` — or a `frequency` — that would
fit, as fields, with the human-readable message alongside. Because rejection replaces the
truncation flag, this body is the only signal a caller gets that a request was too big, so it has
to be machine-readable rather than prose: a client re-tiles the window or drops to a fitting
frequency without parsing an error message. One model serves every 422 on the surface, not only
this one.

The rule governs history, not every list, and the split across the surface is one line:
**discovery lists paginate, series never do.** The catalogue and the per-dataset identifier lists
page by keyset — a list of what exists is a list that grows, and a torn read of one is harmless
next to a torn read of a series. Only the small fixed lists stay bounded-and-truncated instead,
under one ceiling unified across the surface and with the truncation reported explicitly; today
their ceilings disagree and none of them reports anything.

Truncation with a completeness flag is only marginally better than silent truncation: a
statistic computed over a silently shortened series is wrong and looks right, and a flag is easy
to ignore — the failure lands on the consumer the PRD names first.

Pagination introduces a failure that has no cheap fix: a correction landing between page one and
page five yields an assembled series that never existed, with every individual page valid.
Detecting that requires read-consistency-token machinery worth more than the feature. Rejection
keeps one request equal to one query against one snapshot, which is what makes any per-response
data-version marker mean what it appears to mean.

The rule ships with default-frequency history, not after it. The bounded path is the resampled
one and it arrives late; the unbounded path arrives first. Rejection is made workable rather
than hostile by the catalogue (decision 6), whose first and last observation timestamps and
declared cadence let a caller size a request before issuing it.

### 6. A catalogue endpoint fronts the data endpoints

`GET /v1/series` lists the series the API can serve, so a client discovers identifiers instead
of guessing them. Each descriptor carries an opaque internal series id, the alias identifiers
the series answers to, its dataset, unit, source and licence, its series kind and time axis, the
entity key, and its first and last observation timestamps.

The entity key is the part the rest of the descriptor does not cover. Kind, axis and unit say what
a point *means*; the entity key says what it is *of* — the response fields that identify the thing
a series is about, which is `prime_key` for a prime-scoped series, chain, token address and feed
for a price series, and empty for the datasets that have no single entity. Without it a client keying
rows into a store infers identity per dataset from prose, which is the guessing this endpoint
exists to remove.

The internal series id is deliberately opaque and not a parsable naming scheme, so it can be
re-pointed without becoming a contract. It stays a catalogue key and never becomes addressable:
descriptors carry the canonical URLs for the series instead, so a client follows a link rather
than constructing a path. Building a second route family reaching the same data is exactly the
pattern the Context describes.

The listing is scoped to what the caller may actually fetch. A series behind an entitlement the
caller does not hold is absent from the catalogue rather than listed and unfetchable, and a direct
request for it is answered the same way a request for a series that does not exist is answered.
This costs a real affordance — a caller can no longer always distinguish a mistyped identifier
from a series that exists but is not theirs — and that is what hiding means; an answer that
distinguished them would disclose the thing being hidden. It is decided here rather than later
because it is not reversible in this direction: a listing that was complete cannot start omitting
rows without silently changing what an earlier response meant.

This is the indirection layer that makes the eventual move to security-identifier addressing
non-breaking, which the PRD requires: when SECstore lands, the descriptor gains an alias and no
data URL changes.

It also does load-bearing work for the rest of this ADR. The first/last pair answers "is there
data for this series in this window" in one call, rather than the caller firing requests to find
out — the PRD's fan-out concern addressed at the catalogue layer instead of by adding batch
variants to every endpoint. The declared axis is where decision 3 becomes visible to a caller.
The declared kind is what makes decision 7 interpretable. And the timestamps plus cadence are
what make decision 5's rejection actionable.

### 7. Gap policy follows series kind, and filled points are marked

In bucketed output, an empty bucket is filled according to what kind of quantity the series
carries, and the kind is declared per series in the catalogue.

| Series kind | Empty bucket | Reasoning |
|---|---|---|
| **Level** — price, exposure, total risk capital, debt | Carry the last observed value forward, mark the point as filled | The value persists between observations. If no trade happened in a minute the price did not become unknown. |
| **Flow** — event counts, volumes | Emit zero | No events in a period means none occurred. Forward-filling a count is wrong; omitting the row forces the caller to guess between "none" and "no data". |
| **Either, before the first observation** | `null` | Nothing exists to carry forward. |

Marking filled points is the load-bearing part. A mean computed over forward-filled data is
biased toward stale values and looks entirely plausible, and arithmetic on returned series is
the first-named use in the PRD. The flag is present only on filled points; its absence means
observed.

The surface today already forward-fills level series and omits rows for count series. Those are
two correct behaviours for two kinds of quantity. The defect is that the behaviour is
**undeclared**, not that it differs — so the fix is the catalogue declaration plus the filled
marker, not making the two paths agree.

This also dissolves the frequency-versus-cadence problem. Requesting an hourly frequency on a
daily-polled series returns twenty-three filled points and one observed point per day: honest,
self-describing, and visible to the caller. No validation of requested frequency against
declared cadence is needed, and no rejection rule — which is the better outcome, because
rejecting would force a caller to know a series' cadence before querying rather than discovering
it in the response.

## Alternatives Considered

**A single generic series endpoint** (`/v1/series/{series_id}/latest`) instead of
dataset-oriented paths. Attractive for agents: one contract to learn, and new series need no new
routes. Set aside because one endpoint serving every dataset must return the narrowest common
response, and prices, exposures and capital-held carry genuinely different fields. Reachable
later as an additional way in if a consumer wants it.

**Structured, parsable series identifiers** — `price.usd.spot:eth:0xA0b8…`. Guessable and
self-documenting. Set aside because it means inventing and then owning a naming standard, which
ossifies the moment anyone hardcodes one. Opaque ids with a catalogue lookup were preferred.

**Making `series_id` addressable.** A client holding a `series_id` would otherwise have to
construct a dataset path from it. Solved more cheaply by returning the canonical URLs in the
descriptor, which avoids putting a second way to reach the same data on the surface.

**An all-time data-point count in the series descriptor**, so callers could infer update
frequency for irregular series. Set aside on two independent grounds. It is expensive: an exact
`COUNT(*)` per series scans compressed hypertables with no continuous aggregate anywhere, and a
catalogue listing multiplies that by the number of series returned — the endpoint added to
reduce database load would have become the heaviest in the API. And it is ambiguous: under a
versioned schema the number could mean rows written or distinct observation timestamps, and
those diverge exactly where corrections happen, which is where a caller would most want to trust
it. Replaced by declared cadence plus bounded recent-observation counts, which answer the actual
question and can distinguish "updates hourly" from "updated eight thousand times three years
ago, silent since".

**Exposing the version axis.** Callers would have been able to request an earlier
`processing_version`, making any past answer exactly reproducible — the alternative a reader
would most expect us to have taken, given ADR-0002. Declined per decision 4.

**Paginating history.** Would have let a caller walk an arbitrarily long default-frequency
series in pages. Set aside per decision 5: a paginated series can tear, and detecting that costs
more than the feature. Note that `GET /v1/series` *is* paginated, by keyset on the series id — a
catalogue is a list that grows, and a torn read of a catalogue is harmless next to one of a
series.

**Truncating history with a completeness flag.** Cheaper than rejecting and superficially
friendlier. Set aside per decision 5.

**Serving a second provenance on the series surface.** The shipped API carries a third-party
reference feed beside our own figures on three time-series routes, selectable by a parameter.
Keeping it would have preserved a fallback that has a real basis: we price only the chains we
index, so some position rows carry a balance and no USD value, and upstream's figure is something
to fall back to. Set aside because the fallback argument was made about a snapshot route and does
not carry to a series, because the feed behind most of the reference half publishes no timestamp
and so cannot be a series at all, and because nothing on record asked for the rest — the PRD never
mentions reference data. A parameter kept for no stated consumer is one every future dataset has
to answer for. The fallback stays available where it was raised, on the snapshot routes.

**Observation time as a normal, supported axis.** Would have let each feed keep whatever
timestamp it happens to store, with the API declaring which. Set aside per decision 3.

## Consequences

**Cross-proxy aggregation becomes required work, and it is the largest item.** Whole-prime
results (decision 1) mean the additive-versus-shared dedupe rule currently living in prose that
tells clients to "sum them across a prime's proxies — dedupe" has to move into one tested place
in the API. In exchange, the two-scope split that today forces proxy-scoped and prime-scoped
fields to coexist in one response model stops being expressible, which makes the deprecated
unprefixed fields removable rather than merely discouraged.

**`prime_key` has to be minted before anything ships**, and must be stable from first
assignment — its whole purpose is to be the one identifier that never changes. Nothing generates
one today.

**One consequence of multi-form addressing must be documented prominently.** Passing a proxy
address returns the *whole* prime, including chains that proxy has nothing to do with. Query
grove's Avalanche proxy and the response covers mainnet and Base too. This is correct under
decision 1 and surprising without the documentation.

**The `prime` row now holds two mutable facts.** If `prime.id` is stable across a vault
migration, then `vault_address` must be able to change, and `name` already can. That leaves `id`
and `created_at` as the only genuinely immutable columns, which collides with the repository's
rule that in-place mutable columns are reserved for genuinely immutable identity facts. The
precedent for the fix already exists in this codebase — `maple_pool` moved its mutable
attributes into an SCD2 meta table with a `*_current` view — but no rename or migration is in
flight, so the proportionate step is the CI assertion from decision 1, which also produces the
audit record the schema currently lacks. The SCD2 migration is written when a real case arrives
to shape it. Either way this is invisible to the API: the boundary resolves alias to prime, and
whether the alias set is a column or a history table does not reach it.

**Decision 3 obliges ingest changes, not API changes.** A block timestamp must be resolved for
`prime_debt` at ingest — the block number is already on the row, and many other tables already
persist a `block_timestamp`, so this is established practice. The backfill cost for historical
rows is unmeasured. Two column comments must be corrected in the same pass, since taking them at
face value mislabels the flagship dataset. And `allocation_position.created_at` should lose its
`DEFAULT NOW()` and become `NOT NULL` without a default: the column is correct today only
because every writer supplies the block timestamp, and a future writer that omits it would
silently insert ingest time into an event-time series with nothing failing.

**Exactly one first-release series is an observation-time exception, and the audit that was
supposed to find it looked at the wrong thing.** The candidate everyone expected — the
untimestamped third-party payload behind `prime_capital_stack` — is not served as a series at all,
since it is a reference feed and reference provenance stays on the snapshot routes. The exception
is claimed instead by the dataset the PRD called "capital held": the USDS balance in the prime's
SubProxy treasury, read from `allocation_position`. The audit classified that table's timestamp as
event time because it holds a block timestamp. It does — of the block a periodic `balanceOf` sweep
sampled at, not of the block where the balance changed, and the sweep is the only writer the table
has.

Two things follow beyond the one dataset. The finding is a property of the **table**, so every
dataset reading it inherits it — exposure included, which is therefore sampled too. Exposure does
not get the same remedy, though, and the difference is worth stating because it looks like an
oversight otherwise: the treasury balance is a non-rebasing token whose balance moves only on
transfer, so its change points are indexable, while exposure is a *valuation* of yield-bearing and
pooled positions that revalue with no event touching the holder at all — rebasing tokens accrue
continuously, vault shares move with an exchange rate, and pooled positions move on a third party's
trade. The periodic sweep exists precisely to catch those transfer-less changes. Sampling is
therefore the honest representation of exposure rather than a shortcut, and it is declared
permanent rather than pending.

And the audit's method needs the correction on record: reading a column comment established what
the value *is* and not where it *comes from*, and only the write path answers the second question.

**One series outside that audit fails decision 3 without qualifying for its exception, and it is
now the only observation-time series on the surface.** The audit covered the six feeds behind the
three first-release datasets, so it did not reach `protocol_event`, which backs
`/v1/protocol-events`. That route windows and buckets on
`created_at`, a `DEFAULT NOW()` ingest column, so it is on observation time as built. The
exception does not apply: its upstream is the chain, which timestamps every block, so the event
time exists and this is a gap to close at ingest. It is `prime_debt`'s defect one step worse —
`prime_debt` already stores `block_number` and can resolve a real timestamp at ingest, whereas
`protocol_event` has no `block_timestamp` column at all, so closing it needs a column, a writer
change across every indexer that writes the table, and a backfill. Until then the catalogue
declares the axis `observation` with a tracked gap and a required cadence, and the
never-interleave rule means the series is not comparable with the event-time datasets.

**Withdrawing the second provenance removes response fields, not just a parameter.** The
series routes that carry a reference half today carry it as parallel fields in one response model
— a figure and its `reference_`-prefixed twin — plus the splice that fills them. Those come off
with the parameter, in the same change rather than after it, so that no caller ever integrates
against a selector this surface no longer offers. The envelope keeps a source field reading a
single constant: a response that has been logged, cached or quoted out of context should still say
which provenance answered, and that argument does not weaken when there is one answer. What must
not drift is the snapshot routes, which keep the second provenance and must keep behaving exactly
as they do — the risk in this change is scope creep into them, not the removal itself.

**The dataset the PRD named "capital held" could not be specified under that name.** The phrase
mapped onto four distinct shipped measurements — a treasury balance, a third-party risk-capital
total, a third-party asset total, and priced exposure — and three of the four are reference
figures that leave with the second provenance. Naming the quantity rather than adjudicating the
phrase is what settled it, and the surviving figure already ships as a bucketed series, so this is
a rename onto the pattern rather than a dataset to build. Two obligations come with the name. The
catalogue descriptor has to pin the definition, because the name we adopted is the one the
conformance audit flagged as carrying two unrelated measurements, and with the second provenance
gone the ambiguity would otherwise return as a dataset name instead of a field name. And the
equality the surface asserts in prose between our on-chain figure and the upstream total of the
same name is unverified; the cheapest check for it was a request carrying both provenances, which
this ADR's own decisions remove, so it needs a direct comparison against the upstream endpoint
before the name is relied on.

**Decision 4 means a past answer is not reproducible through this API.** A consumer that needs
to reconstruct what we said last Tuesday goes to the database and the append-only guarantees,
not to an endpoint. It also means a pinned window can change under a cached copy; the exposure
is bounded, because only corrections and backfills can alter a window with a fixed upper bound
and corrections are rare, but the code comment justifying the cache policy on the grounds that
the rows are "immutable once observed" is now false and must be corrected — the next reader will
otherwise build something larger on a false premise.

**Decision 5 means a legitimate request can be refused.** This is the intended behaviour and it
is a real cost to callers, paid down by the catalogue: a client that reads first and last
timestamps and declared cadence can size a request before issuing it. It also means the response
envelope carries no truncation flag, because history never truncates.

**Decision 5 makes the shipped `limit` parameter non-conforming on the history routes.** Several
V1 routes carry one today, under ceilings that disagree with each other. On a history route it
goes: a defaulted limit is the silent truncation decision 5 rejects, and it is worse than the
truncation-with-a-flag alternative because there is no flag. On a list route it stays, under the
unified ceiling, and starts reporting truncation.

**Decision 5 obliges a typed error model where the code has a string.** The shared time-series
query adapter in `app/api/time_series.py` raises its 422 with a stringified domain error, so
every rejection on the surface is prose a client would have to parse. Every time-series route
depends on that one adapter, which is where the rejection contract lands.

**Decision 6 adds an endpoint that must be built and then kept populated.** Series kind, time
axis, source, cadence and the entity key are registry metadata authored per series, not derived
at query time. They land in one schema even though several decisions each contributed a field.

**Decision 7 adds a required field to every series descriptor** — the kind — and an optional
per-point field on filled points. Row-wise point encoding makes the latter cheap: a
1,464-point response carries a handful of extra fields, not 1,464.

**Uniformity needs enforcement or it decays.** The conformance test named in decision 2 is what
makes decision 2 self-enforcing rather than aspirational, and it is the difference between this
ADR being a description of the surface and being a property of it.
