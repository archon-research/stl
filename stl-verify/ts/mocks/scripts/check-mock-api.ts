import assert from 'node:assert/strict';

import type {
  FetchResponse,
  MaybeOptionalInit,
  PathsWithMethod,
} from '@archon-research/http-client-core';
import { createApiClient } from '@archon-research/http-client-core';

import { failingHandler } from '../src/failure.ts';
import {
  GROVE_MAINNET_PROXY,
  MOCK_ORIGIN,
  SPARK_BASE_PROXY,
  SPARK_MAINNET_PROXY,
  SPARK_TX_HASH,
  SPARK_VAULT,
  SPUSDS,
} from '../src/index.ts';
import { mockServer } from '../src/node.ts';
import type { paths } from '../src/schema.ts';

/**
 * Drives the mock handlers through the same `createApiClient` the app builds its
 * API layer on — `ui/src/shared/lib/api-client.ts` reaches it via `http-client-react`,
 * which re-exports this exact function.
 *
 * The value it holds is not coverage: a handler path that does not match, a query
 * param the client serializes differently than the handler reads it, or an
 * envelope whose `mode` disagrees with its rows all pass a type-check and fail
 * here.
 */

// Before createApiClient: openapi-fetch snapshots `globalThis.fetch` at
// construction and msw replaces it here. See README, "Gotchas worth knowing".
mockServer.listen();

// Typed against the same generated `paths` the app and the handlers use, so a
// path, a query param or a response field that drifts from the schema is a type
// error here rather than a runtime assertion nobody wrote yet.
const api = createApiClient<paths>(MOCK_ORIGIN);
const UNKNOWN_ADDRESS = `0x${'1'.repeat(40)}`;

/** The media type `createApiClient` pins its client to. */
type ApiMedia = `${string}/${string}`;

type GetPath = PathsWithMethod<paths, 'get'>;
type GetInit<P extends GetPath> = MaybeOptionalInit<paths[P], 'get'>;
type GetBody<P extends GetPath> = NonNullable<
  FetchResponse<paths[P]['get'], GetInit<P>, ApiMedia>['data']
>;

/**
 * openapi-fetch spreads its init as a tuple whose optionality it derives from
 * the resolved path, so it cannot be checked against a still-generic `P`. The
 * cast is confined to these two wrappers; every call site is checked against
 * the signatures above them.
 */
type LooseInit = Parameters<typeof api.GET>[1];

async function request<P extends GetPath>(
  path: P,
  init: GetInit<P>,
  label: string,
): Promise<GetBody<P>> {
  const { data, error, response } = await api.GET(path, init as LooseInit);
  assert.ok(
    response.ok && data !== undefined,
    `${label}: expected 200 with a body, got ${response.status} ${JSON.stringify(error)}`,
  );
  return data as GetBody<P>;
}

/** The mirror of `request`: asserts the mock says no, and how. */
async function expectStatus<P extends GetPath>(
  path: P,
  init: GetInit<P>,
  status: number,
  label: string,
): Promise<unknown> {
  const { response, error } = await api.GET(path, init as LooseInit);
  assert.equal(
    response.status,
    status,
    `${label}: expected ${status}, got ${response.status} ${JSON.stringify(error)}`,
  );
  return error;
}

/**
 * `request` for the cases that deliberately send a spelling the schema types
 * differently — the API accepts more than the schema can express (pydantic reads
 * `aggregate=YES` as true). The path is still checked.
 */
async function requestLoosely<P extends GetPath>(
  path: P,
  init: Record<string, unknown>,
  label: string,
): Promise<GetBody<P>> {
  return request(path, init as GetInit<P>, label);
}

/**
 * The mirror, for the malformed-input cases: they send values the schema forbids
 * on purpose, so the init cannot be checked against it — the path still is, and
 * the status assertion is the point.
 */
async function expectRejection<P extends GetPath>(
  path: P,
  init: Record<string, unknown>,
  status: number,
  label: string,
): Promise<unknown> {
  return expectStatus(path, init as GetInit<P>, status, label);
}

/** `noUncheckedIndexedAccess` is on: assert the element exists and narrow it. */
function at<Rows extends readonly unknown[]>(
  rows: Rows,
  index: number,
  label: string,
): Rows[number] {
  const row = rows[index];

  assert.ok(row !== undefined, `${label}: expected an element at ${index}`);

  return row;
}

/** The same, for a lookup the fixtures are supposed to guarantee. */
function got<K, V>(map: ReadonlyMap<K, V>, key: K, label: string): V {
  const value = map.get(key);

  assert.ok(value !== undefined, `${label}: ${String(key)} is missing`);

  return value;
}

/**
 * Narrow an envelope's `data` to the arm the query selected. Bucketed and
 * unbucketed rows are separate response schemas and the query param that picks
 * between them is not expressible in the response, so the generated type is a
 * union. Asserting the discriminating field is what makes the checks below read
 * one arm rather than the union.
 */
function armWith<Rows extends readonly object[], K extends PropertyKey>(
  rows: Rows,
  key: K,
  label: string,
): Extract<Rows[number], Record<K, unknown>>[] {
  for (const row of rows) {
    assert.ok(
      key in row,
      `${label}: expected rows carrying \`${String(key)}\``,
    );
  }

  // `rows` is a union of arrays, so the element type is `Rows[number]` and the
  // loop above is what proves which arm this is.
  return rows as unknown as Extract<Rows[number], Record<K, unknown>>[];
}

/** Rows and queries these checks pass around, named off the generated paths. */
type AllocationRow = GetBody<'/v1/primes/{prime_id}/allocations'>[number];
type RiskAllocation =
  GetBody<'/v1/primes/{prime_id}/risk-capital'>['per_allocation'][number];
type ExposureQuery = NonNullable<
  paths['/v1/primes/{prime_id}/exposure']['get']['parameters']['query']
>;

const primeAt = (primeId: string) => ({
  params: { path: { prime_id: primeId } },
});
const activity = <Q>(query: Q) => ({ params: { query } });

async function checkPrimesList() {
  const primes = await request('/v1/primes', {}, 'GET /v1/primes');

  assert.equal(primes.length, 6, 'expected six ALM proxies');
  const vaults = new Set(primes.map((prime) => prime.prime_vault_address));
  assert.equal(vaults.size, 2, 'expected two primes behind the six proxies');
  assert.ok(vaults.has(SPARK_VAULT), "spark's vault is missing");
  assert.ok(
    primes.every((prime) => prime.role === 'alm'),
    'every prime row should be an ALM proxy',
  );
}

async function checkProvenanceAvailabilityCoversEveryPrime() {
  const availability = await request(
    '/v1/provenance/available',
    {},
    'GET /v1/provenance/available',
  );
  const primes = await request('/v1/primes', {}, 'GET /v1/primes');

  const names = new Set(primes.map((prime) => prime.name));
  assert.equal(
    availability.primes.length,
    names.size,
    'one availability entry per prime, not per proxy',
  );
  for (const entry of availability.primes) {
    assert.ok(names.has(entry.name), `not a listed prime: ${entry.name}`);
    // The document's two invariants: `indexed` is always present (a prime is
    // only listed because STL indexes it), and `reference`/`both` travel
    // together (both exist exactly when Sky's monitor covers the prime).
    assert.ok(
      entry.available.includes('indexed'),
      `${entry.name}: indexed missing`,
    );
    assert.equal(
      entry.available.includes('reference'),
      entry.available.includes('both'),
      `${entry.name}: reference and both must appear together`,
    );
  }
  // The fixture world serves `reference` everywhere, so advertising the
  // upstream as down would have the UI hide sources the mocks answer.
  assert.ok(
    availability.reference_upstream_reachable,
    'the fixture upstream is reachable',
  );
}

async function checkRegistryLists() {
  const chains = await request('/v1/chains', {}, 'GET /v1/chains');
  const protocols = await request('/v1/protocols', {}, 'GET /v1/protocols');
  const sources = await request('/v1/data-sources', {}, 'GET /v1/data-sources');

  assert.equal(chains.length, 6);
  assert.ok(protocols.length > 0);
  assert.ok(sources.sources.length > 0);
  // Every allocation's chain has to be nameable, or the table renders a raw id.
  const chainIds = new Set(chains.map((chain) => chain.chain_id));
  assert.ok(chainIds.has(1) && chainIds.has(43114));
}

/**
 * The one assertion that would have caught a protocol id used as a token id:
 * every token a fixture points at, on every proxy, has to resolve.
 */
async function checkEveryReferencedTokenResolves() {
  const primes = await request('/v1/primes', {}, 'GET /v1/primes');
  const tokens = await request(
    '/v1/tokens',
    activity({ limit: 500 }),
    'GET /v1/tokens',
  );
  const known = new Map(tokens.map((token) => [token.id, token]));

  for (const prime of primes) {
    const allocations = await request(
      '/v1/primes/{prime_id}/allocations',
      primeAt(prime.address),
      `allocations for ${prime.address}`,
    );
    for (const row of allocations) {
      for (const field of [
        'receipt_token_id',
        'underlying_token_id',
      ] as const) {
        const id = row[field];
        if (id === null || id === undefined) continue;
        assert.ok(
          known.has(id),
          `allocation ${row.symbol} on ${prime.chain} names ${field} ${id}, which /v1/tokens does not hold`,
        );
        assert.equal(
          got(known, id, `token ${id}`).chain_id,
          row.chain_id,
          `allocation ${row.symbol} names ${field} ${id} from another chain`,
        );
      }
    }
  }

  const feed = await request(
    '/v1/allocations/activity',
    activity({ limit: 1000 }),
    'GET /v1/allocations/activity',
  );
  for (const row of armWith(feed.data, 'action_type', 'activity feed')) {
    assert.ok(
      known.has(row.token_id),
      `activity row for ${row.token_symbol} names token ${row.token_id}, which /v1/tokens does not hold`,
    );
  }
}

async function checkRiskCapitalMatchesAllocations() {
  const allocations = await request(
    '/v1/primes/{prime_id}/allocations',
    primeAt(SPARK_MAINNET_PROXY),
    'spark allocations',
  );
  const riskCapital = await request(
    '/v1/primes/{prime_id}/risk-capital',
    primeAt(SPARK_MAINNET_PROXY),
    'spark risk-capital',
  );

  const receiptTokenIds = new Set(
    allocations
      .map((allocation) => allocation.receipt_token_id)
      .filter((id): id is number => id !== null && id !== undefined),
  );
  for (const entry of riskCapital.per_allocation) {
    assert.ok(
      entry.receipt_token_id !== null,
      `risk-capital reports ${entry.symbol} with no receipt token`,
    );
    assert.ok(
      receiptTokenIds.has(entry.receipt_token_id),
      `risk-capital reports ${entry.symbol} (${entry.receipt_token_id}) but no allocation row holds it`,
    );
  }
}

async function checkUnpricedAllocationHasAFixture() {
  const riskCapital = await request(
    '/v1/primes/{prime_id}/risk-capital',
    primeAt(SPARK_MAINNET_PROXY),
    'spark risk-capital',
  );

  const unpriced = riskCapital.per_allocation.filter((row) => !row.applied);
  assert.equal(unpriced.length, 1, 'the applied=false state needs a fixture');
  assert.equal(at(unpriced, 0, 'unpriced rows').unpriced_reason, 'no_model');
}

async function checkCustodyLegIsPrimeScoped() {
  const allocations = await request(
    '/v1/primes/{prime_id}/allocations',
    primeAt(SPARK_MAINNET_PROXY),
    'spark allocations',
  );

  const custody = allocations.find((row) => row.category === 'custody');
  assert.ok(custody, 'the off-chain custody leg is missing');
  assert.equal(custody.scope, 'prime');
  assert.equal(custody.chain_id, 0);
}

async function checkPrimeFilterDoesNotLeak() {
  const feed = await request(
    '/v1/allocations/activity',
    activity({ prime_id: SPARK_MAINNET_PROXY, limit: 50 }),
    'activity?prime_id',
  );

  assert.equal(feed.mode, 'raw');
  assert.ok(feed.data.length > 0, 'prime-filtered activity is empty');
  assert.ok(
    armWith(feed.data, 'action_type', 'prime-filtered activity').every(
      (row) =>
        row.prime_address.toLowerCase() === SPARK_MAINNET_PROXY.toLowerCase(),
    ),
    'prime_id filter leaked another prime into the feed',
  );
}

async function checkActivitySymbolsExistInAllocations() {
  const allocations = await request(
    '/v1/primes/{prime_id}/allocations',
    primeAt(SPARK_MAINNET_PROXY),
    'spark allocations',
  );
  const feed = await request(
    '/v1/allocations/activity',
    activity({ prime_id: SPARK_MAINNET_PROXY, limit: 50 }),
    'activity?prime_id',
  );

  const held = new Set(allocations.map((allocation) => allocation.symbol));
  for (const row of armWith(feed.data, 'action_type', 'activity?prime_id')) {
    assert.ok(
      row.token_symbol !== null && row.token_symbol !== undefined,
      `activity row ${row.action_type} names no token`,
    );
    assert.ok(
      held.has(row.token_symbol),
      `activity mentions ${row.token_symbol}, which the allocation table does not hold`,
    );
  }
}

async function checkDefaultWindowAlwaysHasData() {
  const feed = await request(
    '/v1/allocations/activity',
    {},
    'activity (default 24h window)',
  );

  assert.ok(
    feed.data.length > 0,
    'the default 24h window returned nothing — fixture timestamps are not anchored to the request clock',
  );
}

async function checkRawAndAggregatedActivityAgree() {
  const raw = await request(
    '/v1/allocations/activity',
    activity({ limit: 1000 }),
    'activity (raw)',
  );
  const aggregated = await request(
    '/v1/allocations/activity',
    activity({ aggregate: true, resolution: 'PT1H', limit: 500 }),
    'activity (aggregated)',
  );

  assert.equal(raw.mode, 'raw');
  assert.equal(aggregated.mode, 'aggregated');
  const bucketed = armWith(
    aggregated.data,
    'event_count',
    'aggregated activity',
  ).reduce((total, bucket) => total + bucket.event_count, 0);
  assert.equal(
    bucketed,
    raw.data.length,
    'the aggregated buckets and the raw feed disagree on how many events the window holds',
  );
}

async function checkAggregatedRowShapeAndGrid() {
  const hourly = await request(
    '/v1/allocations/activity',
    activity({ aggregate: true, resolution: 'PT1H' }),
    'activity (PT1H)',
  );

  assert.equal(hourly.window.resolution, 'PT1H');
  assert.equal(hourly.window.interval_ms, 3_600_000);
  assert.equal(hourly.data.length, 25, '24h of hourly buckets, both ends');
  assert.ok(
    'event_count' in at(hourly.data, 0, 'hourly buckets'),
    'aggregated rows carry a count',
  );

  // The resolution has to reach the bucket grid, not just the echoed window: a
  // fixture that ignores it answers every resolution with the same buckets.
  const quarterHourly = await request(
    '/v1/allocations/activity',
    activity({ aggregate: true, resolution: 'PT15M', limit: 500 }),
    'activity (PT15M)',
  );
  assert.equal(quarterHourly.data.length, 97);
}

/**
 * `net_flow_usd` has to be a USD figure, not a sum of token units: a bucket that
 * adds WETH to USDT reports the same USD-per-unit for both, so comparing two
 * denominations is what catches it.
 */
async function checkAggregatedFlowsAreValued() {
  const usdPerUnit = async (symbol: string) => {
    const feed = await request(
      '/v1/allocations/activity',
      activity({
        aggregate: true,
        resolution: 'PT1H',
        token_symbol: symbol,
        limit: 500,
      }),
      `activity?aggregate&token_symbol=${symbol}`,
    );
    const moved = armWith(
      feed.data,
      'event_count',
      `activity?aggregate&token_symbol=${symbol}`,
    ).filter((bucket) => Number(bucket.total_tx_amount) > 0);
    assert.ok(moved.length > 0, `${symbol} moved nothing in the window`);
    return moved.map(
      (bucket) =>
        Math.abs(Number(bucket.net_flow_usd)) / Number(bucket.total_tx_amount),
    );
  };

  for (const ratio of await usdPerUnit('spWETH')) {
    assert.ok(
      ratio > 1000,
      `a spWETH unit is worth ~1900 USD, valued at ${ratio}`,
    );
  }
  for (const ratio of await usdPerUnit('spUSDT')) {
    assert.ok(
      ratio > 0.5 && ratio < 2,
      `a spUSDT unit is worth ~1 USD, valued at ${ratio}`,
    );
  }
  // A directly-held underlying is deliberately worth nothing here: the endpoint
  // values only receipt-token flows, because a treasury token's outflows are
  // recorded as sweeps and its inflows alone are throughput, not net flow.
  for (const ratio of await usdPerUnit('sUSDS')) {
    assert.equal(ratio, 0, 'a direct holding must not be valued');
  }
}

async function checkRawActivityHonoursLimit() {
  const feed = await request(
    '/v1/allocations/activity',
    activity({ limit: 5 }),
    'activity?limit=5',
  );

  assert.equal(feed.data.length, 5, 'limit was not honoured on the raw feed');
  assert.ok(
    'tx_amount' in at(feed.data, 0, 'raw activity rows'),
    'raw rows carry tx_amount',
  );
}

async function checkDebtRawSnapshots() {
  const raw = await request(
    '/v1/primes/{prime_id}/debt',
    { params: { path: { prime_id: SPARK_VAULT }, query: { limit: 1 } } },
    'debt (raw, limit=1)',
  );

  assert.equal(raw.mode, 'raw');
  assert.equal(raw.source, 'indexed');
  assert.equal(raw.data.length, 1, 'limit=1 should return one snapshot');
  const snapshot = at(
    armWith(raw.data, 'ilk_name', 'raw debt snapshots'),
    0,
    'raw debt snapshots',
  );
  assert.equal(snapshot.ilk_name, 'ALLOCATOR-SPARK-A');
  assert.equal(snapshot.prime_address, SPARK_VAULT);
  assert.match(snapshot.debt_wad, /^\d+$/u, 'debt_wad is an integer string');
}

async function checkCompositeAllocationsAreAUnion() {
  const indexed = await request(
    '/v1/primes/{prime_id}/allocations',
    {
      params: {
        path: { prime_id: SPARK_MAINNET_PROXY },
        query: { source: 'indexed' },
      },
    },
    'allocations (indexed)',
  );
  const reference = await request(
    '/v1/primes/{prime_id}/allocations',
    {
      params: {
        path: { prime_id: SPARK_MAINNET_PROXY },
        query: { source: 'reference' },
      },
    },
    'allocations (reference)',
  );
  const composite = await request(
    '/v1/primes/{prime_id}/allocations',
    {
      params: {
        path: { prime_id: SPARK_MAINNET_PROXY },
        query: { source: 'both' },
      },
    },
    'allocations (both)',
  );

  // Every position once: the union is larger than either half and smaller than
  // their sum, because the halves overlap.
  assert.ok(
    composite.length > indexed.length,
    'the union should carry rows the indexed half does not',
  );
  assert.ok(
    composite.length < indexed.length + reference.length,
    'the union should not be the two halves concatenated',
  );

  const keys = composite.map(
    (row) => `${row.receipt_token_id ?? row.symbol}:${row.chain_id}`,
  );
  assert.equal(new Set(keys).size, keys.length, 'a position appears twice');

  // A row only Sky reports keeps its own provenance, which is what the grid
  // badges; a row both describe says so.
  const sources = new Set(composite.map((row) => row.source));
  assert.ok(sources.has('both'), 'no row reports both provenances');
  assert.ok(sources.has('reference'), 'no row reports Sky alone');
}

/**
 * A reference row's `underlying_*` reflects whether it resolves against STL's
 * registry, not whether Sky reports one — the response contract is "populated
 * when the position resolves to STL's registry, null otherwise".
 */
async function checkReferenceAllocationsResolveUnderlyingFromRegistry() {
  const reference = await request(
    '/v1/primes/{prime_id}/allocations',
    {
      params: {
        path: { prime_id: SPARK_MAINNET_PROXY },
        query: { source: 'reference' },
      },
    },
    'allocations (reference)',
  );

  const resolved = reference.filter((row) => row.receipt_token_id != null);
  const unresolved = reference.filter((row) => row.receipt_token_id == null);
  assert.ok(resolved.length > 0, 'no resolved reference row to check');
  assert.ok(unresolved.length > 0, 'no unresolved reference row to check');

  for (const row of resolved) {
    assert.ok(
      row.underlying_token_id != null && row.underlying_token_address != null,
      `${row.symbol}: a row STL's registry resolves should carry an underlying`,
    );
    assert.ok(
      row.underlying_symbol !== '',
      `${row.symbol}: a resolved row's underlying_symbol should not be empty`,
    );
  }

  for (const row of unresolved) {
    assert.ok(
      row.underlying_token_id == null && row.underlying_token_address == null,
      `${row.symbol}: an unresolved row should carry no underlying identity`,
    );
    assert.equal(
      row.underlying_symbol,
      '',
      `${row.symbol}: an unresolved row's underlying_symbol should be empty`,
    );
  }
}

async function checkCompositeRiskCapitalKeepsBothFigures() {
  const composite = await request(
    '/v1/primes/{prime_id}/risk-capital',
    {
      params: {
        path: { prime_id: SPARK_MAINNET_PROXY },
        query: { source: 'both' },
      },
    },
    'risk-capital (both)',
  );

  assert.equal(composite.source, 'both');

  const merged = composite.per_allocation.filter(
    (row) => row.source === 'both',
  );
  assert.ok(merged.length > 0, 'no merged rows in the breakdown');

  // The point of the fixture: the two provenances disagree, and neither figure
  // is overwritten by the other.
  const disagreeing = merged.find(
    (row) =>
      row.reference_required_risk_capital_usd !== null &&
      Number(row.reference_required_risk_capital_usd) !==
        Number(row.required_risk_capital_usd),
  );
  assert.ok(
    disagreeing !== undefined,
    'no row where the provenances disagree, so the merge is untested',
  );
  assert.ok(
    disagreeing.reference_crr_pct !== null &&
      disagreeing.reference_crr_pct !== undefined,
    "Sky's own ratio should ride along, not be derived from its two figures",
  );

  // Sky prices positions STL resolves no receipt token for, so they cannot join
  // a grid row by id. They must still be in the breakdown.
  const skyOnly = composite.per_allocation.filter(
    (row) => row.source === 'reference' && row.receipt_token_id == null,
  );
  assert.ok(
    skyOnly.length > 0,
    'no Sky-only rows, so the unjoinable case is untested',
  );

  // Largest exposure first, like the endpoint.
  const exposures = composite.per_allocation.map((row) =>
    Number(row.exposure_usd),
  );
  assert.deepEqual(
    exposures,
    [...exposures].sort((left, right) => right - left),
    'the merged breakdown is not ordered by exposure',
  );
}

async function checkPositionKeysJoinTheTwoEndpoints() {
  const allocations = await request(
    '/v1/primes/{prime_id}/allocations',
    {
      params: {
        path: { prime_id: SPARK_MAINNET_PROXY },
        query: { source: 'both' },
      },
    },
    'allocations (both)',
  );
  const risk = await request(
    '/v1/primes/{prime_id}/risk-capital',
    {
      params: {
        path: { prime_id: SPARK_MAINNET_PROXY },
        query: { source: 'both' },
      },
    },
    'risk-capital (both)',
  );

  const byKey = new Map<string, RiskAllocation>();
  for (const row of risk.per_allocation) {
    for (const key of row.position_keys ?? []) {
      if (!byKey.has(key)) byKey.set(key, row);
    }
  }
  assert.ok(byKey.size > 0, 'the risk breakdown publishes no position keys');

  const attach = (allocation: AllocationRow) =>
    (allocation.position_keys ?? [])
      .map((key) => byKey.get(key))
      .find((row) => row !== undefined);

  // The point of the keys: positions with no receipt_token_id still find their
  // requirement. Off-chain custody pairs by protocol, the Arkis vault by
  // address — the two rows Sky prices highest and STL resolves no token for.
  const custody = allocations.find((row) => row.protocol_name === 'anchorage');
  assert.ok(custody !== undefined, 'no custody row to join');
  assert.equal(custody.receipt_token_id ?? null, null);
  assert.ok(
    attach(custody) !== undefined,
    'the custody row found no requirement, so the protocol key is broken',
  );

  const arkis = allocations.find((row) => row.symbol === 'sparkPrimeUSDC1');
  assert.ok(arkis !== undefined, 'no Arkis row to join');
  assert.ok(
    attach(arkis) !== undefined,
    'the Arkis vault found no requirement, so the address key is broken',
  );

  // A key is a join key, so a row must never carry one that resolves to a
  // position describing something else.
  for (const allocation of allocations) {
    const attached = attach(allocation);
    if (attached === undefined) continue;
    const sharesAKey = (allocation.position_keys ?? []).some((key) =>
      (attached.position_keys ?? []).includes(key),
    );
    assert.ok(
      sharesAKey,
      `joined ${allocation.symbol} on a key it does not carry`,
    );
  }
}

async function checkProvenanceSelection() {
  const window = { aggregate: true, limit: 3 };

  for (const source of ['indexed', 'reference', 'both'] as const) {
    const envelope = await request(
      '/v1/primes/{prime_id}/exposure',
      {
        params: {
          path: { prime_id: SPARK_MAINNET_PROXY },
          query: { ...window, source },
        },
      },
      `exposure (source=${source})`,
    );
    assert.equal(
      envelope.source,
      source,
      `source=${source} should answer as itself`,
    );
  }

  // The superseded spelling still resolves, and `false` means STL's own figures
  // by name rather than whatever the default is.
  const byFlag = await request(
    '/v1/primes/{prime_id}/exposure',
    {
      params: {
        path: { prime_id: SPARK_MAINNET_PROXY },
        query: { ...window, reference: true },
      },
    },
    'exposure (reference=true)',
  );
  assert.equal(byFlag.source, 'reference');

  const offByName = await request(
    '/v1/primes/{prime_id}/exposure',
    {
      params: {
        path: { prime_id: SPARK_MAINNET_PROXY },
        query: { ...window, reference: false },
      },
    },
    'exposure (reference=false)',
  );
  assert.equal(offByName.source, 'indexed');
}

async function checkProvenanceConflictRejected() {
  // Preferring one silently would answer a different question than one of the
  // two the caller asked, so the API refuses and the mock must too.
  await expectStatus(
    '/v1/primes/{prime_id}/exposure',
    {
      params: {
        path: { prime_id: SPARK_MAINNET_PROXY },
        query: { aggregate: true, source: 'indexed', reference: true },
      },
    },
    422,
    'source and reference disagreeing',
  );
}

async function checkDebtAggregatedBuckets() {
  const aggregated = await request(
    '/v1/primes/{prime_id}/debt',
    {
      params: {
        path: { prime_id: SPARK_MAINNET_PROXY },
        query: { aggregate: true, resolution: 'PT6H' },
      },
    },
    'debt (aggregated)',
  );

  assert.equal(aggregated.mode, 'aggregated');
  assert.equal(aggregated.data.length, 5);
  const bucket = at(aggregated.data, 0, 'aggregated debt buckets');
  assert.ok(
    'debt_wad' in bucket && !('ilk_name' in bucket),
    'aggregated debt buckets carry no ilk identity',
  );
}

/**
 * A bucket's value must follow its instant and nothing the request asked for:
 * page size, range and resolution all have to agree wherever their grids
 * overlap, or paging or rescaling a chart redraws it.
 */
async function checkSeriesValuesFollowTheirInstant() {
  const read = async (query: ExposureQuery, label: string) =>
    request(
      '/v1/primes/{prime_id}/exposure',
      { params: { path: { prime_id: SPARK_MAINNET_PROXY }, query } },
      `exposure (${label})`,
    );

  const daily = await read({ resolution: 'PT1H', limit: 25 }, '24h hourly');
  const byInstant = new Map(
    daily.data.map((bucket) => [bucket.bucket_start, bucket.exposure_usd]),
  );

  const hoursAgo = (hours: number) =>
    new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
  const variants: [Awaited<ReturnType<typeof read>>, string][] = [
    [await read({ resolution: 'PT1H', limit: 10 }, 'limit=10'), 'page size'],
    [
      await read(
        {
          resolution: 'PT1H',
          limit: 500,
          from_timestamp: hoursAgo(24 * 7),
          to_timestamp: hoursAgo(3),
        },
        'a week ending three hours ago',
      ),
      'window',
    ],
    [await read({ resolution: 'PT6H', limit: 500 }, 'PT6H'), 'resolution'],
  ];

  for (const [variant, asked] of variants) {
    const overlap = variant.data.filter((bucket) =>
      byInstant.has(bucket.bucket_start),
    );
    // Without this the loop below passes on an empty intersection, which is the
    // one way a comparison across two grids can look green while proving
    // nothing.
    assert.ok(overlap.length > 0, `no bucket overlaps across the ${asked}`);
    for (const bucket of overlap) {
      assert.equal(
        bucket.exposure_usd,
        byInstant.get(bucket.bucket_start),
        `bucket ${bucket.bucket_start} changed value with the requested ${asked}`,
      );
    }
  }
}

async function checkRepeatedReadsAreStable() {
  const read = () =>
    request(
      '/v1/primes/{prime_id}/total-capital',
      {
        params: {
          path: { prime_id: SPARK_MAINNET_PROXY },
          query: { resolution: 'PT1H' },
        },
      },
      'total-capital',
    );

  const first = await read();
  const second = await read();
  assert.deepEqual(
    first.data.map((bucket) => bucket.total_capital_usd),
    second.data.map((bucket) => bucket.total_capital_usd),
    'the generated series reshuffled between two reads',
  );
}

async function checkTxEventsLookup() {
  const events = await request(
    '/v1/tx/{tx_hash}/events',
    { params: { path: { tx_hash: SPARK_TX_HASH } } },
    'tx events',
  );

  assert.equal(events.length, 3, 'the seeded transaction decodes three logs');
  assert.ok(
    events.every((event) => event.tx_hash === SPARK_TX_HASH),
    'the tx filter returned events from another transaction',
  );
  assert.deepEqual(
    events.map((event) => event.log_index),
    [244, 243, 242],
  );
}

async function checkUnknownTxHasNoEvents() {
  const events = await request(
    '/v1/tx/{tx_hash}/events',
    { params: { path: { tx_hash: `0x${'0'.repeat(64)}` } } },
    'tx events (unknown)',
  );

  assert.deepEqual(events, [], 'an unknown transaction has no events');
}

async function checkActivityTxHashFilter() {
  const feed = await request(
    '/v1/allocations/activity',
    activity({ tx_hash: SPARK_TX_HASH }),
    'activity?tx_hash',
  );

  assert.equal(
    feed.data.length,
    1,
    'tx_hash should isolate the one activity row that carries it',
  );
}

async function checkProtocolEventsFilter() {
  const sparkLend = await request(
    '/v1/protocol-events',
    activity({ protocol_name: 'SparkLend' }),
    'protocol-events?protocol_name',
  );

  assert.equal(sparkLend.mode, 'raw');
  assert.ok(sparkLend.data.length > 0);
  assert.ok(
    armWith(sparkLend.data, 'protocol_name', 'sparklend events').every(
      (event) => event.protocol_name === 'SparkLend',
    ),
    'the protocol filter leaked another protocol',
  );

  // Exact, not substring: the repository behind this endpoint uses equality.
  const lowerCased = await request(
    '/v1/protocol-events',
    activity({ protocol_name: 'sparklend' }),
    'protocol-events?protocol_name=sparklend',
  );
  assert.deepEqual(lowerCased.data, []);
}

async function checkTokenLookup() {
  const token = await request(
    '/v1/tokens/{chain_id}/{token_address}',
    { params: { path: { chain_id: 1, token_address: SPUSDS } } },
    'token',
  );

  assert.equal(token.symbol, 'spUSDS');
  assert.equal(token.id, 736);
}

async function checkTokenPriceLookup() {
  const price = await request(
    '/v1/tokens/{chain_id}/{token_address}/price',
    { params: { path: { chain_id: 1, token_address: SPUSDS } } },
    'token price',
  );

  assert.equal(price.token_id, 736);
  assert.equal(price.is_stale, false);
  assert.ok(price.price_usd !== null);
}

async function checkTokenSymbolFilter() {
  const matched = await request(
    '/v1/tokens',
    activity({ chain_id: 1, symbol: 'usds', limit: 3 }),
    'tokens?symbol',
  );

  assert.equal(matched.length, 3, 'limit was not honoured');
  assert.ok(
    matched.every((row) => row.symbol?.toLowerCase().includes('usds') === true),
    'the symbol filter is a case-insensitive substring match',
  );
}

async function checkRiskResolvesForEveryRegistryToken() {
  // The drawer opens for any allocation row, so any registry token must answer
  // both risk endpoints; aEthUSDT is outside the curated breakdown map and
  // exercises the fallback.
  const aEthUsdt = '0x23878914efe38d27c4d67ab83ed1b93a74d4086a';
  const breakdown = await request(
    '/v1/risk/{chain_id}/{token_address}/breakdown',
    {
      params: {
        path: { chain_id: 1, token_address: aEthUsdt },
        query: { prime_id: SPARK_MAINNET_PROXY },
      },
    },
    'fallback breakdown',
  );
  assert.ok(breakdown.items.length > 0);

  const envelope = await request(
    '/v1/risk/rrc',
    {
      params: {
        query: {
          chain_id: 1,
          prime_id: SPARK_MAINNET_PROXY,
          token_address: aEthUsdt,
        },
      },
    },
    'fallback rrc',
  );
  assert.equal(envelope.results.length, 2);
  // The same coherence the curated asset gets: a fallback body that summarizes
  // spUSDS is the failure this endpoint is most likely to regress into.
  assertMaxCollapsesResults(envelope, 'fallback rrc (aEthUSDT)');
}

async function checkRiskBreakdownScalesToPrime() {
  const pool = await request(
    '/v1/risk/{chain_id}/{token_address}/breakdown',
    { params: { path: { chain_id: 1, token_address: SPUSDS } } },
    'breakdown',
  );
  const grove = await request(
    '/v1/risk/{chain_id}/{token_address}/breakdown',
    {
      params: {
        path: { chain_id: 1, token_address: SPUSDS },
        query: { prime_id: GROVE_MAINNET_PROXY },
      },
    },
    'breakdown?prime_id=grove',
  );

  assert.equal(pool.receipt_token_id, 736);
  assert.ok(pool.items.length > 0);
  // Asserted before the comparison below indexes it: an empty list would
  // otherwise read as `undefined < undefined`, which is false, and report the
  // scaling as broken rather than the fixture as missing.
  assert.ok(grove.items.length > 0, "grove's scaled breakdown is empty");
  assert.ok(
    Number(at(grove.items, 0, "grove's breakdown").amount_usd) <
      Number(at(pool.items, 0, "the pool's breakdown").amount_usd),
    "prime_id should scale the breakdown to that prime's pool share",
  );
}

/**
 * `max_*` summarizes `results`, so it has to be the largest of them on the asset
 * it was asked about — the check that catches a summary copied from another one.
 */
function assertMaxCollapsesResults(
  envelope: GetBody<'/v1/risk/rrc'>,
  label: string,
) {
  assert.equal(
    Number(envelope.max_rrc_usd),
    Math.max(...envelope.results.map((result) => Number(result.rrc_usd))),
    `${label}: max_rrc_usd is not the largest result`,
  );
  assert.equal(
    Number(envelope.max_crr_pct),
    Math.max(
      ...envelope.results.map((result) => Number(result.comparable_crr_pct)),
    ),
    `${label}: max_crr_pct is not the largest result`,
  );

  const suraf = envelope.results.find((r) => r.risk_model === 'suraf');
  assert.ok(suraf !== undefined, `${label}: the suraf result is missing`);
  const surafDetails = suraf.details;
  assert.ok(
    'crr_pct' in surafDetails,
    `${label}: the suraf result carries no crr_pct detail`,
  );
  assert.equal(
    Number(surafDetails.crr_pct),
    Number(suraf.comparable_crr_pct),
    `${label}: suraf reports one CRR on the result and another in its details`,
  );
  const parts =
    Number(surafDetails.unadjusted_crr_pct) + Number(surafDetails.penalty_pp);
  assert.ok(
    Math.abs(Number(surafDetails.crr_pct) - parts) < 1e-9,
    `${label}: crr_pct is not unadjusted_crr_pct + penalty_pp`,
  );
}

async function checkRrcReportsBothModels() {
  const envelope = await request(
    '/v1/risk/rrc',
    activity({
      prime_id: SPARK_MAINNET_PROXY,
      chain_id: 1,
      token_address: SPUSDS,
    }),
    'rrc',
  );

  assert.deepEqual(envelope.results.map((result) => result.risk_model).sort(), [
    'gap_sweep',
    'suraf',
  ]);
  assert.equal(envelope.asset_id, 736);
  // gap_sweep is the model risk-capital reports, so the two must agree.
  const gapSweep = envelope.results.find((r) => r.risk_model === 'gap_sweep');
  assert.ok(gapSweep !== undefined, 'the gap_sweep result is missing');
  assert.equal(envelope.max_rrc_usd, gapSweep.rrc_usd);
  const details = gapSweep.details;
  assert.ok(
    'loss_usd' in details,
    'the gap_sweep result carries no loss_usd detail',
  );
  assert.equal(details.loss_usd, gapSweep.rrc_usd);
  assertMaxCollapsesResults(envelope, 'rrc (spUSDS)');
}

/**
 * The reference tranches split the same response's Total Risk Capital, so they
 * have to add up to it for whichever prime was asked — grove's 9.2M included.
 */
async function checkReferenceTranchesSplitTotalCapital() {
  for (const primeId of [SPARK_MAINNET_PROXY, GROVE_MAINNET_PROXY]) {
    const reference = await request(
      '/v1/primes/{prime_id}/risk-capital',
      { params: { path: { prime_id: primeId }, query: { reference: true } } },
      `risk-capital?reference for ${primeId}`,
    );

    assert.equal(reference.source, 'reference');
    const total = Number(reference.total_risk_capital_usd);
    const tranches =
      Number(reference.junior_risk_capital_usd) +
      Number(reference.senior_risk_capital_usd);
    assert.ok(
      Math.abs(total - tranches) < 0.01,
      `${primeId}: junior + senior is ${tranches}, not the reported total ${total}`,
    );
    const juniorParts =
      Number(reference.internal_junior_risk_capital_usd) +
      Number(reference.external_junior_risk_capital_usd) +
      Number(reference.tokenized_junior_risk_capital_usd);
    assert.ok(
      Math.abs(Number(reference.junior_risk_capital_usd) - juniorParts) < 0.01,
      `${primeId}: the junior tranche does not equal its own split`,
    );
  }
}

async function checkEmptyProxyIsNotAnError() {
  const allocations = await request(
    '/v1/primes/{prime_id}/allocations',
    primeAt(SPARK_BASE_PROXY),
    'allocations for a proxy that holds nothing',
  );

  assert.deepEqual(
    allocations,
    [],
    'a real proxy holding nothing answers an empty list, not a 404',
  );
}

async function checkUnknownPrimeIsNotFound() {
  for (const path of [
    '/v1/primes/{prime_id}/allocations',
    '/v1/primes/{prime_id}/risk-capital',
    '/v1/primes/{prime_id}/exposure',
    '/v1/primes/{prime_id}/total-capital',
    '/v1/primes/{prime_id}/debt',
  ] as const) {
    await expectStatus(path, primeAt(UNKNOWN_ADDRESS), 404, path);
  }
}

async function checkUnknownAssetIsNotFound() {
  await expectStatus(
    '/v1/tokens/{chain_id}/{token_address}',
    { params: { path: { chain_id: 1, token_address: UNKNOWN_ADDRESS } } },
    404,
    'token (unknown)',
  );
  await expectStatus(
    '/v1/risk/{chain_id}/{token_address}/breakdown',
    { params: { path: { chain_id: 1, token_address: UNKNOWN_ADDRESS } } },
    404,
    'breakdown (unknown)',
  );
  await expectStatus(
    '/v1/risk/rrc',
    activity({
      prime_id: SPARK_MAINNET_PROXY,
      chain_id: 1,
      token_address: UNKNOWN_ADDRESS,
    }),
    404,
    'rrc (unknown asset)',
  );
}

/**
 * A fixture table is keyed by addresses the request supplies, so a param naming
 * an inherited member must still miss. Indexed without a guard these answer with
 * `Object.prototype`'s members — a 404 that becomes a 200 carrying a function.
 */
async function checkInheritedKeysAreNotFound() {
  for (const key of ['constructor', '__proto__', 'toString']) {
    await expectStatus(
      '/v1/primes/{prime_id}/risk-capital',
      primeAt(key),
      404,
      `risk-capital for ${key}`,
    );
    await expectStatus(
      '/v1/risk/{chain_id}/{token_address}/breakdown',
      { params: { path: { chain_id: 1, token_address: key } } },
      404,
      `breakdown for ${key}`,
    );
    // Not a 404: the token resolves, but the share lookup for this prime must
    // miss and answer 503 rather than serving the whole pool as one prime's.
    await expectStatus(
      '/v1/risk/{chain_id}/{token_address}/breakdown',
      {
        params: {
          path: { chain_id: 1, token_address: SPUSDS },
          query: { prime_id: key },
        },
      },
      503,
      `breakdown?prime_id=${key}`,
    );
  }
}

async function checkReferenceDebtRequiresAggregate() {
  await expectStatus(
    '/v1/primes/{prime_id}/debt',
    {
      params: {
        path: { prime_id: SPARK_MAINNET_PROXY },
        query: { reference: true },
      },
    },
    400,
    'reference debt without aggregate',
  );
}

async function checkMalformedParamsAreRejected() {
  await expectRejection(
    '/v1/allocations/activity',
    activity({ limit: 'abc' }),
    422,
    'limit=abc',
  );
  await expectStatus(
    '/v1/allocations/activity',
    activity({ limit: 0 }),
    422,
    'limit=0',
  );
  await expectStatus(
    '/v1/allocations/activity',
    activity({ limit: 99_999 }),
    422,
    'limit above the documented maximum',
  );
  await expectRejection(
    '/v1/allocations/activity',
    activity({ chain_id: 'abc' }),
    422,
    'chain_id=abc',
  );
  await expectStatus(
    '/v1/allocations/activity',
    activity({ from_timestamp: 'lastweek' }),
    422,
    'unparseable from_timestamp',
  );
}

/**
 * A `bool` query param is parsed by pydantic, which takes six spellings either
 * side in any case and rejects everything else. Reading an unrecognised value as
 * `false` would serve a raw envelope to a screen that asked for buckets.
 */
async function checkBooleanFlagsFollowPydantic() {
  const feed = await requestLoosely(
    '/v1/allocations/activity',
    activity({ aggregate: 'YES' }),
    'activity?aggregate=YES',
  );
  assert.equal(feed.mode, 'aggregated', 'YES is a true this API accepts');

  await expectRejection(
    '/v1/allocations/activity',
    activity({ aggregate: 'maybe' }),
    422,
    'aggregate=maybe',
  );
}

async function checkIllegalWindowsAreRejected() {
  const now = new Date();
  const earlier = new Date(now.getTime() - 60 * 60 * 1000);

  await expectStatus(
    '/v1/allocations/activity',
    activity({
      from_timestamp: now.toISOString(),
      to_timestamp: earlier.toISOString(),
    }),
    422,
    'inverted window',
  );
  await expectStatus(
    '/v1/allocations/activity',
    activity({
      from_timestamp: new Date(
        now.getTime() - 400 * 24 * 60 * 60 * 1000,
      ).toISOString(),
    }),
    422,
    'window beyond the 366-day maximum',
  );
  await expectStatus(
    '/v1/allocations/activity',
    activity({ aggregate: true, resolution: 'PT1M' }),
    422,
    'resolution finer than the 24h floor',
  );
}

async function checkRrcRejectsAmbiguousIdentity() {
  await expectRejection(
    '/v1/risk/rrc',
    activity({ chain_id: 1, token_address: SPUSDS }),
    422,
    'rrc without prime_id',
  );
  await expectStatus(
    '/v1/risk/rrc',
    activity({ prime_id: SPARK_MAINNET_PROXY, chain_id: 1 }),
    422,
    'rrc with half an asset pair',
  );
  await expectStatus(
    '/v1/risk/rrc',
    activity({
      prime_id: SPARK_MAINNET_PROXY,
      asset_id: 736,
      chain_id: 1,
      token_address: SPUSDS,
    }),
    422,
    'rrc with both asset identities',
  );
}

/**
 * The failure control is the only route to an error state offline — every
 * fixture succeeds — so the recovery behaviour built on it (the range
 * selector's retry, the cards' error copy) stops being exercised the moment
 * this stops working, and nothing else would say so.
 */
async function checkAReadCanBeMadeToFail() {
  const healthy = await request(
    '/v1/primes/{prime_id}/risk-capital',
    primeAt(SPARK_MAINNET_PROXY),
    'risk-capital before the failure is installed',
  );
  assert.ok(
    healthy.prime_exposure_usd !== undefined,
    'the read has to succeed first, or the 503 below proves nothing',
  );

  // Dropped by the runner's `mockServer.reset()` after this check.
  mockServer.server.use(failingHandler('risk-capital'));

  const error = await expectStatus(
    '/v1/primes/{prime_id}/risk-capital',
    primeAt(SPARK_MAINNET_PROXY),
    503,
    'risk-capital, failing',
  );
  const detail = (error as { detail?: unknown } | undefined)?.detail;
  assert.ok(
    typeof detail === 'string' && /unavailable/u.test(detail),
    'a failed read reports why as a string, in the shape FastAPI raises',
  );
}

const checks: [string, () => Promise<void>][] = [
  ['primes list shape', checkPrimesList],
  ['registry lists', checkRegistryLists],
  ['every referenced token resolves', checkEveryReferencedTokenResolves],
  [
    'risk-capital rows have allocation rows',
    checkRiskCapitalMatchesAllocations,
  ],
  ['the unpriced allocation has a fixture', checkUnpricedAllocationHasAFixture],
  ['the custody leg is prime-scoped', checkCustodyLegIsPrimeScoped],
  ['the prime_id filter does not leak', checkPrimeFilterDoesNotLeak],
  ['activity symbols are held', checkActivitySymbolsExistInAllocations],
  ['the default 24h window has data', checkDefaultWindowAlwaysHasData],
  ['raw and aggregated activity agree', checkRawAndAggregatedActivityAgree],
  ['the aggregated grid follows resolution', checkAggregatedRowShapeAndGrid],
  ['aggregated flows are valued in USD', checkAggregatedFlowsAreValued],
  ['the raw feed honours limit', checkRawActivityHonoursLimit],
  ['debt raw snapshots', checkDebtRawSnapshots],
  ['provenance selection', checkProvenanceSelection],
  ['composite allocations are a union', checkCompositeAllocationsAreAUnion],
  [
    'reference allocations resolve underlying from the registry',
    checkReferenceAllocationsResolveUnderlyingFromRegistry,
  ],
  [
    'position keys join the allocations and risk endpoints',
    checkPositionKeysJoinTheTwoEndpoints,
  ],
  [
    'composite risk capital keeps both figures',
    checkCompositeRiskCapitalKeepsBothFigures,
  ],
  [
    'availability covers every prime',
    checkProvenanceAvailabilityCoversEveryPrime,
  ],
  [
    'a contradictory provenance pair is refused',
    checkProvenanceConflictRejected,
  ],
  ['debt aggregated buckets', checkDebtAggregatedBuckets],
  ['series values follow their instant', checkSeriesValuesFollowTheirInstant],
  ['repeated reads are stable', checkRepeatedReadsAreStable],
  ['tx-events lookup', checkTxEventsLookup],
  ['an unknown tx has no events', checkUnknownTxHasNoEvents],
  ['the activity tx_hash filter', checkActivityTxHashFilter],
  ['the protocol-events filter is exact', checkProtocolEventsFilter],
  ['token lookup', checkTokenLookup],
  ['token price lookup', checkTokenPriceLookup],
  ['the token symbol filter', checkTokenSymbolFilter],
  ['the breakdown scales to a prime', checkRiskBreakdownScalesToPrime],
  [
    'every registry token resolves breakdown and rrc',
    checkRiskResolvesForEveryRegistryToken,
  ],
  ['rrc reports both models', checkRrcReportsBothModels],
  [
    'the reference tranches split total capital',
    checkReferenceTranchesSplitTotalCapital,
  ],
  ['an empty proxy is not an error', checkEmptyProxyIsNotAnError],
  ['an unknown prime is a 404', checkUnknownPrimeIsNotFound],
  ['an unknown asset is a 404', checkUnknownAssetIsNotFound],
  ['an inherited key is not a fixture', checkInheritedKeysAreNotFound],
  ['reference debt requires aggregate', checkReferenceDebtRequiresAggregate],
  ['malformed params are rejected', checkMalformedParamsAreRejected],
  ['boolean flags follow pydantic', checkBooleanFlagsFollowPydantic],
  ['illegal windows are rejected', checkIllegalWindowsAreRejected],
  ['rrc rejects an ambiguous identity', checkRrcRejectsAmbiguousIdentity],
  ['a read can be made to fail', checkAReadCanBeMadeToFail],
];

let failed = 0;
for (const [name, check] of checks) {
  try {
    await check();
    console.log(`ok   ${name}`);
  } catch (error) {
    // An AssertionError means the mock is wrong. Anything else means this script
    // is, and reporting that as one failing fixture buries the stack that says
    // where.
    if (!(error instanceof assert.AssertionError)) {
      console.error(`ERROR ${name} — the check threw before asserting`);
      mockServer.close();
      throw error;
    }
    failed += 1;
    console.error(`FAIL ${name}`);
    console.error(error.message);
  } finally {
    mockServer.reset();
  }
}

mockServer.close();

console.log(
  `\n${checks.length - failed}/${checks.length} mock API checks passed`,
);
process.exitCode = failed === 0 ? 0 : 1;
