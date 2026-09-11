import type { EdgeRow, NodeRow } from '../lib/contract.ts';
import { kindOf } from '../schema/edges.ts';
import type { RecordType } from '../schema/vocabularies.ts';
import { CONCEPT_SEED, NARROWER_THAN_SEED } from './taxonomy-seed.ts';
import { ENTITY_SEED, SECURITY_SEED, SOURCE_SEED } from './worksheet-seed.ts';

/**
 * The mock as an append-only store, resolved the way the database resolves.
 *
 * This is the one place the mock deliberately does more than "a POST shows up in
 * the following GET", and it earns the cost. `createMockStore` models a mutable
 * collection, which is precisely what `sec_node` is not: there is no update and
 * no delete, a change is a new row, and *which* row is current is the outcome of
 * a two-step resolution. A mock that let a PUT overwrite a row would let the UI
 * be built against semantics the store does not have — and the bill for that
 * arrives at integration, when every edit screen turns out to need a valid-time
 * window it never collected.
 *
 * So appends accumulate and reads resolve, in the order the migration's views
 * use — latest append per `(id, valid_from)` **first**, then the valid window.
 * The other order resurrects superseded rows, which is exactly the bug the view
 * comments warn about, and having it wrong here would look like working code.
 */

/** A stored append. `ingestSeq` stands in for `ingest_xid`. */
type NodeAppend = NodeRow & { ingestSeq: number };
type EdgeAppend = EdgeRow & { ingestSeq: number };

const OPEN = 'infinity';

/** Monotonic stand-ins for what the database generates. */
let recordSeq = 0;
let ingestSeq = 0;

let nodeAppends: NodeAppend[] = [];
let edgeAppends: EdgeAppend[] = [];

function nextRecordId(): number {
  recordSeq += 1;

  return recordSeq;
}

function nextIngestSeq(): number {
  ingestSeq += 1;

  return ingestSeq;
}

/**
 * A stand-in for `content_hash`.
 *
 * Not sha256: the real hash is computed by the append guard over a Postgres
 * serialization of the row, and reproducing that in the browser would be a
 * fiction with a convincing shape. A short deterministic digest keeps the column
 * present and visibly opaque without inviting anyone to verify a chain against
 * it.
 */
function mockHash(seed: string, previous: string | null): string {
  let h = 0x811c9dc5;
  for (const ch of `${previous ?? ''}|${seed}`) {
    h ^= ch.codePointAt(0) ?? 0;
    h = Math.imul(h, 0x01000193) >>> 0;
  }

  return `mock:${h.toString(16).padStart(8, '0')}`;
}

const SEED_DATE = '2026-08-26';
const SEED_ACTOR = 'migration:20260904_120100';

function seedNode(
  id: string,
  recordType: RecordType,
  attrs: Record<string, unknown>,
  options?: { status?: string; chainId?: number },
): NodeAppend {
  const recordId = nextRecordId();

  return {
    id,
    record_type: recordType,
    chain_id: options?.chainId ?? null,
    status: options?.status ?? 'ACTIVE',
    attrs,
    valid_from: SEED_DATE,
    valid_to: OPEN,
    record_id: recordId,
    processing_version: 0,
    ingested_at: `${SEED_DATE}T00:00:00Z`,
    actor: SEED_ACTOR,
    change_reason_code: 'SEED_LOAD',
    change_reason: 'Seed: wave 1',
    approved_by: null,
    supersedes_record_id: null,
    source_system: 'migration',
    content_hash: mockHash(`${id}:${recordId}`, null),
    ingestSeq: nextIngestSeq(),
  };
}

function seedEdge(
  srcId: string,
  dstId: string,
  relType: string,
  extra?: Partial<EdgeRow>,
): EdgeAppend {
  const recordId = nextRecordId();
  const srcKind = kindOfId(srcId);
  const dstKind = kindOfId(dstId);

  return {
    edge_id: `rel:${relType}:${srcId}:${dstId}:1`,
    edge_seq: 1,
    src_id: srcId,
    src_kind: srcKind,
    dst_id: dstId,
    dst_kind: dstKind,
    rel_type: relType,
    rel_weight: null,
    weight_basis: null,
    weight_asof_block: null,
    payload: {},
    valid_from: SEED_DATE,
    valid_to: OPEN,
    record_id: recordId,
    processing_version: 0,
    ingested_at: `${SEED_DATE}T00:00:00Z`,
    actor: SEED_ACTOR,
    change_reason_code: 'SEED_LOAD',
    change_reason: 'Seed: wave 1',
    approved_by: null,
    supersedes_record_id: null,
    source_system: 'migration',
    content_hash: mockHash(`${relType}:${srcId}:${dstId}:${recordId}`, null),
    ingestSeq: nextIngestSeq(),
    ...extra,
  };
}

/**
 * The kind an id claims, or a throw.
 *
 * `kindOf` is the shared rule — one copy, in the schema layer, mirroring
 * `sec_node_id_prefix_chk`. The throw is this module's addition: an ungoverned
 * id in a *fixture* is a bug in the fixture, not a request to reject.
 */
function kindOfId(id: string): RecordType {
  const kind = kindOf(id);
  if (kind === undefined) {
    throw new Error(`ungoverned node id in a fixture: ${id}`);
  }

  return kind;
}

function buildSeed(): void {
  recordSeq = 0;
  ingestSeq = 0;
  nodeAppends = [];
  edgeAppends = [];

  for (const [id, conceptClass, label, definition] of CONCEPT_SEED) {
    nodeAppends.push(
      seedNode(id, 'CONCEPT', {
        concept_class: conceptClass,
        label,
        definition,
        vocabulary_source: 'ref',
      }),
    );
  }

  for (const [src, dst] of NARROWER_THAN_SEED) {
    edgeAppends.push(seedEdge(src, dst, 'NARROWER_THAN'));
  }

  for (const entity of ENTITY_SEED) {
    nodeAppends.push(
      seedNode(entity.id, 'ENTITY', entity.attrs, { status: entity.status }),
    );
    if (entity.parentEntityId !== undefined) {
      edgeAppends.push(
        seedEdge(entity.id, entity.parentEntityId, 'SUBSIDIARY_OF'),
      );
    }
  }

  for (const source of SOURCE_SEED) {
    nodeAppends.push(
      seedNode(source.id, 'SOURCE', source.attrs, { status: source.status }),
    );
  }

  for (const security of SECURITY_SEED) {
    nodeAppends.push(
      seedNode(security.id, 'SECURITY', security.attrs, {
        ...(security.chainId !== undefined && { chainId: security.chainId }),
      }),
    );

    for (const conceptId of security.belongsTo) {
      edgeAppends.push(seedEdge(security.id, conceptId, 'BELONGS_TO'));
    }

    if (security.issuerEntityId !== undefined) {
      edgeAppends.push(
        seedEdge(security.id, security.issuerEntityId, 'ISSUED_BY'),
      );
    }

    if (security.underlying !== undefined) {
      edgeAppends.push(
        seedEdge(security.id, security.underlying, 'HAS_UNDERLYING', {
          rel_weight: '1.000000000000000000',
          weight_basis: 'VALUE',
        }),
      );
    }
  }
}

buildSeed();

/** Re-seeds, for `setupMocks({ onReset })`. */
export function resetStore(): void {
  buildSeed();
}

/**
 * Resolves the current row per logical record.
 *
 * Two steps, in this order:
 *
 * 1. Group by `(id, valid_from)` and keep the winning **append**:
 *    `processing_version` DESC, then knowledge time DESC. A correction at
 *    version N beats every later append at 0 for that window, which is the
 *    documented consequence in the migration — a curator's correction is not
 *    silently undone by the next pipeline run.
 * 2. Of those, keep the windows containing the as-of date. A zero-length window
 *    contains no date, so a tombstone drops out here with its history intact.
 */
function resolve<
  T extends {
    id: string;
    valid_from: string;
    valid_to: string;
    processing_version: number;
    ingestSeq: number;
  },
>(appends: readonly T[], logicalKey: (row: T) => string, asOf: string): T[] {
  const winners = new Map<string, T>();

  for (const row of appends) {
    const key = `${logicalKey(row)}|${row.valid_from}`;
    const held = winners.get(key);
    const wins =
      held === undefined ||
      row.processing_version > held.processing_version ||
      (row.processing_version === held.processing_version &&
        row.ingestSeq > held.ingestSeq);

    if (wins) {
      winners.set(key, row);
    }
  }

  return [...winners.values()].filter(
    (row) =>
      row.valid_from <= asOf && (row.valid_to === OPEN || row.valid_to > asOf),
  );
}

function today(): string {
  // The mock's own clock. `fixtures are relative to a clock, not to a date` in
  // the sibling mocks workspace holds here too: a hard-coded date makes every
  // open window look closed the moment it passes.
  return new Date().toISOString().slice(0, 10);
}

export type NodeQuery = {
  recordType?: RecordType;
  q?: string;
  conceptClass?: string;
  narrowerThan?: string;
  status?: string;
  limit?: number;
  asOf?: string;
};

export function listNodes(query: NodeQuery): NodeRow[] {
  const asOf = query.asOf ?? today();
  let rows = resolve(nodeAppends, (r) => r.id, asOf);

  if (query.recordType !== undefined) {
    rows = rows.filter((r) => r.record_type === query.recordType);
  }

  if (query.status !== undefined) {
    rows = rows.filter((r) => r.status === query.status);
  }

  if (query.conceptClass !== undefined) {
    rows = rows.filter((r) => r.attrs['concept_class'] === query.conceptClass);
  }

  if (query.narrowerThan !== undefined) {
    // The subtree walk. `dim_cluster` is the pivot that makes this a lookup in
    // production; here it is the walk itself, capped like the real one.
    const subtree = descendantsOf(query.narrowerThan, asOf);
    rows = rows.filter((r) => subtree.has(r.id));
  }

  if (query.q !== undefined && query.q !== '') {
    const needle = query.q.toLowerCase();
    rows = rows.filter(
      (r) =>
        r.id.toLowerCase().includes(needle) ||
        JSON.stringify(r.attrs).toLowerCase().includes(needle),
    );
  }

  return rows
    .sort((a, b) => a.id.localeCompare(b.id))
    .slice(0, query.limit ?? 100)
    .map(stripInternal);
}

/**
 * Every concept under an ancestor, walked through `NARROWER_THAN`.
 *
 * Depth-capped at 16 and cycle-guarded, matching the look-through walk's bounds.
 * The cap is not decoration: a `NARROWER_THAN` cycle is authorable — nothing in
 * the engine rejects one — and without the guard this is an infinite loop in the
 * dev server.
 */
function descendantsOf(ancestorId: string, asOf?: string): Set<string> {
  const edges = resolve(
    edgeAppends.map((e) => ({ ...e, id: e.edge_id })),
    (r) => `${r.rel_type}|${r.src_id}|${r.dst_id}|${r.edge_seq}`,
    asOf ?? today(),
  ).filter((e) => e.rel_type === 'NARROWER_THAN');

  const childrenOf = new Map<string, string[]>();
  for (const edge of edges) {
    const list = childrenOf.get(edge.dst_id);
    if (list === undefined) {
      childrenOf.set(edge.dst_id, [edge.src_id]);
    } else {
      list.push(edge.src_id);
    }
  }

  const seen = new Set<string>([ancestorId]);
  let frontier = [ancestorId];

  for (let depth = 0; depth < 16 && frontier.length > 0; depth += 1) {
    const next: string[] = [];
    for (const parent of frontier) {
      for (const child of childrenOf.get(parent) ?? []) {
        if (!seen.has(child)) {
          seen.add(child);
          next.push(child);
        }
      }
    }
    frontier = next;
  }

  return seen;
}

/** The concepts a node belongs to, closed over `NARROWER_THAN` ancestry. */
export function conceptClosureFor(nodeId: string): string[] {
  const asOf = today();
  const memberships = resolve(
    edgeAppends.map((e) => ({ ...e, id: e.edge_id })),
    (r) => `${r.rel_type}|${r.src_id}|${r.dst_id}|${r.edge_seq}`,
    asOf,
  ).filter((e) => e.src_id === nodeId && e.rel_type === 'BELONGS_TO');

  const closure = new Set<string>();
  const parentEdges = resolve(
    edgeAppends.map((e) => ({ ...e, id: e.edge_id })),
    (r) => `${r.rel_type}|${r.src_id}|${r.dst_id}|${r.edge_seq}`,
    asOf,
  ).filter((e) => e.rel_type === 'NARROWER_THAN');

  const parentOf = new Map<string, string>();
  for (const edge of parentEdges) {
    parentOf.set(edge.src_id, edge.dst_id);
  }

  for (const membership of memberships) {
    let cursor: string | undefined = membership.dst_id;
    for (let depth = 0; depth < 16 && cursor !== undefined; depth += 1) {
      if (closure.has(cursor)) {
        break;
      }
      closure.add(cursor);
      cursor = parentOf.get(cursor);
    }
  }

  return [...closure];
}

export function getNode(id: string, asOf?: string): NodeRow | undefined {
  const row = resolve(nodeAppends, (r) => r.id, asOf ?? today()).find(
    (r) => r.id === id,
  );

  return row === undefined ? undefined : stripInternal(row);
}

/** Every append for one node, newest first — the bitemporal history view. */
export function nodeHistory(id: string): NodeRow[] {
  return nodeAppends
    .filter((r) => r.id === id)
    .sort((a, b) => b.ingestSeq - a.ingestSeq)
    .map(stripInternal);
}

export type EdgeQuery = {
  srcId?: string;
  dstId?: string;
  relType?: string;
  asOf?: string;
  limit?: number;
};

export function listEdges(query: EdgeQuery): EdgeRow[] {
  let rows = resolve(
    edgeAppends.map((e) => ({ ...e, id: e.edge_id })),
    (r) => `${r.rel_type}|${r.src_id}|${r.dst_id}|${r.edge_seq}`,
    query.asOf ?? today(),
  );

  if (query.srcId !== undefined) {
    rows = rows.filter((r) => r.src_id === query.srcId);
  }

  if (query.dstId !== undefined) {
    rows = rows.filter((r) => r.dst_id === query.dstId);
  }

  if (query.relType !== undefined) {
    rows = rows.filter((r) => r.rel_type === query.relType);
  }

  return rows.slice(0, query.limit ?? 200).map(stripEdgeInternal);
}

/** Edges out of a node, for the shape evaluation a form runs. */
export function edgesOut(
  nodeId: string,
): { relType: string; direction: 'out' | 'in' }[] {
  return listEdges({ srcId: nodeId }).map((e) => ({
    relType: e.rel_type,
    direction: 'out' as const,
  }));
}

/**
 * Reads a string off an untyped request body.
 *
 * `String(body['x'] ?? fallback)` is what this replaces, and it was wrong as
 * well as unlint-able: a client that sent an object for `status` would store the
 * literal text `[object Object]` and the row would look merely odd rather than
 * rejected. Anything that is not a string falls back.
 */
function str(value: unknown, fallback: string): string {
  return typeof value === 'string' ? value : fallback;
}

/** Reads a plain object off an untyped request body. */
function obj(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? { ...value }
    : {};
}

export type AppendResult =
  | {
      ok: true;
      recordId: number;
      processingVersion: number;
      contentHash: string;
      ingestedAt: string;
    }
  | { ok: false; status: 403 | 409 | 422; message: string };

/**
 * Appends a node version.
 *
 * Rejections here stand in for the write boundary, and they are the ones the
 * client *cannot* check for itself: that an endpoint exists as a current node,
 * and that the id prefix agrees with the declared kind. The cross-field rules the
 * client already enforces are re-checked deliberately — a mock that trusts its
 * client is a mock that hides a missing server check.
 */
export function appendNode(
  body: Record<string, unknown>,
  recordType: RecordType,
): AppendResult {
  const id = body['id'];
  if (typeof id !== 'string') {
    return { ok: false, status: 422, message: 'id is required' };
  }

  if (kindOfIdSafe(id) !== recordType) {
    return {
      ok: false,
      status: 422,
      message: `a ${recordType} id must carry the ${recordType.toLowerCase()} prefix`,
    };
  }

  const validFrom = str(body['valid_from'], today());
  const reasonCode = str(body['change_reason_code'], '');
  const approvedBy = body['approved_by'];

  if (REQUIRES_APPROVAL.has(reasonCode) && typeof approvedBy !== 'string') {
    return {
      ok: false,
      status: 422,
      message: `${reasonCode} requires approved_by`,
    };
  }

  const supersedes = body['supersedes_record_id'];
  let previousHash: string | null = null;
  if (typeof supersedes === 'number') {
    const predecessor = nodeAppends.find((r) => r.record_id === supersedes);
    if (predecessor === undefined) {
      // The guard needs the predecessor's hash to chain this row's, so an
      // unresolvable pointer is rejected rather than stored as an orphan.
      return {
        ok: false,
        status: 422,
        message: `supersedes_record_id ${supersedes} does not resolve`,
      };
    }
    previousHash = predecessor.content_hash;
  }

  const recordId = nextRecordId();
  const attrs = foldAttrs(body, recordType);
  const append: NodeAppend = {
    id,
    record_type: recordType,
    chain_id: typeof body['chain_id'] === 'number' ? body['chain_id'] : null,
    status: str(body['status'], 'ACTIVE'),
    attrs,
    valid_from: validFrom,
    valid_to: str(body['valid_to'], OPEN),
    record_id: recordId,
    processing_version:
      typeof body['processing_version'] === 'number'
        ? body['processing_version']
        : 0,
    ingested_at: new Date().toISOString(),
    actor: 'curator:local',
    change_reason_code: reasonCode,
    change_reason: str(body['change_reason'], ''),
    approved_by: typeof approvedBy === 'string' ? approvedBy : null,
    supersedes_record_id: typeof supersedes === 'number' ? supersedes : null,
    source_system: 'curation-ui',
    content_hash: mockHash(`${id}:${recordId}:${validFrom}`, previousHash),
    ingestSeq: nextIngestSeq(),
  };

  nodeAppends.push(append);

  return {
    ok: true,
    recordId,
    processingVersion: append.processing_version,
    contentHash: append.content_hash,
    ingestedAt: append.ingested_at,
  };
}

export function appendEdge(body: Record<string, unknown>): AppendResult {
  const srcId = str(body['src_id'], '');
  const dstId = str(body['dst_id'], '');
  const relType = str(body['rel_type'], '');

  // Endpoint existence and kind (GQ-11), which the append guard rejects — the
  // ADR moved this from the validator to the engine on 2026-09-11. Either way it
  // is the one rule the form genuinely cannot enforce, since the
  // client does not hold the node set.
  for (const [id, label] of [
    [srcId, 'src_id'],
    [dstId, 'dst_id'],
  ] as const) {
    if (getNode(id) === undefined) {
      return {
        ok: false,
        status: 422,
        message: `${label} ${id} is not a current node (GQ-11)`,
      };
    }
  }

  const reasonCode = str(body['change_reason_code'], '');
  if (
    REQUIRES_APPROVAL.has(reasonCode) &&
    typeof body['approved_by'] !== 'string'
  ) {
    return {
      ok: false,
      status: 422,
      message: `${reasonCode} requires approved_by`,
    };
  }

  const recordId = nextRecordId();
  const edgeSeq = typeof body['edge_seq'] === 'number' ? body['edge_seq'] : 1;
  const validFrom = str(body['valid_from'], today());
  const append: EdgeAppend = {
    edge_id: `rel:${relType}:${srcId}:${dstId}:${edgeSeq}`,
    edge_seq: edgeSeq,
    src_id: srcId,
    src_kind: kindOfId(srcId),
    dst_id: dstId,
    dst_kind: kindOfId(dstId),
    rel_type: relType,
    rel_weight:
      typeof body['rel_weight'] === 'string' ? body['rel_weight'] : null,
    weight_basis:
      typeof body['weight_basis'] === 'string' ? body['weight_basis'] : null,
    weight_asof_block:
      typeof body['weight_asof_block'] === 'number'
        ? body['weight_asof_block']
        : null,
    payload: obj(body['payload']),
    valid_from: validFrom,
    valid_to: str(body['valid_to'], OPEN),
    record_id: recordId,
    processing_version: 0,
    ingested_at: new Date().toISOString(),
    actor: 'curator:local',
    change_reason_code: reasonCode,
    change_reason: str(body['change_reason'], ''),
    approved_by:
      typeof body['approved_by'] === 'string' ? body['approved_by'] : null,
    supersedes_record_id:
      typeof body['supersedes_record_id'] === 'number'
        ? body['supersedes_record_id']
        : null,
    source_system: 'curation-ui',
    content_hash: mockHash(`${relType}:${srcId}:${dstId}:${recordId}`, null),
    ingestSeq: nextIngestSeq(),
  };

  edgeAppends.push(append);

  return {
    ok: true,
    recordId,
    processingVersion: 0,
    contentHash: append.content_hash,
    ingestedAt: append.ingested_at,
  };
}

const REQUIRES_APPROVAL = new Set([
  'RECLASSIFICATION',
  'REPOINT',
  'RESTATEMENT',
  'RETRACTION',
  'DEDUP_SUPERSEDE',
]);

/** Node columns, so everything else on the body folds into `attrs`. */
const NODE_COLUMNS = new Set([
  'id',
  'record_type',
  'chain_id',
  'status',
  'valid_from',
  'valid_to',
  'processing_version',
  'change_reason_code',
  'change_reason',
  'approved_by',
  'supersedes_record_id',
]);

function foldAttrs(
  body: Record<string, unknown>,
  _recordType: RecordType,
): Record<string, unknown> {
  const attrs: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(body)) {
    if (!NODE_COLUMNS.has(key) && value !== undefined) {
      attrs[key] = value;
    }
  }

  return attrs;
}

function kindOfIdSafe(id: string): RecordType | undefined {
  return kindOf(id);
}

function stripInternal(row: NodeAppend): NodeRow {
  const { ingestSeq: _ignored, ...rest } = row;

  return rest;
}

function stripEdgeInternal(row: EdgeAppend & { id?: string }): EdgeRow {
  const { ingestSeq: _ignored, id: _alsoIgnored, ...rest } = row;

  return rest;
}

export type RepointResult =
  | {
      ok: true;
      closed: { recordId: number; contentHash: string; ingestedAt: string };
      opened: { recordId: number; contentHash: string; ingestedAt: string };
      closedValidTo: string;
    }
  | { ok: false; status: 403 | 404 | 422; message: string };

/**
 * Closes the current window on an edge and opens a new one, as one operation.
 *
 * The close is an *append*, not an update: the prior edge is re-appended with
 * the same logical key and the same `valid_from`, carrying a shorter
 * `valid_to`. It therefore lands in the same resolution group as the row it
 * supersedes — group by `(logical edge, valid_from)`, latest append wins — and
 * the open row drops out of current reads with its history intact.
 *
 * Both halves stay at `processing_version` 0. A valid-time change is not a
 * correction, and versioning it as one would make the next ordinary append for
 * that window silently lose (resolution orders by processing_version first).
 */
export function repointEdge(body: Record<string, unknown>): RepointResult {
  const relType = str(body['rel_type'], '');
  const srcId = str(body['src_id'], '');
  const currentDstId = str(body['dst_id'], '');
  const newDstId = str(body['new_dst_id'], '');
  const effectiveFrom = str(body['effective_from'], today());
  const edgeSeq = typeof body['edge_seq'] === 'number' ? body['edge_seq'] : 1;
  const reasonCode = str(body['change_reason_code'], '');

  if (
    REQUIRES_APPROVAL.has(reasonCode) &&
    typeof body['approved_by'] !== 'string'
  ) {
    return {
      ok: false,
      status: 422,
      message: `${reasonCode} requires approved_by`,
    };
  }

  const current = listEdges({ srcId, relType }).find(
    (e) => e.dst_id === currentDstId && e.edge_seq === edgeSeq,
  );

  if (current === undefined) {
    return {
      ok: false,
      status: 404,
      message: `no current ${relType} edge from ${srcId} to ${currentDstId}`,
    };
  }

  // A window cannot end before it began, and ending it on its own start date
  // would make it a tombstone rather than a closed window (GQ-06).
  if (effectiveFrom <= current.valid_from) {
    return {
      ok: false,
      status: 422,
      message: `the new window must start after ${current.valid_from}, where the current one does`,
    };
  }

  if (getNode(newDstId) === undefined) {
    return {
      ok: false,
      status: 422,
      message: `new_dst_id ${newDstId} is not a current node (GQ-11)`,
    };
  }

  const provenance = {
    change_reason_code: reasonCode,
    change_reason: str(body['change_reason'], ''),
    ...(typeof body['approved_by'] === 'string' && {
      approved_by: body['approved_by'],
    }),
  };

  const closed = appendEdge({
    rel_type: relType,
    src_id: srcId,
    dst_id: currentDstId,
    edge_seq: edgeSeq,
    valid_from: current.valid_from,
    valid_to: effectiveFrom,
    rel_weight: current.rel_weight,
    weight_basis: current.weight_basis,
    payload: current.payload,
    ...provenance,
  });

  const opened = appendEdge({
    rel_type: relType,
    src_id: srcId,
    dst_id: newDstId,
    edge_seq: edgeSeq,
    valid_from: effectiveFrom,
    valid_to: OPEN,
    rel_weight: current.rel_weight,
    weight_basis: current.weight_basis,
    payload: current.payload,
    ...provenance,
  });

  // Both are validated above, so a failure here is a bug rather than a user
  // error — and a half-applied re-point is exactly what this endpoint exists to
  // prevent, so it is reported rather than left in place.
  if (!closed.ok) {
    return { ok: false, status: 422, message: closed.message };
  }

  if (!opened.ok) {
    return { ok: false, status: 422, message: opened.message };
  }

  return {
    ok: true,
    closed: {
      recordId: closed.recordId,
      contentHash: closed.contentHash,
      ingestedAt: closed.ingestedAt,
    },
    opened: {
      recordId: opened.recordId,
      contentHash: opened.contentHash,
      ingestedAt: opened.ingestedAt,
    },
    closedValidTo: effectiveFrom,
  };
}
