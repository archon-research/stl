import { createMockApi, setupMocks } from '@archon-research/http-client-msw';

import type { NodeValidityRow, Problem } from '../lib/contract.ts';
import type { paths } from '../lib/contract.ts';
import { kindOf } from '../schema/edges.ts';
import { evaluateShapes } from '../schema/shapes.ts';
import type { RecordType } from '../schema/vocabularies.ts';
import { appendPrices, listPrices, resetPrices } from './price-store.ts';
import {
  appendEdge,
  appendNode,
  repointEdge,
  conceptClosureFor,
  edgesOut,
  getNode,
  listEdges,
  listNodes,
  nodeHistory,
  resetStore,
} from './store.ts';

/**
 * The offline secstore API.
 *
 * Typed against the hand-authored `paths`, so a handler on a path the contract
 * does not declare — or a body the operation does not allow — fails to compile.
 * That is the same guarantee `@stl-verify/mocks` gets from the generated
 * document, and it is worth having a wave early: the contract is the artefact
 * under review here, and a mock free to drift from it would be reviewing
 * nothing.
 *
 * Kept in the ui workspace rather than in `@stl-verify/mocks` deliberately. That
 * workspace's handlers are bound to the *real* generated `paths` and its
 * `getMockHandlers()` is documented as "every endpoint `queries.ts` calls";
 * putting speculative endpoints beside them would make it unclear which
 * contracts exist. When the Python endpoints land and the OpenAPI document
 * carries them, these move over and the hand-authored type is deleted.
 */

const mock = createMockApi<{ [P in keyof paths]: paths[P] }>();

function problem(message: string): Problem {
  return { detail: message };
}

/**
 * Reads a string off an append body.
 *
 * The wire bodies are `Record<string, unknown>` (see `contract.ts`), so a
 * handler narrows what it needs. A missing field becomes `''`, which then fails
 * the register's own existence checks below — the same outcome the validator
 * would produce, rather than a stored row containing `undefined`.
 */
function str(value: unknown, fallback = ''): string {
  return typeof value === 'string' ? value : fallback;
}

/**
 * Which kind a node append is for.
 *
 * The contract has one `POST /nodes` rather than one per kind, so the kind comes
 * off the body's own id prefix — which is exactly how the database derives
 * `record_type` in `sec_node_id_prefix_chk`. Deriving it the same way here means
 * a mismatched prefix is rejected in the same place for the same reason.
 */
function recordTypeOf(body: Record<string, unknown>): RecordType | undefined {
  const id = body['id'];
  if (typeof id !== 'string') {
    return undefined;
  }

  const declared = body['record_type'];
  if (isRecordType(declared)) {
    return declared;
  }

  return kindOf(id);
}

function isRecordType(value: unknown): value is RecordType {
  return (
    value === 'SECURITY' ||
    value === 'ENTITY' ||
    value === 'CONCEPT' ||
    value === 'SOURCE' ||
    value === 'ACCOUNT'
  );
}

/** The registers, which wave 1 defers — held as plain collections. */
const instrumentRegister: {
  instrument_key: string;
  key_namespace: string;
  security_id: string;
  chain_id: number | null;
  valid_from: string;
  processing_version: number;
}[] = [
  {
    instrument_key: 'evm:1:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
    key_namespace: 'evm_contract',
    security_id: 'sec-usdc',
    chain_id: 1,
    valid_from: '2026-08-26',
    processing_version: 0,
  },
  {
    instrument_key: 'evm:1:0xdac17f958d2ee523a2206206994597c13d831ec7',
    key_namespace: 'evm_contract',
    security_id: 'sec-usdt',
    chain_id: 1,
    valid_from: '2026-08-26',
    processing_version: 0,
  },
];

const aliasRegister: {
  id_scheme: string;
  id_value: string;
  node_id: string;
  valid_from: string;
  valid_to: string;
}[] = [
  {
    id_scheme: 'LEI',
    id_value: '549300UHJLR6LBGAFV55',
    node_id: 'em-issuer-circle',
    valid_from: '2026-08-26',
    valid_to: 'infinity',
  },
  {
    id_scheme: 'LEI',
    id_value: '254900OW87ICXIE4NO74',
    node_id: 'em-issuer-tether',
    valid_from: '2026-08-26',
    valid_to: 'infinity',
  },
];

const seededInstrumentRegister = [...instrumentRegister];
const seededAliasRegister = [...aliasRegister];

function resetRegisters(): void {
  instrumentRegister.length = 0;
  instrumentRegister.push(...seededInstrumentRegister);
  aliasRegister.length = 0;
  aliasRegister.push(...seededAliasRegister);
}

/**
 * `node_validity`, computed rather than stored.
 *
 * In production this is a pivot table refreshed on graph change. Computing it
 * here on every read is the honest mock: it means the worklist reflects an
 * append immediately, which is the behaviour a curator working the queue
 * expects, and it keeps the staleness question — which is a real operational
 * property of the pivot — out of a fixture that cannot model it.
 */
function computeValidity(
  recordType?: RecordType,
  severity?: string,
): NodeValidityRow[] {
  const rows: NodeValidityRow[] = [];
  const kinds: RecordType[] =
    recordType === undefined
      ? ['SECURITY', 'ENTITY', 'CONCEPT', 'SOURCE', 'ACCOUNT']
      : [recordType];

  for (const kind of kinds) {
    for (const node of listNodes({ recordType: kind, limit: 1000 })) {
      const gaps = evaluateShapes(
        kind,
        conceptClosureFor(node.id),
        node.attrs,
        edgesOut(node.id),
      );

      for (const gap of gaps) {
        if (severity !== undefined && gap.severity !== severity) {
          continue;
        }

        rows.push({
          node_id: node.id,
          record_type: kind,
          shape_id: gap.shapeId,
          severity: gap.severity,
          target: gap.target,
          kind: gap.kind,
          message: gap.message,
        });
      }
    }
  }

  return rows;
}

export const curationMocks = setupMocks(
  [
    mock.get('/v1/secstore/nodes', ({ query, response }) => {
      const recordType = query.get('record_type');
      const q = query.get('q');
      const conceptClass = query.get('concept_class');
      const narrowerThan = query.get('narrower_than');
      const status = query.get('status');
      const limit = query.get('limit');
      const asOf = query.get('as_of');

      return response(200).json(
        listNodes({
          ...(recordType !== null && { recordType: recordType as RecordType }),
          ...(q !== null && { q }),
          ...(conceptClass !== null && { conceptClass }),
          ...(narrowerThan !== null && { narrowerThan }),
          ...(status !== null && { status }),
          ...(limit !== null && { limit: Number(limit) }),
          ...(asOf !== null && { asOf }),
        }),
      );
    }),

    mock.post('/v1/secstore/nodes', async ({ request, response }) => {
      const body = await request.json();
      const recordType = recordTypeOf(body);

      if (recordType === undefined) {
        return response(422).json(
          problem(
            'id must carry a governed kind prefix: em- sec- concept- src- acct-',
          ),
        );
      }

      const result = appendNode(body, recordType);
      if (!result.ok) {
        return result.status === 403
          ? response(403).json(problem(result.message))
          : response(422).json(problem(result.message));
      }

      return response(201).json({
        record_id: result.recordId,
        processing_version: result.processingVersion,
        content_hash: result.contentHash,
        ingested_at: result.ingestedAt,
      });
    }),

    mock.get('/v1/secstore/nodes/{node_id}', ({ params, query, response }) => {
      const asOf = query.get('as_of');
      const node = getNode(params.node_id, asOf ?? undefined);

      return node === undefined
        ? response(404).json(problem(`no current node ${params.node_id}`))
        : response(200).json(node);
    }),

    mock.get('/v1/secstore/nodes/{node_id}/history', ({ params, response }) => {
      const history = nodeHistory(params.node_id);

      return history.length === 0
        ? response(404).json(problem(`no appends for ${params.node_id}`))
        : response(200).json(history);
    }),

    mock.get('/v1/secstore/edges', ({ query, response }) => {
      const srcId = query.get('src_id');
      const dstId = query.get('dst_id');
      const relType = query.get('rel_type');
      const asOf = query.get('as_of');
      const limit = query.get('limit');

      return response(200).json(
        listEdges({
          ...(srcId !== null && { srcId }),
          ...(dstId !== null && { dstId }),
          ...(relType !== null && { relType }),
          ...(asOf !== null && { asOf }),
          ...(limit !== null && { limit: Number(limit) }),
        }),
      );
    }),

    mock.post('/v1/secstore/edges', async ({ request, response }) => {
      const body = await request.json();
      const result = appendEdge(body);

      if (!result.ok) {
        return result.status === 403
          ? response(403).json(problem(result.message))
          : response(422).json(problem(result.message));
      }

      return response(201).json({
        record_id: result.recordId,
        processing_version: result.processingVersion,
        content_hash: result.contentHash,
        ingested_at: result.ingestedAt,
      });
    }),

    mock.post('/v1/secstore/edges/repoint', async ({ request, response }) => {
      const body = await request.json();
      const result = repointEdge(body);

      if (!result.ok) {
        switch (result.status) {
          case 403:
            return response(403).json(problem(result.message));
          case 404:
            return response(404).json(problem(result.message));
          default:
            return response(422).json(problem(result.message));
        }
      }

      return response(201).json({
        closed: {
          record_id: result.closed.recordId,
          processing_version: 0,
          content_hash: result.closed.contentHash,
          ingested_at: result.closed.ingestedAt,
        },
        opened: {
          record_id: result.opened.recordId,
          processing_version: 0,
          content_hash: result.opened.contentHash,
          ingested_at: result.opened.ingestedAt,
        },
        closed_valid_to: result.closedValidTo,
      });
    }),

    mock.get('/v1/prices', ({ query, response }) => {
      const chainId = query.get('chain_id');
      const tokenAddress = query.get('token_address');
      const limit = query.get('limit');

      return response(200).json(
        listPrices({
          ...(chainId !== null && { chainId: Number(chainId) }),
          ...(tokenAddress !== null && { tokenAddress }),
          ...(limit !== null && { limit: Number(limit) }),
        }),
      );
    }),

    mock.post('/v1/prices', async ({ request, response }) => {
      const body = await request.json();
      // The body is `Record<string, unknown>` at the wire, so the row list is
      // narrowed here rather than asserted.
      const raw = body['rows'];
      const rows = Array.isArray(raw)
        ? raw.filter(
            (row): row is Record<string, unknown> =>
              typeof row === 'object' && row !== null && !Array.isArray(row),
          )
        : [];
      const result = appendPrices(rows);

      return response(201).json({
        accepted: result.accepted,
        rejected: result.rejected,
        first_record_id: result.firstRecordId,
      });
    }),

    mock.get('/v1/secstore/node-validity', ({ query, response }) => {
      const recordType = query.get('record_type');
      const severity = query.get('severity');
      const limit = query.get('limit');
      const rows = computeValidity(
        recordType === null ? undefined : (recordType as RecordType),
        severity ?? undefined,
      );

      return response(200).json(
        rows.slice(0, limit === null ? 200 : Number(limit)),
      );
    }),

    mock.get('/v1/secstore/registers/instrument', ({ query, response }) => {
      const q = query.get('q');
      const securityId = query.get('security_id');
      let rows = [...instrumentRegister];

      if (securityId !== null) {
        rows = rows.filter((r) => r.security_id === securityId);
      }

      if (q !== null && q !== '') {
        const needle = q.toLowerCase();
        rows = rows.filter(
          (r) =>
            r.instrument_key.toLowerCase().includes(needle) ||
            r.security_id.toLowerCase().includes(needle),
        );
      }

      return response(200).json(rows);
    }),

    mock.post(
      '/v1/secstore/registers/instrument',
      async ({ request, response }) => {
        const raw = await request.json();
        const body = {
          instrument_key: str(raw['instrument_key']),
          key_namespace: str(raw['key_namespace']),
          security_id: str(raw['security_id']),
          chain_id:
            typeof raw['chain_id'] === 'number' ? raw['chain_id'] : null,
          valid_from: str(raw['valid_from']),
        };

        // The unique-current guarantee, which is the whole reason the register is
        // a table and not an edge type. Re-pointing a live key is a close-and-open
        // pair, not a second insert, so a bare duplicate is a conflict.
        if (
          instrumentRegister.some(
            (r) => r.instrument_key === body.instrument_key,
          )
        ) {
          return response(409).json(
            problem(
              `${body.instrument_key} already has a current mapping; re-point it instead of adding a second`,
            ),
          );
        }

        if (getNode(body.security_id) === undefined) {
          return response(422).json(
            problem(`security_id ${body.security_id} is not a current node`),
          );
        }

        instrumentRegister.push({
          instrument_key: body.instrument_key,
          key_namespace: body.key_namespace,
          security_id: body.security_id,
          chain_id: body.chain_id,
          valid_from: body.valid_from,
          processing_version: 0,
        });

        return response(201).json({
          record_id: instrumentRegister.length,
          processing_version: 0,
          content_hash: `mock:register:${body.instrument_key}`,
          ingested_at: new Date().toISOString(),
        });
      },
    ),

    mock.get('/v1/secstore/registers/alias', ({ query, response }) => {
      const q = query.get('q');
      const idScheme = query.get('id_scheme');
      const nodeId = query.get('node_id');
      let rows = [...aliasRegister];

      if (idScheme !== null) {
        rows = rows.filter((r) => r.id_scheme === idScheme);
      }

      if (nodeId !== null) {
        rows = rows.filter((r) => r.node_id === nodeId);
      }

      if (q !== null && q !== '') {
        const needle = q.toLowerCase();
        rows = rows.filter(
          (r) =>
            r.id_value.toLowerCase().includes(needle) ||
            r.node_id.toLowerCase().includes(needle),
        );
      }

      return response(200).json(rows);
    }),

    mock.post('/v1/secstore/registers/alias', async ({ request, response }) => {
      const raw = await request.json();
      const body = {
        id_scheme: str(raw['id_scheme']),
        id_value: str(raw['id_value']),
        node_id: str(raw['node_id']),
        valid_from: str(raw['valid_from']),
        valid_to: str(raw['valid_to'], 'infinity'),
      };

      // Per-scheme uniqueness. Two nodes claiming one LEI is the failure the
      // alias register exists to make impossible.
      const clash = aliasRegister.find(
        (r) =>
          r.id_scheme === body.id_scheme &&
          r.id_value === body.id_value &&
          r.node_id !== body.node_id &&
          r.valid_to === 'infinity',
      );

      if (clash !== undefined) {
        return response(409).json(
          problem(
            `${body.id_scheme} ${body.id_value} already resolves to ${clash.node_id}`,
          ),
        );
      }

      if (getNode(body.node_id) === undefined) {
        return response(422).json(
          problem(`node_id ${body.node_id} is not a current node`),
        );
      }

      aliasRegister.push({
        id_scheme: body.id_scheme,
        id_value: body.id_value,
        node_id: body.node_id,
        valid_from: body.valid_from,
        valid_to: body.valid_to,
      });

      return response(201).json({
        record_id: aliasRegister.length,
        processing_version: 0,
        content_hash: `mock:alias:${body.id_value}`,
        ingested_at: new Date().toISOString(),
      });
    }),
  ],
  { onReset: [resetStore, resetRegisters, resetPrices] },
);
