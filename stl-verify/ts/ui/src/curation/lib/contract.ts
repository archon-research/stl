import type { ShapeSeverity } from '../schema/shapes.ts';
import type { RecordType } from '../schema/vocabularies.ts';

/**
 * The **proposed** secstore API, hand-authored in the shape
 * `openapi-typescript` emits.
 *
 * There is no Python endpoint behind any of this yet, which is the point: the
 * spike works backwards from the contract so the client can be built while the
 * service is being designed, and so the endpoint list is a reviewable artefact
 * rather than an implication of some component's fetch call.
 *
 * Written this way — rather than as loose fetch wrappers — because
 * `createMockApi<paths>` and `createApiClient<paths>` both read their whole
 * surface off this type. So the mock, the client and the query keys all agree by
 * construction, and when `make -C python export-openapi-schema` finally emits
 * the real document, swapping the generated `paths` in here turns every
 * divergence into a compile error instead of a runtime 404. That is the same
 * discipline `mocks/README.md` describes, applied one step before the API
 * exists.
 *
 * The request bodies are `z.infer` of the write schemas, so the contract cannot
 * drift from the forms either. The engine-assigned columns are absent
 * throughout: `ingest_xid`, `content_hash`, `record_id` and the generated
 * `edge_id` are the append guard's to compute, and a client that offers them is
 * a client whose writes get rejected.
 */

/** A resolved node row, as `sec_node_current` returns it. */
export type NodeRow = {
  id: string;
  record_type: RecordType;
  chain_id: number | null;
  status: string;
  attrs: Record<string, unknown>;
  valid_from: string;
  valid_to: string;
  record_id: number;
  processing_version: number;
  ingested_at: string;
  actor: string;
  change_reason_code: string;
  change_reason: string;
  approved_by: string | null;
  supersedes_record_id: number | null;
  source_system: string;
  /** Hex rendering of the `bytea`; the chain is verified server-side. */
  content_hash: string;
};

/** A resolved edge row, as `sec_edge_current` returns it. */
export type EdgeRow = {
  edge_id: string;
  edge_seq: number;
  src_id: string;
  src_kind: RecordType;
  dst_id: string;
  dst_kind: RecordType;
  rel_type: string;
  rel_weight: string | null;
  weight_basis: string | null;
  weight_asof_block: number | null;
  payload: Record<string, unknown>;
  valid_from: string;
  valid_to: string;
  record_id: number;
  processing_version: number;
  ingested_at: string;
  actor: string;
  change_reason_code: string;
  change_reason: string;
  approved_by: string | null;
  supersedes_record_id: number | null;
  source_system: string;
  content_hash: string;
};

/**
 * A row of `node_validity`: one unmet shape obligation.
 *
 * This is the stewardship worklist, and it is a *read* — the store holds the
 * flagged row and reports the gap, rather than refusing the write. A UI that
 * only ever showed a create form would never surface it, which is why the
 * registry gives every resource a worklist view alongside its list.
 */
export type NodeValidityRow = {
  node_id: string;
  record_type: RecordType;
  shape_id: string;
  severity: ShapeSeverity;
  target: string;
  kind: 'field' | 'edge';
  message: string;
};

/** What an append returns: the stored row's version identity. */
type AppendReceipt = {
  record_id: number;
  processing_version: number;
  content_hash: string;
  ingested_at: string;
};

/**
 * What a re-point returns: both halves of the pair.
 *
 * Two receipts rather than one, because two rows were written and a manifest
 * may cite either — the closed window is as much a fact as the opened one.
 */
type RepointReceipt = {
  closed: AppendReceipt;
  opened: AppendReceipt;
  /** The window the prior edge now ends at, echoed so the UI can show it. */
  closed_valid_to: string;
};

/** FastAPI's error envelope, which the existing client already understands. */
export type Problem = {
  detail: string | { loc: (string | number)[]; msg: string; type: string }[];
};

/**
 * Every append body, at the wire level.
 *
 * Uniform on purpose, and it is a real trade rather than laziness. The precise
 * shapes — `EdgeWrite`, `InstrumentRegisterWrite`, `AliasRegisterWrite` — are
 * where they belong: on the zod schemas that generate and validate the forms,
 * and a call site that knows its resource still annotates with them (see
 * `ClassifyView`, which builds an `EdgeWrite`).
 *
 * What a *per-path* body type bought was nothing, and it cost the generic create
 * view its types: that view picks its endpoint from a registry row at runtime, so
 * the path is a union of four literals and the body a union of four shapes, which
 * no amount of inference resolves — it forced an `as never` on the one call that
 * every resource goes through. Uniform here, precise at the schema, and the
 * server validates either way.
 */
type WriteBody = Record<string, unknown>;

type Json<T> = {
  headers: { [name: string]: unknown };
  content: { 'application/json': T };
};

type NoParams = {
  query?: never;
  header?: never;
  path?: never;
  cookie?: never;
};

export interface paths {
  /**
   * The node read. `record_type` scopes it to a kind, which is the index
   * `sec_node_type_idx` exists for; `q` is a substring match over id and label.
   */
  '/v1/secstore/nodes': {
    parameters: NoParams;
    get: {
      parameters: {
        query?: {
          record_type?: RecordType;
          q?: string;
          /** Scopes CONCEPT reads to one class, for a classification picker. */
          concept_class?: string;
          /**
           * Restricts to concepts under this ancestor, walked through
           * NARROWER_THAN. This is what narrows a 152-value security-type picker
           * to the handful legal under the chosen asset class.
           */
          narrower_than?: string;
          status?: string;
          limit?: number;
          /**
           * Valid-time as-of date. Absent means the operational current read;
           * anything a calculation depends on passes it explicitly, because
           * `_current` is never what a reproducible read uses.
           */
          as_of?: string;
        };
        header?: never;
        path?: never;
        cookie?: never;
      };
      requestBody?: never;
      responses: { 200: Json<NodeRow[]>; 422: Json<Problem> };
    };
    /** Appends a node version. Never an update: the store is append-only. */
    post: {
      parameters: NoParams;
      requestBody: { content: { 'application/json': WriteBody } };
      responses: {
        201: Json<AppendReceipt>;
        403: Json<Problem>;
        422: Json<Problem>;
      };
    };
    put?: never;
    delete?: never;
    options?: never;
    head?: never;
    patch?: never;
    trace?: never;
  };

  '/v1/secstore/nodes/{node_id}': {
    parameters: NoParams;
    get: {
      parameters: {
        query?: { as_of?: string };
        header?: never;
        path: { node_id: string };
        cookie?: never;
      };
      requestBody?: never;
      responses: { 200: Json<NodeRow>; 404: Json<Problem>; 422: Json<Problem> };
    };
    put?: never;
    post?: never;
    delete?: never;
    options?: never;
    head?: never;
    patch?: never;
    trace?: never;
  };

  /**
   * Every append for one node, newest first — the correction chain and the
   * valid-time windows. A bitemporal store without a history view is a store
   * whose second clock is invisible.
   */
  '/v1/secstore/nodes/{node_id}/history': {
    parameters: NoParams;
    get: {
      parameters: {
        query?: never;
        header?: never;
        path: { node_id: string };
        cookie?: never;
      };
      requestBody?: never;
      responses: { 200: Json<NodeRow[]>; 404: Json<Problem> };
    };
    put?: never;
    post?: never;
    delete?: never;
    options?: never;
    head?: never;
    patch?: never;
    trace?: never;
  };

  '/v1/secstore/edges': {
    parameters: NoParams;
    get: {
      parameters: {
        query?: {
          src_id?: string;
          dst_id?: string;
          rel_type?: string;
          as_of?: string;
          limit?: number;
        };
        header?: never;
        path?: never;
        cookie?: never;
      };
      requestBody?: never;
      responses: { 200: Json<EdgeRow[]>; 422: Json<Problem> };
    };
    post: {
      parameters: NoParams;
      requestBody: { content: { 'application/json': WriteBody } };
      responses: {
        201: Json<AppendReceipt>;
        403: Json<Problem>;
        422: Json<Problem>;
      };
    };
    put?: never;
    delete?: never;
    options?: never;
    head?: never;
    patch?: never;
    trace?: never;
  };

  /**
   * Re-point an edge: close the current window and open a new one, atomically.
   *
   * This exists because the pair is not safely a client's to compose. A
   * re-classification is two appends — the prior edge re-appended with a
   * shorter `valid_to`, and the new target opened from the same date — and
   * issuing them separately can leave a node with no classification (close
   * lands, open fails) or with two (open lands, close fails). The engine will
   * not catch the second: single-valued cardinality is a DQ check over current
   * state, never a write trigger, because an open edge always time-overlaps
   * its own replacement.
   *
   * The server also already knows the thing the client would otherwise have to
   * fetch first — the current row's `valid_from`, which the close append has to
   * reuse to land in the same resolution group.
   *
   * Both appends stay at `processing_version` 0: a valid-time change is not a
   * correction. Correcting a wrongly *recorded* classification is a different
   * operation (a restatement at version N with `supersedes_record_id`), and
   * withdrawing one that should never have existed is a third (a zero-length
   * tombstone window).
   */
  '/v1/secstore/edges/repoint': {
    parameters: NoParams;
    post: {
      parameters: NoParams;
      requestBody: { content: { 'application/json': WriteBody } };
      responses: {
        201: Json<RepointReceipt>;
        403: Json<Problem>;
        404: Json<Problem>;
        422: Json<Problem>;
      };
    };
    get?: never;
    put?: never;
    delete?: never;
    options?: never;
    head?: never;
    patch?: never;
    trace?: never;
  };

  /** The stewardship worklist. */
  '/v1/secstore/node-validity': {
    parameters: NoParams;
    get: {
      parameters: {
        query?: {
          record_type?: RecordType;
          severity?: ShapeSeverity;
          limit?: number;
        };
        header?: never;
        path?: never;
        cookie?: never;
      };
      requestBody?: never;
      responses: { 200: Json<NodeValidityRow[]>; 422: Json<Problem> };
    };
    put?: never;
    post?: never;
    delete?: never;
    options?: never;
    head?: never;
    patch?: never;
    trace?: never;
  };

  '/v1/secstore/registers/instrument': {
    parameters: NoParams;
    get: {
      parameters: {
        query?: { q?: string; security_id?: string; limit?: number };
        header?: never;
        path?: never;
        cookie?: never;
      };
      requestBody?: never;
      responses: {
        200: Json<
          {
            instrument_key: string;
            key_namespace: string;
            security_id: string;
            chain_id: number | null;
            valid_from: string;
            processing_version: number;
          }[]
        >;
        422: Json<Problem>;
      };
    };
    post: {
      parameters: NoParams;
      requestBody: { content: { 'application/json': WriteBody } };
      responses: {
        201: Json<AppendReceipt>;
        409: Json<Problem>;
        422: Json<Problem>;
      };
    };
    put?: never;
    delete?: never;
    options?: never;
    head?: never;
    patch?: never;
    trace?: never;
  };

  '/v1/secstore/registers/alias': {
    parameters: NoParams;
    get: {
      parameters: {
        query?: {
          q?: string;
          id_scheme?: string;
          node_id?: string;
          limit?: number;
        };
        header?: never;
        path?: never;
        cookie?: never;
      };
      requestBody?: never;
      responses: {
        200: Json<
          {
            id_scheme: string;
            id_value: string;
            node_id: string;
            valid_from: string;
            valid_to: string;
          }[]
        >;
        422: Json<Problem>;
      };
    };
    post: {
      parameters: NoParams;
      requestBody: { content: { 'application/json': WriteBody } };
      responses: {
        201: Json<AppendReceipt>;
        409: Json<Problem>;
        422: Json<Problem>;
      };
    };
    put?: never;
    delete?: never;
    options?: never;
    head?: never;
    patch?: never;
    trace?: never;
  };
}
