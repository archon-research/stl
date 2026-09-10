import * as z from 'zod';

import {
  anyNodeId,
  exactDecimal,
  ui,
  validDate,
  validToDate,
} from './primitives.ts';
import { checkProvenance, provenanceInput } from './provenance.ts';
import {
  ID_PREFIX,
  REL_TYPE_CODES,
  RECORD_TYPES,
  relTypeSpec,
  WEIGHT_BASES,
} from './vocabularies.ts';

/**
 * The relationship store as a write schema.
 *
 * One form serves all 13 ratified types, because `rel_type_vocabulary` already
 * says everything a form needs to configure itself: which kinds each endpoint
 * may be, whether the type is weighted and under which basis, and what
 * cardinality to warn about. Thirteen hand-written edge forms would be thirteen
 * copies of the same five fields differing only in the vocabulary row they
 * hard-code — and each would go stale on its own schedule as draft types ratify.
 *
 * The type-dependent parts are enforced in `.check()` rather than by building a
 * discriminated union over `rel_type`. A union would give a sharper static type,
 * but it would also mean the schema shape changes as the user picks a type,
 * which is precisely what a single long-lived form state cannot survive: every
 * field would remount and the half-filled endpoint would be discarded.
 */

/** Per-type payload clusters, for the types whose payload ADR-0007 names. */
const EDGE_PAYLOADS = {
  SPLIT_FROM: z.object({
    ratio: ui(exactDecimal, {
      label: 'Ratio',
      help: 'New units per old unit; a reverse split is below 1',
    }),
    ex_date: ui(validDate, { label: 'Ex date' }),
  }),
  SUCCEEDED_BY: z.object({
    ratio: ui(exactDecimal, {
      label: 'Ratio',
      help: 'Successor units per predecessor unit — MKR to SKY was 1:24000',
    }),
    event_date: ui(validDate, { label: 'Event date' }),
  }),
} as const;

export type PayloadRelType = keyof typeof EDGE_PAYLOADS;

function hasPayloadSchema(relType: string): relType is PayloadRelType {
  return relType in EDGE_PAYLOADS;
}

/** Which kind an id claims to be, read off its governed prefix. */
export function kindOf(id: string): (typeof RECORD_TYPES)[number] | undefined {
  return RECORD_TYPES.find((kind) => id.startsWith(ID_PREFIX[kind]));
}

export const edgeWrite = z
  .object({
    rel_type: ui(z.enum(REL_TYPE_CODES), {
      label: 'Relationship',
      help: 'Picking a type narrows the endpoints and decides whether a weight applies',
      group: 'Relationship',
      order: 1,
    }),
    src_id: ui(anyNodeId, {
      widget: 'reference',
      label: 'Source',
      help: 'The node the relationship points from',
      group: 'Relationship',
      order: 2,
    }),
    dst_id: ui(anyNodeId, {
      widget: 'reference',
      label: 'Destination',
      help: 'The node the relationship points to',
      group: 'Relationship',
      order: 3,
    }),
    edge_seq: ui(z.int().min(1).default(1), {
      label: 'Sequence',
      help: 'Base 1. Raise it only to let a deliberately duplicated edge coexist with its twin',
      group: 'Relationship',
      order: 4,
    }),
    rel_weight: ui(exactDecimal.optional(), {
      widget: 'decimal',
      label: 'Weight',
      help: 'Exact decimal. Look-through multiplies these along a path, so it is never a float',
      group: 'Weight',
      order: 1,
    }),
    weight_basis: ui(z.enum(WEIGHT_BASES).optional(), {
      label: 'Weight basis',
      help: 'Fixed by the relationship type; shown for confirmation',
      group: 'Weight',
      order: 2,
    }),
    weight_asof_block: ui(z.int().positive().optional(), {
      label: 'Weight as-of block',
      help: 'Only for a market-derived weight; a curated weight leaves this blank',
      group: 'Weight',
      order: 3,
    }),
    valid_from: ui(validDate, {
      label: 'Valid from',
      group: 'Lifecycle',
      order: 1,
    }),
    valid_to: ui(validToDate.default('infinity'), {
      label: 'Valid to',
      help: 'Exclusive. Leave open unless closing the window; equal to valid_from is a tombstone',
      group: 'Lifecycle',
      order: 2,
    }),
    payload: ui(z.record(z.string(), z.unknown()).default({}), {
      widget: 'json',
      label: 'Payload',
      help: 'Type-specific cluster: a ratio and its date, a rating and its agency',
      group: 'Payload',
      order: 1,
    }),
    ...provenanceInput.shape,
  })
  .check((ctx) => {
    const v = ctx.value;
    checkProvenance(v, ctx);

    const spec = relTypeSpec(v.rel_type);
    if (spec === undefined) {
      // Unreachable while `rel_type` is the enum, and left in so that widening
      // the field later fails loudly here rather than skipping every check
      // below.
      ctx.issues.push({
        code: 'custom',
        path: ['rel_type'],
        message: `${v.rel_type} is not a ratified relationship type`,
        input: v.rel_type,
      });

      return;
    }

    // Endpoint kind legality: the (rel_type, src_kind, dst_kind) triple. The
    // database re-checks that a declared kind agrees with its id prefix, but the
    // triple itself is cross-row and validator-owned — so catching it here is
    // the difference between an inline error and a rejected append.
    const srcKind = kindOf(v.src_id);
    if (
      srcKind !== undefined &&
      !(spec.srcKinds as readonly string[]).includes(srcKind)
    ) {
      ctx.issues.push({
        code: 'custom',
        path: ['src_id'],
        message: `${v.rel_type} takes ${spec.srcKinds.join(' or ')} as its source, not ${srcKind}`,
        input: v.src_id,
      });
    }

    const dstKind = kindOf(v.dst_id);
    if (
      dstKind !== undefined &&
      !(spec.dstKinds as readonly string[]).includes(dstKind)
    ) {
      ctx.issues.push({
        code: 'custom',
        path: ['dst_id'],
        message: `${v.rel_type} takes ${spec.dstKinds.join(' or ')} as its destination, not ${dstKind}`,
        input: v.dst_id,
      });
    }

    // A self-edge is a cycle of length one. The look-through walk has a cycle
    // guard, but a NARROWER_THAN self-edge would also make a concept its own
    // shape ancestor, which fails at authoring rather than at load.
    if (v.src_id === v.dst_id) {
      ctx.issues.push({
        code: 'custom',
        path: ['dst_id'],
        message: 'an edge from a node to itself is a cycle',
        input: v.dst_id,
      });
    }

    // `sec_edge_weight_basis_chk`: a weight without a basis is unsummable, so
    // the pair is enforced by the engine. The converse — a basis on an
    // unweighted type — is not a CHECK but is still wrong, and it is the more
    // likely mistake now the basis is prefilled from the vocabulary.
    if (v.rel_weight !== undefined && v.weight_basis === undefined) {
      ctx.issues.push({
        code: 'custom',
        path: ['weight_basis'],
        message: 'a weight must declare its basis',
        input: v.weight_basis,
      });
    }

    if (spec.weightBasis === null && v.rel_weight !== undefined) {
      ctx.issues.push({
        code: 'custom',
        path: ['rel_weight'],
        message: `${v.rel_type} is unweighted`,
        input: v.rel_weight,
      });
    }

    if (
      spec.weightBasis !== null &&
      v.weight_basis !== undefined &&
      v.weight_basis !== spec.weightBasis
    ) {
      ctx.issues.push({
        code: 'custom',
        path: ['weight_basis'],
        message: `${v.rel_type} declares ${spec.weightBasis}`,
        input: v.weight_basis,
      });
    }

    // Half-open window. Equality is legal and load-bearing — a zero-length
    // window is the only way to retract an edge, since an edge has no status to
    // retire it — so it is admitted here and paired with its reason code.
    if (v.valid_to !== 'infinity' && v.valid_to < v.valid_from) {
      ctx.issues.push({
        code: 'custom',
        path: ['valid_to'],
        message: 'the window end cannot precede its start',
        input: v.valid_to,
      });
    }

    if (
      v.valid_to !== 'infinity' &&
      v.valid_to === v.valid_from &&
      v.change_reason_code !== 'RETRACTION'
    ) {
      ctx.issues.push({
        code: 'custom',
        path: ['valid_to'],
        message:
          'a zero-length window is a tombstone; pair it with the RETRACTION reason code',
        input: v.valid_to,
      });
    }

    // Only ALLOCATES is derived-only today and it is not seeded, so this guards
    // a type that does not exist yet on purpose: it is the check that has to be
    // in place before the first derived type ratifies, not after.
    if (v.weight_asof_block !== undefined && v.rel_weight === undefined) {
      ctx.issues.push({
        code: 'custom',
        path: ['weight_asof_block'],
        message: 'a block stamp without a weight stamps nothing',
        input: v.weight_asof_block,
      });
    }

    // Payload shape, for the types whose cluster ADR-0007 fixes. Unknown keys
    // are allowed through: the payload is deliberately open, and a curator
    // recording something the model has not named yet should not be blocked.
    if (hasPayloadSchema(v.rel_type)) {
      const parsed = EDGE_PAYLOADS[v.rel_type].safeParse(v.payload);
      if (!parsed.success) {
        for (const issue of parsed.error.issues) {
          ctx.issues.push({
            code: 'custom',
            path: ['payload', ...issue.path],
            message: issue.message,
            input: v.payload,
          });
        }
      }
    }
  });

export type EdgeWrite = z.infer<typeof edgeWrite>;
