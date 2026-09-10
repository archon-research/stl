import * as z from 'zod';

import { anyNodeId, nodeId, ui, validDate } from './primitives.ts';
import { checkProvenance, provenanceInput } from './provenance.ts';
import type { RelType, WeightBasis } from './vocabularies.ts';

/**
 * Composite workflows: one screen, several appends.
 *
 * The generated forms cover "append a row to one store". Real curation does not
 * decompose that way, and the classification worksheet is the proof: one of its
 * rows is a SECURITY node plus a BELONGS_TO edge per classification level plus
 * an ISSUED_BY edge plus a HAS_UNDERLYING edge — five appends across two stores,
 * which a curator thinks of as *one* decision about one instrument.
 *
 * So a workflow is its own schema over the *decision*, and it fans out to
 * appends on submit. Two consequences that matter:
 *
 * 1. **It is not atomic here.** The contract has no batch endpoint, so the
 *    workflow issues appends in sequence and reports which one failed. That is
 *    survivable because every append is independently valid and the store is
 *    append-only — a half-applied classification is an under-curated node, which
 *    `node_validity` already models, not a corrupt one. It is still the wrong
 *    long-run answer, and a transactional endpoint is the fix. Named here so it
 *    is a decision rather than an omission.
 * 2. **The narrowing is dynamic.** `security_type` is legal only under the
 *    chosen `asset_class`, so its picker's scope depends on a sibling field's
 *    value — something static `ui()` metadata cannot express. That is what
 *    `SchemaForm`'s per-field override slot is for, and this schema is the case
 *    that justified having one.
 */

/** Classify an existing security: the worksheet row, as a decision. */
export const classifySecurity = z
  .object({
    security_id: ui(nodeId('SECURITY'), {
      widget: 'reference',
      label: 'Security',
      help: 'The instrument being classified',
      group: 'Instrument',
      order: 1,
      targetKinds: ['SECURITY'],
    }),
    asset_class: ui(anyNodeId, {
      widget: 'reference',
      label: 'Asset class',
      help: 'Level 1. Choosing it narrows the type picker below',
      group: 'Classification',
      order: 1,
      targetKinds: ['CONCEPT'],
      conceptClass: 'instrument_type',
      narrowerThan: 'concept-instrument_type',
    }),
    security_type: ui(anyNodeId.optional(), {
      widget: 'reference',
      label: 'Security type',
      help: 'Level 2, scoped to the chosen asset class',
      group: 'Classification',
      order: 2,
      targetKinds: ['CONCEPT'],
      conceptClass: 'instrument_type',
    }),
    security_subtype: ui(anyNodeId.optional(), {
      widget: 'reference',
      label: 'Subtype',
      help: 'Level 3. Five of the worksheet’s nine subtype values have no concept node yet',
      group: 'Classification',
      order: 3,
      targetKinds: ['CONCEPT'],
      conceptClass: 'instrument_subtype',
    }),
    issuer_entity_id: ui(anyNodeId.optional(), {
      widget: 'reference',
      label: 'Issuer',
      help: 'Leave blank for an issuer-less wrapper; the row stores and is flagged',
      group: 'Relationships',
      order: 1,
      targetKinds: ['ENTITY'],
    }),
    underlying_security_id: ui(anyNodeId.optional(), {
      widget: 'reference',
      label: 'Underlying',
      help: 'The look-through spine. A fiat-backed stablecoin has none by design',
      group: 'Relationships',
      order: 2,
      targetKinds: ['SECURITY'],
    }),
    valid_from: ui(validDate, {
      label: 'Valid from',
      group: 'Lifecycle',
      order: 1,
    }),
    ...provenanceInput.shape,
  })
  .check((ctx) => {
    const v = ctx.value;
    checkProvenance(v, ctx);

    // A subtype without a type is a level-3 membership hanging off nothing:
    // BELONGS_TO is `1_per_class` and the taxonomy is a chain, so skipping a
    // level leaves the closure unable to place the node.
    if (v.security_subtype !== undefined && v.security_type === undefined) {
      ctx.issues.push({
        code: 'custom',
        path: ['security_type'],
        message:
          'a subtype needs its type: the taxonomy is walked level by level',
        input: v.security_type,
      });
    }

    if (v.underlying_security_id === v.security_id) {
      ctx.issues.push({
        code: 'custom',
        path: ['underlying_security_id'],
        message: 'a security cannot be its own underlying',
        input: v.underlying_security_id,
      });
    }
  });

/** One append the workflow will issue, in order. */
export type PlannedAppend = {
  label: string;
  relType: RelType;
  srcId: string;
  dstId: string;
  weight?: string;
  weightBasis?: WeightBasis;
};

/**
 * What the plan can be computed from.
 *
 * Deliberately looser than `ClassifySecurity`: the plan has to be visible while
 * the form is still invalid. Requiring a fully parsed value meant the preview
 * stayed empty until the provenance block was filled in — so a curator picked
 * three classifications and was told there was nothing to append, which is the
 * opposite of what the preview is for. The classification fields are enough to
 * describe the plan; the provenance is what makes it *submittable*.
 */
export type PlannableClassification = {
  security_id?: string | undefined;
  asset_class?: string | undefined;
  security_type?: string | undefined;
  security_subtype?: string | undefined;
  issuer_entity_id?: string | undefined;
  underlying_security_id?: string | undefined;
};

/**
 * The appends a classification decision becomes.
 *
 * Kept as a pure function so the screen can *show* the plan before running it.
 * That preview is the mitigation for the non-atomicity above: a curator who can
 * see five appends listed understands what a partial failure would leave behind,
 * where a single "Save" button would imply a transaction that does not exist.
 */
export function planClassification(
  value: PlannableClassification,
): PlannedAppend[] {
  const planned: PlannedAppend[] = [];

  if (value.security_id === undefined || value.security_id === '') {
    return planned;
  }

  for (const [level, conceptId] of [
    ['asset class', value.asset_class],
    ['type', value.security_type],
    ['subtype', value.security_subtype],
  ] as const) {
    if (conceptId !== undefined) {
      planned.push({
        label: `BELONGS_TO — ${level}`,
        relType: 'BELONGS_TO',
        srcId: value.security_id,
        dstId: conceptId,
      });
    }
  }

  if (value.issuer_entity_id !== undefined) {
    planned.push({
      label: 'ISSUED_BY — issuer',
      relType: 'ISSUED_BY',
      srcId: value.security_id,
      dstId: value.issuer_entity_id,
    });
  }

  if (value.underlying_security_id !== undefined) {
    planned.push({
      label: 'HAS_UNDERLYING — look-through',
      relType: 'HAS_UNDERLYING',
      srcId: value.security_id,
      dstId: value.underlying_security_id,
      // A single underlying takes the whole value. A basket would need one edge
      // per leg with weights summing to 1 under VALUE, which is a different
      // screen and is not this one.
      weight: '1.000000000000000000',
      weightBasis: 'VALUE',
    });
  }

  return planned;
}
