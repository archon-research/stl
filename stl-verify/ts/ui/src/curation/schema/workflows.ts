import * as z from 'zod';

import { anyNodeId, nodeId, ui, validDate } from './primitives.ts';
import { checkProvenance, provenanceInput } from './provenance.ts';
import type { RelType, WeightBasis } from './vocabularies.ts';

/**
 * Composite workflows: one screen, several appends.
 *
 * The generated forms cover "append a row to one store". Real curation does not
 * decompose that way, and the classification worksheet is the proof: one of its
 * rows is a SECURITY node plus its instrument-type membership plus a subtype
 * plus an issuer plus a look-through leg — several writes across two stores,
 * which a curator thinks of as *one* decision about one instrument.
 *
 * So a workflow is its own schema over the *decision*, and it fans out to
 * appends on submit. Two consequences that matter:
 *
 * 1. **Replacing a membership is one call; adding one is another.** A
 *    classification landing where none existed is a bare append; one replacing
 *    an existing membership is a re-point — close the old window, open the new —
 *    which the server does atomically because the halves are unsafe to compose
 *    client-side. The remaining fan-out is still sequential with no batch
 *    endpoint, survivable only because each write is independently valid and a
 *    half-classified node is under-curated rather than corrupt.
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

    // A subtype qualifies a type — FIAT_BACKED says what kind of STABLECOIN —
    // so one without the other places the node in a class whose meaning depends
    // on a membership it does not hold.
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

/**
 * One write the workflow will issue, in order.
 *
 * `open` is a bare append; `repoint` closes an existing window and opens the new
 * one in a single server call. Which of the two a classification becomes depends
 * entirely on whether the node already has a membership in that concept class.
 */
export type PlannedAppend = {
  label: string;
  relType: RelType;
  srcId: string;
  dstId: string;
  weight?: string;
  weightBasis?: WeightBasis;
  /** Present when this replaces a current edge: the target being closed. */
  replaces?: string;
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

/** A membership the node already holds, with the class it occupies. */
export type CurrentMembership = {
  conceptId: string;
  conceptClass: string;
};

/**
 * The writes a classification decision becomes.
 *
 * **One `BELONGS_TO` into the instrument-type taxonomy, not one per level.**
 * ADR-0007 is explicit: *"Asset class, type and subtype are not columns: they are
 * walked from one `BELONGS_TO` through `NARROWER_THAN`"*, and GQ-13 flags a node
 * holding more than one membership per concept class. So the asset-class picker
 * is a *narrowing control* for choosing the type — it scopes the subtree — and
 * only the most specific choice becomes an edge. The class above it is derived
 * by walking up, and storing it too would be the second, staler answer the ADR
 * warns about.
 *
 * The subtype is a separate concept class (`instrument_subtype`), so it is a
 * second edge rather than a third level of the same one.
 *
 * Kept pure so the screen can *show* the plan before running it — which is what
 * makes the difference between an open and a re-point visible before it happens.
 */
export function planClassification(
  value: PlannableClassification,
  current: readonly CurrentMembership[] = [],
): PlannedAppend[] {
  const planned: PlannedAppend[] = [];

  // Bound once so the closure below keeps the narrowing.
  const srcId = value.security_id;
  if (srcId === undefined || srcId === '') {
    return planned;
  }

  const membership = (
    conceptClass: string,
    chosen: string | undefined,
    label: string,
  ) => {
    if (chosen === undefined) {
      return;
    }

    const held = current.find((m) => m.conceptClass === conceptClass);
    if (held?.conceptId === chosen) {
      return;
    }

    planned.push({
      label: held === undefined ? `BELONGS_TO — ${label}` : `Re-point ${label}`,
      relType: 'BELONGS_TO',
      srcId,
      dstId: chosen,
      ...(held !== undefined && { replaces: held.conceptId }),
    });
  };

  // The most specific instrument-type choice wins; the asset class only scoped
  // the search that produced it.
  membership(
    'instrument_type',
    value.security_type ?? value.asset_class,
    'instrument type',
  );
  membership('instrument_subtype', value.security_subtype, 'subtype');

  if (value.issuer_entity_id !== undefined) {
    planned.push({
      label: 'ISSUED_BY — issuer',
      relType: 'ISSUED_BY',
      srcId,
      dstId: value.issuer_entity_id,
    });
  }

  if (value.underlying_security_id !== undefined) {
    planned.push({
      label: 'HAS_UNDERLYING — look-through',
      relType: 'HAS_UNDERLYING',
      srcId,
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
