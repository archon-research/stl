import * as z from 'zod';

import { ui } from './primitives.ts';
import { CHANGE_REASON_CODES, requiresApproval } from './vocabularies.ts';

/**
 * The provenance block: the audit spine ADR-0007 §4 puts on every append, to
 * both stores, identically.
 *
 * That it is identical is the single strongest argument for generating these
 * forms rather than hand-writing them. Nine of the columns on `sec_node` and
 * eleven on `sec_edge` are this block, so a hand-written form per resource would
 * restate it per resource — and the parts that must not be sent
 * (`ingest_xid`, `content_hash`, `record_id`) would be restated as omissions,
 * which is exactly the kind of thing that gets re-added by accident. Declared
 * once, it renders as one section on every form and the write layer strips the
 * engine-assigned columns in one place.
 *
 * What a curator supplies is narrow: a reason code, a narrative, and an approver
 * where the code demands one. `actor` comes from the session, `run_id` from the
 * writer run, `source_system` from the surface — none of them are a curator's to
 * type, and VEC-647 is where they stop being placeholders.
 */
export const provenanceInput = z.object({
  change_reason_code: ui(z.enum(CHANGE_REASON_CODES), {
    label: 'Reason code',
    help: 'The structured reason; some codes require an approver',
    group: 'Provenance',
    order: 1,
  }),
  change_reason: ui(z.string().trim().min(8).max(1000), {
    widget: 'textarea',
    label: 'Reason',
    help: 'Free text. Cite the source when the code is CURATED_SOURCE',
    group: 'Provenance',
    order: 2,
  }),
  approved_by: ui(z.string().trim().min(3).max(128).optional(), {
    label: 'Approved by',
    help: 'Required for reclassification, re-point, restatement, retraction and dedup',
    group: 'Provenance',
    order: 3,
  }),
  supersedes_record_id: ui(z.int().positive().optional(), {
    label: 'Supersedes record',
    help: 'The record_id this append corrects or retracts',
    group: 'Provenance',
    order: 4,
  }),
});

export type ProvenanceInput = z.infer<typeof provenanceInput>;

/**
 * The two cross-field rules the provenance block carries, applied to any object
 * that embeds it.
 *
 * They are attached with `.check()` on the composed resource schema rather than
 * on `provenanceInput` itself: a `refine` on the inner object would run before
 * the outer object is assembled, and `supersedes_record_id`'s rule needs to see
 * a sibling the inner object does not have. Keeping them as a reusable checker
 * is what lets every resource inherit both without restating either.
 */
export function checkProvenance(
  value: ProvenanceInput,
  ctx: z.core.ParsePayload,
): void {
  if (requiresApproval(value.change_reason_code) && !value.approved_by) {
    ctx.issues.push({
      code: 'custom',
      path: ['approved_by'],
      message: `${value.change_reason_code} requires an approver distinct from the appender`,
      input: value.approved_by,
    });
  }

  // A restatement without a target is unchainable: the guard needs the
  // predecessor's content_hash to chain this row's, so it is rejected at the
  // write boundary rather than stored as an orphan correction.
  if (
    (value.change_reason_code === 'RESTATEMENT' ||
      value.change_reason_code === 'RETRACTION') &&
    value.supersedes_record_id === undefined
  ) {
    ctx.issues.push({
      code: 'custom',
      path: ['supersedes_record_id'],
      message: `${value.change_reason_code} must name the record it supersedes`,
      input: value.supersedes_record_id,
    });
  }
}

/**
 * The columns the append guard owns, listed so the write layer can assert it
 * stripped them rather than trusting that no form ever added one.
 *
 * `ingest_xid` is rejected outright if supplied; `content_hash` is recomputed and
 * rejected on mismatch; `record_id` and `edge_id` are generated. Sending any of
 * them is a client bug that the database is entitled to refuse.
 */
export const ENGINE_ASSIGNED_COLUMNS = [
  'record_id',
  'edge_id',
  'ingest_xid',
  'ingested_at',
  'content_hash',
] as const;
