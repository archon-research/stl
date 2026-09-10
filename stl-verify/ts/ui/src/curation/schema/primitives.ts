import * as z from 'zod';

import { ID_PREFIX, type RecordType } from './vocabularies.ts';

/**
 * The shared field vocabulary: the primitives every resource schema is built
 * from, and the typed UI hint that travels with them.
 *
 * The hint is carried in zod's own `.meta()` rather than in a parallel
 * description object keyed by field name. Two structures describing one field
 * drift the moment a field is renamed — one of them silently keeps the old key —
 * whereas a hint attached to the schema moves with it and disappears with it.
 */

/** How a field is rendered, when the inferred default is not what we want. */
export type FieldWidget =
  | 'text'
  | 'textarea'
  | 'select'
  | 'switch'
  | 'number'
  | 'decimal'
  | 'date'
  | 'reference'
  | 'json'
  | 'readonly';

export type FieldUi = {
  /** Overrides the widget the zod type would otherwise infer. */
  widget?: FieldWidget;
  label?: string;
  /** Shown under the control at rest; the error text replaces it when invalid. */
  help?: string;
  placeholder?: string;
  /**
   * For `reference` fields: which node kinds the picker may resolve. The picker
   * queries the node reads scoped to these, which is how endpoint pickers stay
   * inside `rel_type_vocabulary`'s legal kinds.
   */
  targetKinds?: readonly RecordType[];
  /**
   * For `reference` fields into CONCEPT: the concept class the target must
   * belong to, and optionally the ancestor it must sit under. `1_per_class`
   * cardinality on BELONGS_TO is per concept class, so the class is what makes
   * one membership distinguishable from another.
   */
  conceptClass?: string;
  narrowerThan?: string;
  /** Field group, used to section a generated form. */
  group?: string;
  /** Ordering hint within a group; lower sorts first. */
  order?: number;
  /**
   * Set on the columns the append guard computes or assigns. They render as
   * read-only context and are never sent, because a supplied value is either
   * rejected (`ingest_xid`) or verified and rejected on mismatch
   * (`content_hash`).
   */
  engineAssigned?: boolean;
};

const UI_KEY = 'stlCurationUi';

/** Attaches a UI hint to a schema, returning the same schema type. */
export function ui<T extends z.ZodType>(schema: T, hint: FieldUi): T {
  return schema.meta({ [UI_KEY]: hint }) as T;
}

/**
 * Reads a UI hint back off a schema.
 *
 * `.meta()` survives the optional/nullable/default wrappers, so a hint applied
 * before `.optional()` is still readable from the outside. The shape is
 * re-asserted here because `.meta()` itself is `Record<string, unknown>`.
 */
export function fieldUi(schema: z.ZodType): FieldUi {
  const hint = schema.meta()?.[UI_KEY];

  return typeof hint === 'object' && hint !== null ? (hint as FieldUi) : {};
}

/**
 * A node id of a given kind.
 *
 * The prefix check is `sec_node_id_prefix_chk` restated on the client. It is
 * worth restating because it is the one identity rule a curator can violate by
 * typing: the prefix is governed, house-assigned, and never derived from a
 * ticker or a name, so `em-usdc` for a security is a mistake the form should
 * catch rather than a round trip.
 */
export function nodeId(kind: RecordType) {
  const prefix = ID_PREFIX[kind];

  return z
    .string()
    .trim()
    .min(prefix.length + 1, `a ${kind} id is ${prefix} plus a stable slug`)
    .max(128)
    .startsWith(prefix, `a ${kind} id must start with ${prefix}`)
    .regex(
      /^[a-z][a-z0-9-]*[a-z0-9]$/,
      'lower case, digits and hyphens only — ids are opaque slugs, not names',
    );
}

/** A node id of any governed kind, for a field whose target kind varies. */
export const anyNodeId = z
  .string()
  .trim()
  .min(2)
  .max(128)
  .refine(
    (v) => Object.values(ID_PREFIX).some((p) => v.startsWith(p)),
    'must carry a governed kind prefix: em- sec- concept- src- acct-',
  );

/**
 * A valid-time date.
 *
 * The grain is a day, deliberately and with a known cost: two changes to one
 * record on the same date are not both representable, since both windows are
 * `[D, D+1)` and resolution keeps one. The form surfaces that where it bites —
 * on a same-day re-version — rather than pretending a timestamp is available.
 */
export const validDate = z.iso.date();

/**
 * `valid_to`, which is `NOT NULL` in the database with `'infinity'` as the open
 * sentinel — so the sentinel is admitted as a literal rather than spelled as a
 * null the column does not accept.
 */
export const validToDate = z.union([z.iso.date(), z.literal('infinity')]);

/**
 * `numeric(30,18)` as a string, never a JS number.
 *
 * `rel_weight` is exact decimal because look-through multiplies weights along a
 * path and RP-4.4 requires the products to be reproducible. A float round-trip
 * through JSON loses that, so the whole client side of the weight is a string
 * and the arithmetic stays in the database.
 */
export const exactDecimal = z
  .string()
  .trim()
  .regex(
    /^-?\d{1,12}(\.\d{1,18})?$/,
    'up to 12 integer digits and 18 decimal places, as an exact decimal',
  );

/** A chain id, soft FK to `chain.chain_id`; absent for off-chain things. */
export const chainId = z.int().positive().max(2_147_483_647);

/** An EVM contract address, the native instrument key for a token. */
export const evmAddress = z
  .string()
  .trim()
  .regex(/^0x[0-9a-fA-F]{40}$/, 'a 20-byte hex address, 0x-prefixed');

/** An ISO 3166-1 alpha-2 country, with XX for "unknown" as the worksheets use it. */
export const countryCode = z
  .string()
  .trim()
  .length(2)
  .regex(/^[A-Z]{2}$/, 'ISO 3166-1 alpha-2, upper case');

/** A GLEIF LEI: 20 alphanumerics, the last two a check digit pair. */
export const lei = z
  .string()
  .trim()
  .regex(
    /^[0-9A-Z]{18}[0-9]{2}$/,
    '20 characters: 18 alphanumeric then 2 check digits',
  );
