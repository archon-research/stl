/**
 * The governed vocabularies of the combined master, transcribed from the wave-1
 * migration (`20260904_120000_secstore_node_edge_stores_and_vocabularies.sql`,
 * VEC-617 / PR #875) rather than from the classification worksheets.
 *
 * The worksheets and the migration disagree in three places, and the migration
 * wins every time: there are three weight bases and not four (no `UNITS`),
 * cardinality is spelled `1_per_class` / `1_per_parent` (not "1 per class"), and
 * only 13 relationship types are seeded — the draft ones in ADR-0007 §5 land by
 * migration as they ratify. A form offering a value the engine rejects is worse
 * than a form missing one, so these lists track the seeded rows.
 *
 * Every list here is `as const` and reaches zod through `z.enum`, so a value the
 * vocabulary drops becomes a type error at the call site that still names it.
 */

/** `weight_basis_vocabulary`. Each names a share of a whole, which is what makes
 * weights multiplicable along a path and summable under one basis. */
export const WEIGHT_BASES = ['VALUE', 'NOTIONAL', 'OWNERSHIP_PCT'] as const;

/** `sec_node.record_type`. ACCOUNT is staged; EVENT is deferred and absent. */
export const RECORD_TYPES = [
  'SECURITY',
  'ENTITY',
  'CONCEPT',
  'SOURCE',
  'ACCOUNT',
] as const;

/**
 * The id prefix each kind must carry. `sec_node_id_prefix_chk` makes
 * `record_type` a deterministic function of the prefix, and `sec_edge` re-applies
 * the same check to both endpoints — so this map is the client-side copy of a
 * constraint the database also enforces, and the form can reject a mistyped
 * endpoint before the round trip.
 */
export const ID_PREFIX: Record<RecordType, string> = {
  ENTITY: 'em-',
  SECURITY: 'sec-',
  CONCEPT: 'concept-',
  SOURCE: 'src-',
  ACCOUNT: 'acct-',
};

/** `rel_type_vocabulary.family`. */
export const REL_FAMILIES = [
  'composition',
  'issuance_ownership_control',
  'holding_allocation',
  'classification_governance',
  'identity_resolution',
  'lifecycle',
] as const;

/** `rel_type_vocabulary.cardinality`. */
export const CARDINALITIES = ['1', 'n', '1_per_class', '1_per_parent'] as const;

/** `concept_class_vocabulary`. */
export const CONCEPT_CLASSES = [
  'instrument_type',
  'instrument_subtype',
  'entity_type',
  'counterparty_role',
  'sector',
  'credit_rating',
  'jurisdiction',
  'currency',
] as const;

/**
 * `change_reason_vocabulary`, with `requires_approval` carried across.
 *
 * That flag is the reason this is a table of objects rather than a list of
 * codes: it drives a conditional requirement on `approved_by` in every node and
 * edge form, so the client can raise the error inline instead of learning it
 * from a rejected append.
 */
export const CHANGE_REASONS = [
  {
    code: 'SEED_LOAD',
    requiresApproval: false,
    description: 'initial vocabulary/schema/data seed',
  },
  {
    code: 'PORT_FROM_STANDALONE',
    requiresApproval: false,
    description: 'row ported from the frozen standalone masters',
  },
  {
    code: 'RULE_DERIVED',
    requiresApproval: false,
    description: 'loader rule where the shape is known',
  },
  {
    code: 'CURATED_SOURCE',
    requiresApproval: false,
    description: 'sourced judgment; the source is cited in change_reason',
  },
  {
    code: 'RECLASSIFICATION',
    requiresApproval: true,
    description: 'a classification moved',
  },
  {
    code: 'REPOINT',
    requiresApproval: true,
    description: 'an edge or register mapping re-pointed',
  },
  {
    code: 'VALID_TIME_AMEND',
    requiresApproval: false,
    description: 'late-arriving or amended source data; valid window corrected',
  },
  {
    code: 'RESTATEMENT',
    requiresApproval: true,
    description: 'an earlier record was wrong; supersedes_record_id set',
  },
  {
    code: 'RETRACTION',
    requiresApproval: true,
    description: 'tombstone: the record should never have existed',
  },
  {
    code: 'CORPORATE_ACTION',
    requiresApproval: false,
    description: 'status version + succession edge',
  },
  {
    code: 'DEDUP_SUPERSEDE',
    requiresApproval: true,
    description: 'SAME_AS / SUPERSEDES outcome',
  },
] as const;

/**
 * The same codes as a literal tuple, because `z.enum` needs one.
 *
 * Written out rather than derived with `.map()`: a mapped array is
 * `string[]`, and the cast back to a non-empty literal tuple that `z.enum`
 * requires is exactly the kind of assertion that can go stale silently. The
 * `satisfies` below makes the duplication safe instead — a code here that is not
 * in `CHANGE_REASONS`, or a reason row whose code is missing here, fails to
 * compile.
 */
export const CHANGE_REASON_CODES = [
  'SEED_LOAD',
  'PORT_FROM_STANDALONE',
  'RULE_DERIVED',
  'CURATED_SOURCE',
  'RECLASSIFICATION',
  'REPOINT',
  'VALID_TIME_AMEND',
  'RESTATEMENT',
  'RETRACTION',
  'CORPORATE_ACTION',
  'DEDUP_SUPERSEDE',
] as const satisfies readonly (typeof CHANGE_REASONS)[number]['code'][];

/** Codes whose append must carry an `approved_by` distinct from `actor`. */
const CODES_REQUIRING_APPROVAL: readonly string[] = CHANGE_REASONS.filter(
  (r) => r.requiresApproval,
).map((r) => r.code);

/**
 * `node_status_vocabulary`, per kind.
 *
 * `pairsWith` is the edge type a transition into the status has to be
 * accompanied by. It is what lets the status form tell a curator that moving a
 * security to MERGED without a SUCCEEDED_BY edge leaves the register pointing at
 * a node that no longer trades — a cross-resource rule no single-field
 * validation can express.
 */
const NODE_STATUSES = {
  SECURITY: [
    { status: 'ACTIVE', terminal: false, pairsWith: null },
    { status: 'SUSPENDED', terminal: false, pairsWith: null },
    { status: 'DELISTED', terminal: false, pairsWith: null },
    { status: 'DEFAULTED', terminal: false, pairsWith: null },
    { status: 'MATURED', terminal: true, pairsWith: null },
    { status: 'REDEEMED', terminal: true, pairsWith: null },
    { status: 'CONVERTED', terminal: true, pairsWith: 'CONVERTS_TO' },
    { status: 'MERGED', terminal: true, pairsWith: 'SUCCEEDED_BY' },
    { status: 'EXPIRED', terminal: true, pairsWith: null },
    { status: 'RETIRED', terminal: true, pairsWith: null },
  ],
  ENTITY: [
    { status: 'ACTIVE', terminal: false, pairsWith: null },
    { status: 'INACTIVE', terminal: false, pairsWith: null },
    { status: 'IN_LIQUIDATION', terminal: false, pairsWith: null },
    { status: 'DISSOLVED', terminal: true, pairsWith: null },
    { status: 'MERGED', terminal: true, pairsWith: null },
    { status: 'SUPERSEDED', terminal: true, pairsWith: 'SUPERSEDES' },
  ],
  CONCEPT: [
    { status: 'ACTIVE', terminal: false, pairsWith: null },
    { status: 'DEPRECATED', terminal: false, pairsWith: null },
    { status: 'RETIRED', terminal: true, pairsWith: null },
    { status: 'SUPERSEDED', terminal: true, pairsWith: 'SUPERSEDES' },
  ],
  SOURCE: [
    { status: 'ACTIVE', terminal: false, pairsWith: null },
    { status: 'SUSPENDED', terminal: false, pairsWith: null },
    { status: 'DECOMMISSIONED', terminal: true, pairsWith: null },
    { status: 'SUPERSEDED', terminal: true, pairsWith: 'SUPERSEDES' },
  ],
  ACCOUNT: [
    { status: 'ACTIVE', terminal: false, pairsWith: null },
    { status: 'FROZEN', terminal: false, pairsWith: null },
    { status: 'CLOSED', terminal: true, pairsWith: null },
  ],
} as const satisfies Record<
  RecordType,
  readonly { status: string; terminal: boolean; pairsWith: string | null }[]
>;

/**
 * The 13 ratified relationship types, with their endpoint kinds, cardinality and
 * declared weight basis.
 *
 * This is the client's copy of `rel_type_vocabulary`, and it is what makes the
 * edge form self-configuring: picking a `rel_type` narrows the endpoint pickers
 * to its legal kinds, decides whether the weight fields appear at all, and fixes
 * the basis rather than offering a choice. Endpoint *legality* is this triple;
 * endpoint *existence* is cross-row and stays with the validator, which is why
 * the reference pickers resolve against the node reads.
 */
export const REL_TYPES = [
  {
    relType: 'HAS_UNDERLYING',
    family: 'composition',
    srcKinds: ['SECURITY'],
    dstKinds: ['SECURITY'],
    cardinality: 'n',
    weightBasis: 'VALUE',
    description: 'what a token or wrapper is built on; the look-through spine',
  },
  {
    relType: 'ISSUED_BY',
    family: 'issuance_ownership_control',
    srcKinds: ['SECURITY'],
    dstKinds: ['ENTITY'],
    cardinality: '1',
    weightBasis: null,
    description: 'the issuer; replaces issuer_entity_id as authority',
  },
  {
    relType: 'SUBSIDIARY_OF',
    family: 'issuance_ownership_control',
    srcKinds: ['ENTITY'],
    dstKinds: ['ENTITY'],
    cardinality: '1_per_parent',
    weightBasis: 'OWNERSHIP_PCT',
    description:
      'legal parent; ultimate parent derived by walking, never stored',
  },
  {
    relType: 'AFFILIATE_OF',
    family: 'issuance_ownership_control',
    srcKinds: ['ENTITY'],
    dstKinds: ['ENTITY'],
    cardinality: 'n',
    weightBasis: null,
    description: 'related, not owned',
  },
  {
    relType: 'HELD_BY',
    family: 'holding_allocation',
    srcKinds: ['SECURITY'],
    dstKinds: ['ENTITY'],
    cardinality: 'n',
    weightBasis: null,
    description:
      'holder of record where holding is a reference fact; balances stay in the timeseries',
  },
  {
    relType: 'BELONGS_TO',
    family: 'classification_governance',
    srcKinds: ['SECURITY', 'ENTITY', 'ACCOUNT'],
    dstKinds: ['CONCEPT'],
    cardinality: '1_per_class',
    weightBasis: null,
    description: 'category membership',
  },
  {
    relType: 'NARROWER_THAN',
    family: 'classification_governance',
    srcKinds: ['CONCEPT'],
    dstKinds: ['CONCEPT'],
    cardinality: '1',
    weightBasis: null,
    description: 'taxonomy hierarchy; shape inheritance path',
  },
  {
    relType: 'GOVERNED_BY',
    family: 'classification_governance',
    srcKinds: ['ENTITY', 'ACCOUNT'],
    dstKinds: ['CONCEPT'],
    cardinality: 'n',
    weightBasis: null,
    description: 'which rule set applies',
  },
  {
    relType: 'SCORED_BY',
    family: 'classification_governance',
    srcKinds: ['CONCEPT'],
    dstKinds: ['CONCEPT'],
    cardinality: 'n',
    weightBasis: null,
    description: 'concept-to-concept pivot: asset class -> risk model',
  },
  {
    relType: 'OWNED_BY',
    family: 'classification_governance',
    srcKinds: ['CONCEPT'],
    dstKinds: ['ENTITY'],
    cardinality: '1',
    weightBasis: null,
    description: 'stewardship of a rule set',
  },
  {
    relType: 'SOURCED_FROM',
    family: 'classification_governance',
    srcKinds: ['SECURITY', 'ENTITY', 'CONCEPT', 'SOURCE', 'ACCOUNT'],
    dstKinds: ['SOURCE'],
    cardinality: 'n',
    weightBasis: null,
    description: 'feed provenance where lineage points at a source',
  },
  {
    relType: 'SUCCEEDED_BY',
    family: 'lifecycle',
    srcKinds: ['SECURITY'],
    dstKinds: ['SECURITY'],
    cardinality: '1',
    weightBasis: null,
    description:
      'merger / redenomination; old node -> MERGED; register re-points',
  },
  {
    relType: 'SPLIT_FROM',
    family: 'lifecycle',
    srcKinds: ['SECURITY'],
    dstKinds: ['SECURITY'],
    cardinality: '1',
    weightBasis: null,
    description: 'split / reverse split; payload: ratio, ex_date',
  },
] as const satisfies readonly {
  relType: string;
  family: (typeof REL_FAMILIES)[number];
  srcKinds: readonly RecordType[];
  dstKinds: readonly RecordType[];
  cardinality: (typeof CARDINALITIES)[number];
  weightBasis: (typeof WEIGHT_BASES)[number] | null;
  description: string;
}[];

/** The ratified type names as a literal tuple, for `z.enum`. See CHANGE_REASON_CODES. */
export const REL_TYPE_CODES = [
  'HAS_UNDERLYING',
  'ISSUED_BY',
  'SUBSIDIARY_OF',
  'AFFILIATE_OF',
  'HELD_BY',
  'BELONGS_TO',
  'NARROWER_THAN',
  'GOVERNED_BY',
  'SCORED_BY',
  'OWNED_BY',
  'SOURCED_FROM',
  'SUCCEEDED_BY',
  'SPLIT_FROM',
] as const satisfies readonly RelType[];

export type RecordType = (typeof RECORD_TYPES)[number];
export type WeightBasis = (typeof WEIGHT_BASES)[number];
export type RelType = (typeof REL_TYPES)[number]['relType'];

/**
 * The vocabulary row for a type, or nothing.
 *
 * Takes a plain `string` and returns `undefined` rather than taking `RelType`
 * and throwing. Both callers hold a string that *might* be a ratified type — a
 * half-edited select value, a body off the wire — so a signature demanding
 * `RelType` only moves the problem to a cast at each call site.
 */
export function relTypeSpec(
  relType: string,
): (typeof REL_TYPES)[number] | undefined {
  return REL_TYPES.find((r) => r.relType === relType);
}

/** Whether a code obliges the append to carry an approver. */
export function requiresApproval(code: string): boolean {
  return CODES_REQUIRING_APPROVAL.includes(code);
}

/** The statuses legal for a kind, which is a composite FK in the database. */
export function statusesFor(
  recordType: RecordType,
): readonly { status: string; terminal: boolean; pairsWith: string | null }[] {
  return NODE_STATUSES[recordType];
}
