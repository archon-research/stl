import type { RecordType } from './vocabularies.ts';

/**
 * The shape system (ADR-0007 §6, VEC-622) as client-side data.
 *
 * This is the part of the model that makes schema-driven forms genuinely
 * different from generated CRUD, and it turns on one distinction: **severity**.
 *
 * - `REQUIRED` — the validator rejects the write. A blocking form error.
 * - `EXPECTED` — the row is *stored*, flagged in `node_validity`, and excluded
 *   from metrics until curated. Not an error.
 * - `ADVISORY` — reported only.
 *
 * A form that treats all three as blocking is wrong in the expensive direction:
 * thirteen of the fifteen ratified shapes are EXPECTED, so a blocking form would
 * refuse most of the partially-curated rows the store is designed to hold — and
 * the whole point of `node_validity` is that incomplete curation is a worklist,
 * not a rejection. So the generated form renders EXPECTED gaps as warnings that
 * name what the row will be excluded from, and lets the curator submit anyway.
 *
 * `maturityTier` gates enforcement independently: a `DRAFT` shape is unenforced
 * even at REQUIRED severity, which is how a shape is authored and reviewed
 * before it starts rejecting writes.
 */

export type ShapeSeverity = 'REQUIRED' | 'EXPECTED' | 'ADVISORY';
type ShapeMaturity = 'DRAFT' | 'GOVERNED' | 'FROZEN';

type RequiredEdge = {
  relType: string;
  direction: 'out' | 'in';
  min: number;
  /** `null` for unbounded. */
  max: number | null;
  targetKind: RecordType;
  /** The target must sit under this concept, walked through NARROWER_THAN. */
  narrowerThan?: string;
};

type Shape = {
  shapeId: string;
  /** Activates on a node kind. */
  appliesToKind?: RecordType;
  /** Activates on membership of a concept, which is itself an edge. */
  appliesToConcept?: string;
  requiredFields: readonly string[];
  requiredEdges: readonly RequiredEdge[];
  severity: ShapeSeverity;
  maturityTier: ShapeMaturity;
  ownerRole: string;
};

/** The 15 ratified shapes. */
const SHAPES: readonly Shape[] = [
  {
    shapeId: 'concept_base',
    appliesToKind: 'CONCEPT',
    requiredFields: ['concept_class', 'label', 'definition'],
    requiredEdges: [
      {
        relType: 'NARROWER_THAN',
        direction: 'out',
        min: 1,
        max: 1,
        targetKind: 'CONCEPT',
      },
    ],
    severity: 'EXPECTED',
    maturityTier: 'GOVERNED',
    ownerRole: 'ref-data-steward',
  },
  {
    shapeId: 'security_base',
    appliesToKind: 'SECURITY',
    requiredFields: [],
    requiredEdges: [
      {
        relType: 'BELONGS_TO',
        direction: 'out',
        min: 1,
        max: 1,
        targetKind: 'CONCEPT',
        narrowerThan: 'concept-instrument_type',
      },
    ],
    severity: 'EXPECTED',
    maturityTier: 'GOVERNED',
    ownerRole: 'ref-data-steward',
  },
  {
    shapeId: 'security_issued',
    appliesToKind: 'SECURITY',
    requiredFields: ['currency'],
    requiredEdges: [
      {
        relType: 'ISSUED_BY',
        direction: 'out',
        min: 1,
        max: 1,
        targetKind: 'ENTITY',
      },
    ],
    severity: 'EXPECTED',
    maturityTier: 'GOVERNED',
    ownerRole: 'ref-data-steward',
  },
  {
    shapeId: 'entity_base',
    appliesToKind: 'ENTITY',
    requiredFields: ['short_name', 'entity_type'],
    requiredEdges: [],
    severity: 'EXPECTED',
    maturityTier: 'GOVERNED',
    ownerRole: 'ref-data-steward',
  },
  {
    shapeId: 'source_base',
    appliesToKind: 'SOURCE',
    requiredFields: ['licence', 'redistributable'],
    requiredEdges: [],
    severity: 'REQUIRED',
    maturityTier: 'GOVERNED',
    ownerRole: 'ref-data-steward',
  },
  {
    shapeId: 'account_book',
    appliesToKind: 'ACCOUNT',
    requiredFields: [],
    requiredEdges: [
      {
        relType: 'OPERATED_BY',
        direction: 'out',
        min: 1,
        max: 1,
        targetKind: 'ENTITY',
      },
    ],
    severity: 'EXPECTED',
    maturityTier: 'DRAFT',
    ownerRole: 'ref-data-steward',
  },
  {
    shapeId: 'entity_person',
    appliesToConcept: 'concept-natural_person',
    requiredFields: ['surrogate_key'],
    requiredEdges: [],
    severity: 'REQUIRED',
    maturityTier: 'GOVERNED',
    ownerRole: 'ref-data-steward',
  },
  {
    shapeId: 'concept_rule_class',
    appliesToConcept: 'concept-rule_class',
    requiredFields: ['external_ref'],
    requiredEdges: [],
    severity: 'REQUIRED',
    maturityTier: 'GOVERNED',
    ownerRole: 'model-review',
  },
  {
    shapeId: 'on_chain_token',
    appliesToConcept: 'concept-on_chain_token',
    requiredFields: ['address', 'chain_id'],
    requiredEdges: [],
    severity: 'REQUIRED',
    maturityTier: 'GOVERNED',
    ownerRole: 'ref-data-steward',
  },
  {
    shapeId: 'stablecoin',
    appliesToConcept: 'concept-st-digital_asset-stablecoin',
    requiredFields: [],
    requiredEdges: [
      {
        relType: 'HAS_UNDERLYING',
        direction: 'out',
        min: 1,
        max: null,
        targetKind: 'SECURITY',
      },
      {
        relType: 'PEGGED_TO',
        direction: 'out',
        min: 1,
        max: 1,
        targetKind: 'CONCEPT',
      },
    ],
    severity: 'EXPECTED',
    maturityTier: 'GOVERNED',
    ownerRole: 'ref-data-steward',
  },
  {
    shapeId: 'yield_bearing',
    appliesToConcept: 'concept-st-digital_asset-yield_bearing_token',
    requiredFields: [],
    requiredEdges: [
      {
        relType: 'HAS_UNDERLYING',
        direction: 'out',
        min: 1,
        max: null,
        targetKind: 'SECURITY',
      },
      {
        relType: 'ISSUED_BY',
        direction: 'out',
        min: 1,
        max: 1,
        targetKind: 'ENTITY',
      },
    ],
    severity: 'EXPECTED',
    maturityTier: 'GOVERNED',
    ownerRole: 'ref-data-steward',
  },
  {
    shapeId: 'wrapped_token',
    appliesToConcept: 'concept-st-digital_asset-wrapped_token',
    requiredFields: [],
    requiredEdges: [
      {
        relType: 'HAS_UNDERLYING',
        direction: 'out',
        min: 1,
        max: 1,
        targetKind: 'SECURITY',
      },
    ],
    severity: 'EXPECTED',
    maturityTier: 'GOVERNED',
    ownerRole: 'ref-data-steward',
  },
  {
    shapeId: 'lp_token',
    appliesToConcept: 'concept-st-digital_asset-lp_token',
    requiredFields: [],
    requiredEdges: [
      {
        relType: 'HAS_UNDERLYING',
        direction: 'out',
        min: 2,
        max: null,
        targetKind: 'SECURITY',
      },
    ],
    severity: 'EXPECTED',
    maturityTier: 'GOVERNED',
    ownerRole: 'ref-data-steward',
  },
  {
    shapeId: 'tokenised_fund',
    appliesToConcept: 'concept-st-money_market-tokenised_fund',
    requiredFields: [],
    requiredEdges: [
      {
        relType: 'ISSUED_BY',
        direction: 'out',
        min: 1,
        max: 1,
        targetKind: 'ENTITY',
      },
    ],
    severity: 'EXPECTED',
    maturityTier: 'GOVERNED',
    ownerRole: 'ref-data-steward',
  },
  {
    shapeId: 'structured_credit',
    appliesToConcept: 'concept-st-structured_credit-clo',
    requiredFields: ['credit_tranche'],
    requiredEdges: [
      {
        relType: 'ISSUED_BY',
        direction: 'out',
        min: 1,
        max: 1,
        targetKind: 'ENTITY',
      },
    ],
    severity: 'EXPECTED',
    maturityTier: 'GOVERNED',
    ownerRole: 'ref-data-steward',
  },
];

/** One unmet shape obligation, as the form and `node_validity` both express it. */
export type ShapeGap = {
  shapeId: string;
  severity: ShapeSeverity;
  /** The field or the relationship type the obligation is about. */
  target: string;
  kind: 'field' | 'edge';
  message: string;
};

/**
 * Which shapes are active for a node, given its kind and the concepts it belongs
 * to.
 *
 * Inheritance through `NARROWER_THAN` ancestry is the caller's to supply: pass
 * the *closure* of the node's memberships, not just its direct ones, because a
 * node in `concept-st-digital_asset-stablecoin` also inherits every shape on
 * that concept's ancestors. Resolving the closure is a graph walk, which belongs
 * with the data layer rather than here — `dim_cluster` is the pivot table that
 * exists to make it a lookup.
 */
function activeShapes(
  recordType: RecordType,
  conceptClosure: readonly string[],
): readonly Shape[] {
  return SHAPES.filter((shape) => {
    if (shape.maturityTier === 'DRAFT') {
      return false;
    }

    if (shape.appliesToKind !== undefined) {
      return shape.appliesToKind === recordType;
    }

    return shape.appliesToConcept !== undefined
      ? conceptClosure.includes(shape.appliesToConcept)
      : false;
  });
}

/**
 * Evaluates the active shapes against a draft row and its edges, returning the
 * unmet obligations rather than a boolean.
 *
 * Returning gaps and not a verdict is the whole point: the caller decides what
 * REQUIRED blocks and what EXPECTED merely warns about, and the same evaluation
 * drives both the inline warnings on a create form and the stewardship worklist
 * a `node_validity` screen would show.
 */
export function evaluateShapes(
  recordType: RecordType,
  conceptClosure: readonly string[],
  fields: Readonly<Record<string, unknown>>,
  edges: readonly { relType: string; direction: 'out' | 'in' }[],
): readonly ShapeGap[] {
  const gaps: ShapeGap[] = [];

  for (const shape of activeShapes(recordType, conceptClosure)) {
    for (const field of shape.requiredFields) {
      const value = fields[field];
      const missing = value === undefined || value === null || value === '';
      if (missing) {
        gaps.push({
          shapeId: shape.shapeId,
          severity: shape.severity,
          target: field,
          kind: 'field',
          message:
            shape.severity === 'REQUIRED'
              ? `${shape.shapeId} requires ${field}`
              : `${shape.shapeId} expects ${field}; the row stores but stays out of metrics until it is set`,
        });
      }
    }

    for (const edge of shape.requiredEdges) {
      const count = edges.filter(
        (e) => e.relType === edge.relType && e.direction === edge.direction,
      ).length;

      if (count < edge.min) {
        gaps.push({
          shapeId: shape.shapeId,
          severity: shape.severity,
          target: edge.relType,
          kind: 'edge',
          message:
            edge.min === 1
              ? `${shape.shapeId} expects a ${edge.relType} edge to a ${edge.targetKind}`
              : `${shape.shapeId} expects at least ${edge.min} ${edge.relType} edges`,
        });
      }

      if (edge.max !== null && count > edge.max && countableMax(edge)) {
        gaps.push({
          shapeId: shape.shapeId,
          severity: shape.severity,
          target: edge.relType,
          kind: 'edge',
          message: `${shape.shapeId} allows at most ${edge.max} ${edge.relType} edge${edge.max === 1 ? '' : 's'}`,
        });
      }
    }
  }

  return gaps;
}

/**
 * Whether an upper bound can honestly be checked by counting.
 *
 * It usually cannot, and getting this wrong is a false error rather than a
 * missed one. Two reasons, both from the vocabulary:
 *
 * - **Per-class cardinality.** BELONGS_TO is `1_per_class`, so a security with
 *   an asset class *and* a type has two of them legitimately. Counting all
 *   BELONGS_TO edges against a max of 1 reports the correctly-classified
 *   security as over-linked — which is what a naive count did here before this
 *   guard existed.
 * - **Subtree-scoped targets.** A shape that says "at most one BELONGS_TO to
 *   something narrower than `concept-instrument_type`" bounds a *scoped* count,
 *   and the scope is a NARROWER_THAN closure the client does not hold.
 *
 * Lower bounds are safe either way: an edge that is absent is absent whatever
 * the scope. So `min` is always checked and `max` only where the count is
 * unambiguous. The scoped upper bounds stay with the validator, which can see
 * the closure — GQ-13 in the DQ set.
 */
function countableMax(edge: RequiredEdge): boolean {
  const perScope = PER_SCOPE_REL_TYPES.has(edge.relType);

  return !perScope && edge.narrowerThan === undefined;
}

/** Rel types whose declared cardinality is scoped rather than absolute. */
const PER_SCOPE_REL_TYPES = new Set(['BELONGS_TO', 'SUBSIDIARY_OF']);
