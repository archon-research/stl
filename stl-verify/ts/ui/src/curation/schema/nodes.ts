import * as z from 'zod';

import {
  chainId,
  countryCode,
  evmAddress,
  nodeId,
  ui,
  validDate,
} from './primitives.ts';
import { checkProvenance, provenanceInput } from './provenance.ts';
import { CONCEPT_CLASSES, statusesFor } from './vocabularies.ts';

/**
 * The five node kinds as write schemas.
 *
 * `sec_node.attrs` is `jsonb` and the shape system (VEC-622) decides
 * required-ness per kind, so these schemas are the client's statement of the
 * shapes rather than a mirror of a column list. Two consequences worth naming,
 * because they shape every form below:
 *
 * 1. **Classification is not an attribute.** The worksheets carry `asset_class`,
 *    `security_type`, `security_subtype`, `issuer_entity_id`, `peg_currency` and
 *    `underlying` as columns, and ADR-0007 turns every one of them into an edge:
 *    BELONGS_TO into the concept taxonomy, ISSUED_BY, PEGGED_TO, HAS_UNDERLYING.
 *    So "create a security" is one node append plus several edge appends, and the
 *    form that looks like the worksheet is a composite over two resources. That
 *    is the single biggest divergence between how a curator thinks and how the
 *    store is shaped, and `workflows.ts` is where it is reconciled.
 *
 * 2. **Attributes are sparse by design.** Only what cannot be an edge stays on
 *    the node: identity, the things a shape marks REQUIRED, and the on-chain
 *    coordinates. Everything else moved.
 */

/**
 * The statuses legal for one kind, as an enum.
 *
 * Destructured rather than asserted into a non-empty tuple: every kind has at
 * least ACTIVE, so the throw is unreachable, but writing it that way means the
 * shape of `node_status_vocabulary` is checked rather than assumed.
 */
const statusEnum = (kind: Parameters<typeof statusesFor>[0]) => {
  const [first, ...rest] = statusesFor(kind).map((s) => s.status);
  if (first === undefined) {
    throw new Error(`no statuses declared for ${kind}`);
  }

  return z.enum([first, ...rest]);
};

/**
 * The window start every node append carries. `status` is kind-scoped (a
 * composite FK on `node_status_vocabulary`), so it is composed per kind below
 * rather than shared here.
 */
const nodeSpine = {
  valid_from: ui(validDate, {
    label: 'Valid from',
    help: 'Start of the window this version is true for, UTC date',
    group: 'Lifecycle',
    order: 2,
  }),
};

/**
 * SECURITY attributes.
 *
 * `address` and `chain_id` are optional here and made required by the
 * `on_chain_token` shape, which activates on concept membership rather than on
 * kind — so the requirement cannot live in this object. `shapes.ts` carries it.
 */
const securityAttrs = z.object({
  ticker: ui(z.string().trim().min(1).max(24), {
    label: 'Ticker',
    help: 'The symbol as it trades; not an identifier',
    group: 'Identity',
    order: 2,
  }),
  security_name: ui(z.string().trim().min(2).max(256), {
    label: 'Name',
    group: 'Identity',
    order: 3,
  }),
  currency: ui(
    z
      .string()
      .trim()
      .length(3)
      .regex(/^[A-Z]{3}$/)
      .optional(),
    {
      label: 'Currency',
      help: 'ISO 4217 currency of denomination; the security_issued shape expects it',
      group: 'Economics',
      order: 1,
    },
  ),
  is_tokenised: ui(z.boolean(), {
    label: 'Tokenised',
    help: 'Whether the instrument exists as an on-chain token',
    group: 'On-chain',
    order: 1,
  }),
  token_standard: ui(
    z.enum(['ERC-20', 'ERC-4626', 'NATIVE', 'OTHER']).optional(),
    {
      label: 'Token standard',
      group: 'On-chain',
      order: 2,
    },
  ),
  address: ui(evmAddress.optional(), {
    label: 'Contract address',
    help: 'The native instrument key; lands in the instrument register per chain',
    group: 'On-chain',
    order: 3,
  }),
  chain_id: ui(chainId.optional(), {
    label: 'Chain',
    group: 'On-chain',
    order: 4,
  }),
  country_of_issuance: ui(countryCode.optional(), {
    label: 'Country of issuance',
    group: 'Jurisdiction',
    order: 1,
  }),
  country_of_risk: ui(countryCode.optional(), {
    label: 'Country of risk',
    group: 'Jurisdiction',
    order: 2,
  }),
  credit_tranche: ui(z.string().trim().max(24).optional(), {
    label: 'Credit tranche',
    help: 'Required by the structured_credit shape, e.g. AAA',
    group: 'Credit',
    order: 1,
  }),
  credit_quality: ui(z.string().trim().max(24).optional(), {
    label: 'Credit quality',
    group: 'Credit',
    order: 2,
  }),
  collateral_pool: ui(z.string().trim().max(128).optional(), {
    label: 'Collateral pool',
    group: 'Credit',
    order: 3,
  }),
  backing: ui(z.string().trim().max(128).optional(), {
    label: 'Backing',
    help: 'What stands behind it, where that is not an edge',
    group: 'Credit',
    order: 4,
  }),
});

/** ENTITY attributes. LEI is deliberately absent — it is an alias, not an attribute. */
const entityAttrs = z.object({
  short_name: ui(z.string().trim().min(1).max(64), {
    label: 'Short name',
    help: 'The name the desk uses; the entity_base shape requires it',
    group: 'Identity',
    order: 2,
  }),
  legal_name: ui(z.string().trim().min(2).max(256).optional(), {
    label: 'Legal name',
    help: 'As registered. Leave blank when the legal entity is unverified',
    group: 'Identity',
    order: 3,
  }),
  entity_type: ui(
    z.enum([
      'SPV',
      'DAO',
      'CORPORATION',
      'FOUNDATION',
      'LLC',
      'BANK',
      'FUND',
      'TRUST',
      'PARTNERSHIP',
      'HOLDING_COMPANY',
      'ASSOCIATION',
      'GOVERNMENT',
      'SOVEREIGN',
      'SUPRANATIONAL',
      'INDIVIDUAL',
      'OTHER',
      'UNKNOWN',
    ]),
    { label: 'Legal form', group: 'Identity', order: 4 },
  ),
  counterparty_role: ui(
    z
      .enum([
        'PRIME',
        'ISSUER',
        'CUSTODIAN',
        'TRADING_COUNTERPARTY',
        'PROTOCOL',
        'PROTOCOL_OPERATOR',
        'BORROWER',
        'LENDER',
        'GUARANTOR',
        'SERVICER',
        'SPONSOR',
        'ORIGINATOR',
        'OTHER',
        'UNKNOWN',
      ])
      .optional(),
    { label: 'Counterparty role', group: 'Classification', order: 1 },
  ),
  sector: ui(
    z
      .enum([
        'FINANCIALS',
        'INFORMATION_TECHNOLOGY',
        'DIGITAL_ASSETS',
        'GOVERNMENT',
        'COMMUNICATION_SERVICES',
        'CONSUMER_DISCRETIONARY',
        'CONSUMER_STAPLES',
        'ENERGY',
        'HEALTH_CARE',
        'INDUSTRIALS',
        'MATERIALS',
        'REAL_ESTATE',
        'UTILITIES',
        'OTHER',
        'UNKNOWN',
      ])
      .optional(),
    {
      label: 'Sector',
      help: 'GICS-anchored',
      group: 'Classification',
      order: 2,
    },
  ),
  origination_type: ui(
    z
      .enum([
        'PROTOCOL',
        'INSTITUTIONAL',
        'BILATERAL',
        'RETAIL',
        'OTHER',
        'UNKNOWN',
      ])
      .optional(),
    { label: 'Origination', group: 'Classification', order: 3 },
  ),
  is_internal: ui(z.boolean(), {
    label: 'Internal',
    help: 'Part of the house group rather than a third party',
    group: 'Classification',
    order: 4,
  }),
  domicile_country: ui(countryCode.optional(), {
    label: 'Domicile',
    group: 'Jurisdiction',
    order: 1,
  }),
  country_of_risk: ui(countryCode.optional(), {
    label: 'Country of risk',
    group: 'Jurisdiction',
    order: 2,
  }),
});

/**
 * CONCEPT attributes.
 *
 * `definition` is required and it is the interesting one: ADR-0007 says a
 * concept without a definition is a label, not a category. The minimum length is
 * the form's way of holding that line — one or two sentences stating what
 * qualifies for membership, not a restatement of the label.
 */
const conceptAttrs = z.object({
  concept_class: ui(z.enum(CONCEPT_CLASSES), {
    label: 'Concept class',
    help: 'Which kind of category this is',
    group: 'Identity',
    order: 2,
  }),
  label: ui(z.string().trim().min(1).max(128), {
    label: 'Label',
    group: 'Identity',
    order: 3,
  }),
  definition: ui(z.string().trim().min(20).max(1000), {
    widget: 'textarea',
    label: 'Definition',
    help: 'One or two sentences stating what qualifies for membership',
    group: 'Identity',
    order: 4,
  }),
  vocabulary_source: ui(z.string().trim().max(128).optional(), {
    label: 'Vocabulary source',
    help: 'Where the value came from, e.g. ref_asset_class_l1 or GICS',
    group: 'Anchoring',
    order: 1,
  }),
  external_uri: ui(z.url().max(512).optional(), {
    label: 'External anchor',
    help: 'The standard this is anchored to, where one exists',
    group: 'Anchoring',
    order: 2,
  }),
});

/**
 * SOURCE attributes. The `source_base` shape is one of only two ratified shapes
 * at REQUIRED severity, so unlike every other kind these two genuinely block the
 * write — which is why they are required here and not deferred to `shapes.ts`.
 */
const sourceAttrs = z.object({
  label: ui(z.string().trim().min(2).max(128), {
    label: 'Name',
    group: 'Identity',
    order: 2,
  }),
  licence: ui(z.string().trim().min(2).max(256), {
    label: 'Licence',
    help: 'The licence the feed is used under; required by source_base',
    group: 'Terms',
    order: 1,
  }),
  redistributable: ui(z.boolean(), {
    label: 'Redistributable',
    help: 'Whether derived values may be exposed downstream',
    group: 'Terms',
    order: 2,
  }),
  reliability: ui(
    z.enum(['AUTHORITATIVE', 'CORROBORATING', 'INDICATIVE']).optional(),
    {
      label: 'Reliability',
      group: 'Terms',
      order: 3,
    },
  ),
});

/** ACCOUNT attributes. Staged in ADR-0007, so deliberately thin. */
const accountAttrs = z.object({
  label: ui(z.string().trim().min(2).max(128), {
    label: 'Name',
    group: 'Identity',
    order: 2,
  }),
  book_currency: ui(
    z
      .string()
      .trim()
      .length(3)
      .regex(/^[A-Z]{3}$/)
      .optional(),
    {
      label: 'Book currency',
      group: 'Economics',
      order: 1,
    },
  ),
});

const ATTRS = {
  SECURITY: securityAttrs,
  ENTITY: entityAttrs,
  CONCEPT: conceptAttrs,
  SOURCE: sourceAttrs,
  ACCOUNT: accountAttrs,
} as const;

/**
 * Composes the write schema for a node kind: the id, the lifecycle spine, the
 * kind's attributes, and the provenance block, with the provenance cross-field
 * rules attached.
 *
 * The nesting is flat on purpose. `attrs` is one jsonb column, but a form whose
 * fields are `attrs.ticker` gains nothing from the nesting and loses the ability
 * to put a jurisdiction field next to a lifecycle one in the same section — the
 * write layer folds the attribute fields back into `attrs` in one place instead.
 */
function nodeWriteSchema(kind: keyof typeof ATTRS) {
  return z
    .object({
      id: ui(nodeId(kind), {
        label: 'Node id',
        help: `Opaque and house-assigned, ${kind.toLowerCase()} prefix, assigned once`,
        group: 'Identity',
        order: 1,
      }),
      ...nodeSpine,
      status: ui(statusEnum(kind), {
        label: 'Status',
        group: 'Lifecycle',
        order: 1,
      }),
      ...ATTRS[kind].shape,
      ...provenanceInput.shape,
    })
    .check((ctx) => {
      checkProvenance(ctx.value, ctx);
    });
}

export const securityWrite = nodeWriteSchema('SECURITY');
export const entityWrite = nodeWriteSchema('ENTITY');
export const conceptWrite = nodeWriteSchema('CONCEPT');
export const sourceWrite = nodeWriteSchema('SOURCE');
export const accountWrite = nodeWriteSchema('ACCOUNT');
