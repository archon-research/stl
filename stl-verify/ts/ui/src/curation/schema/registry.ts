import type * as z from 'zod';

import { edgeWrite } from './edges.ts';
import {
  accountWrite,
  conceptWrite,
  entityWrite,
  securityWrite,
  sourceWrite,
} from './nodes.ts';
import { aliasRegisterWrite, instrumentRegisterWrite } from './registers.ts';
import type { RecordType } from './vocabularies.ts';

/**
 * The resource registry: one row per curatable thing.
 *
 * This is the layer that turns "we have schemas" into "we have an application".
 * Routes, navigation, list columns and the create forms are all read off it, so
 * adding a resource is a registry entry rather than a directory of files —
 * which is the same trade `dashboardkit` makes in uikit, and it holds for the
 * same reason: every one of these screens differs only in its schema, its
 * columns, and where its writes go.
 *
 * Three things are deliberately *not* in here, and each is a boundary worth
 * naming:
 *
 * - **No per-resource layout.** A resource that needs one uses the graded
 *   overrides on `SchemaForm`, or drops to `useSchemaForm`. A registry field for
 *   layout would grow into a second, worse component API.
 * - **No permissions.** `owner_role` is on the shape and the write path is
 *   VEC-647's. Guessing at it here would bake in an access model that ticket has
 *   not decided, and unpicking that later is the expensive order.
 * - **No delete.** Nothing in this store is deletable. `remove` is not a missing
 *   feature, it is a tombstone append — the same code path as a create, with a
 *   zero-length valid window and the RETRACTION reason code. The registry's CRUD
 *   is therefore Create, Read, Append and Retract, and the naming here says so.
 */

type ResourceColumn = {
  key: string;
  header: string;
  /** Reads from `attrs` rather than the row's own columns. */
  fromAttrs?: boolean;
  /** Renders in a monospace column, for ids and hashes. */
  mono?: boolean;
};

export type CurationResource = {
  /** URL segment and registry key. */
  key: string;
  label: string;
  singular: string;
  /**
   * Which store the append lands in. It decides the endpoint, and for `node` it
   * also decides that the attribute fields fold into `attrs`.
   */
  store: 'node' | 'edge' | 'instrument_register' | 'alias_register';
  /** Present for node resources; the discriminator the read is scoped by. */
  recordType?: RecordType;
  schema: z.ZodType;
  /** Supplied by the app rather than asked of the curator. */
  hidden?: readonly string[];
  columns: readonly ResourceColumn[];
  description: string;
};

const NODE_COLUMNS: readonly ResourceColumn[] = [
  { key: 'id', header: 'Id', mono: true },
  { key: 'status', header: 'Status' },
  { key: 'valid_from', header: 'Valid from' },
  { key: 'processing_version', header: 'PV' },
  { key: 'change_reason_code', header: 'Reason' },
];

export const RESOURCES: readonly CurationResource[] = [
  {
    key: 'securities',
    label: 'Securities',
    singular: 'security',
    store: 'node',
    recordType: 'SECURITY',
    schema: securityWrite,
    columns: [
      { key: 'id', header: 'Id', mono: true },
      { key: 'ticker', header: 'Ticker', fromAttrs: true },
      { key: 'security_name', header: 'Name', fromAttrs: true },
      { key: 'currency', header: 'Ccy', fromAttrs: true },
      { key: 'status', header: 'Status' },
      { key: 'valid_from', header: 'Valid from' },
    ],
    description:
      'Curated instruments. Classification, issuer, peg and underlying are edges, not fields here.',
  },
  {
    key: 'entities',
    label: 'Entities',
    singular: 'entity',
    store: 'node',
    recordType: 'ENTITY',
    schema: entityWrite,
    columns: [
      { key: 'id', header: 'Id', mono: true },
      { key: 'short_name', header: 'Short name', fromAttrs: true },
      { key: 'entity_type', header: 'Legal form', fromAttrs: true },
      { key: 'domicile_country', header: 'Domicile', fromAttrs: true },
      { key: 'status', header: 'Status' },
    ],
    description:
      'Legal persons and operators. LEI and BIC are aliases, held in the alias register.',
  },
  {
    key: 'concepts',
    label: 'Concepts',
    singular: 'concept',
    store: 'node',
    recordType: 'CONCEPT',
    schema: conceptWrite,
    columns: [
      { key: 'id', header: 'Id', mono: true },
      { key: 'concept_class', header: 'Class', fromAttrs: true },
      { key: 'label', header: 'Label', fromAttrs: true },
      { key: 'status', header: 'Status' },
    ],
    description:
      'Shared categories. A concept without a definition is a label, not a category.',
  },
  {
    key: 'sources',
    label: 'Sources',
    singular: 'source',
    store: 'node',
    recordType: 'SOURCE',
    schema: sourceWrite,
    columns: [
      { key: 'id', header: 'Id', mono: true },
      { key: 'label', header: 'Name', fromAttrs: true },
      { key: 'licence', header: 'Licence', fromAttrs: true },
      { key: 'redistributable', header: 'Redistributable', fromAttrs: true },
    ],
    description:
      'Feeds and datasets. The only kind whose shape is REQUIRED severity: licence and redistributability block the write.',
  },
  {
    key: 'accounts',
    label: 'Accounts',
    singular: 'account',
    store: 'node',
    recordType: 'ACCOUNT',
    schema: accountWrite,
    columns: NODE_COLUMNS,
    description:
      'Books that hold things. Staged in ADR-0007, so deliberately thin.',
  },
  {
    key: 'edges',
    label: 'Relationships',
    singular: 'relationship',
    store: 'edge',
    schema: edgeWrite,
    hidden: ['edge_seq'],
    columns: [
      { key: 'rel_type', header: 'Type' },
      { key: 'src_id', header: 'Source', mono: true },
      { key: 'dst_id', header: 'Destination', mono: true },
      { key: 'rel_weight', header: 'Weight', mono: true },
      { key: 'weight_basis', header: 'Basis' },
      { key: 'valid_from', header: 'From' },
      { key: 'valid_to', header: 'To' },
    ],
    description:
      'One form for all 13 ratified types: the vocabulary narrows the endpoints and decides the weight.',
  },
  {
    key: 'instrument-register',
    label: 'Instrument register',
    singular: 'instrument key',
    store: 'instrument_register',
    schema: instrumentRegisterWrite,
    columns: [
      { key: 'instrument_key', header: 'Key', mono: true },
      { key: 'key_namespace', header: 'Namespace' },
      { key: 'security_id', header: 'Security', mono: true },
      { key: 'chain_id', header: 'Chain' },
      { key: 'valid_from', header: 'From' },
    ],
    description:
      'Native key to security. Exactly one current mapping per key — the guarantee the hottest join needs.',
  },
  {
    key: 'alias-register',
    label: 'Alias register',
    singular: 'alias',
    store: 'alias_register',
    schema: aliasRegisterWrite,
    columns: [
      { key: 'id_scheme', header: 'Scheme' },
      { key: 'id_value', header: 'Value', mono: true },
      { key: 'node_id', header: 'Node', mono: true },
      { key: 'valid_from', header: 'From' },
      { key: 'valid_to', header: 'To' },
    ],
    description:
      'Public identifiers: ISIN, FIGI, CUSIP, LEI, ticker, holder address. Lookups, never ids.',
  },
];

export function resourceByKey(key: string): CurationResource | undefined {
  return RESOURCES.find((r) => r.key === key);
}
