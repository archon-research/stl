import { CodeBlock, Panel } from '@archon-research/design-system';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from '@tanstack/react-router';
import { useMemo } from 'react';

import { css } from '#styled-system/css';

import { SchemaForm, SchemaFormActions } from '../form/SchemaForm.tsx';
import { useSchemaForm } from '../form/useSchemaForm.ts';
import { api } from '../lib/api.ts';
import { ENGINE_ASSIGNED_COLUMNS } from '../schema/provenance.ts';
import type { CurationResource } from '../schema/registry.ts';

/**
 * The generated create form, for any resource in the registry.
 *
 * Every screen here is this component with a different registry row — which is
 * the claim the spike exists to test. Eight resources, one form, and the only
 * per-resource inputs are a zod schema and a list of fields the app supplies
 * itself.
 *
 * The append preview beneath the form is not decoration. The store is
 * append-only and bitemporal, so what a curator is about to do is *not* "save
 * this record" — it is "add a row with this valid window and this reason". The
 * preview shows the body that will be posted, which is the only honest way to
 * make the difference visible before it is committed.
 */
export function ResourceCreateView({
  resource,
}: {
  resource: CurationResource;
}) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const today = useMemo(() => new Date().toISOString().slice(0, 10), []);

  const append = useMutation(
    api.mutationOptions('post', endpointFor(resource), {
      invalidates: invalidatesFor(resource),
    }),
  );

  const form = useSchemaForm({
    schema: resource.schema,
    initial: {
      valid_from: today,
      status: 'ACTIVE',
      change_reason_code: 'CURATED_SOURCE',
    },
    onSubmit: async (value) => {
      await append.mutateAsync({ body: stripEngineAssigned(value) });

      await queryClient.invalidateQueries();
      await navigate({
        to: '/$resourceKey',
        params: { resourceKey: resource.key },
      });
    },
  });

  return (
    <div className={stack}>
      <div>
        <h1 className={heading}>New {resource.singular}</h1>
        <p className={subheading}>{resource.description}</p>
      </div>

      <SchemaForm
        form={form}
        {...(resource.hidden !== undefined && { hidden: resource.hidden })}
      >
        <Panel title="The append" density="compact">
          <p className={note}>
            Not an update: this posts one new row. `record_id`, `ingest_xid` and
            `content_hash` are the append guard&rsquo;s and are never sent.
          </p>
          <CodeBlock>
            {JSON.stringify(
              form.parsed === undefined
                ? { status: 'incomplete', errors: form.allErrors }
                : stripEngineAssigned(form.parsed),
              null,
              2,
            )}
          </CodeBlock>
        </Panel>

        <SchemaFormActions form={form} label={`Append ${resource.singular}`} />
      </SchemaForm>
    </div>
  );
}

function endpointFor(resource: CurationResource) {
  switch (resource.store) {
    case 'edge':
      return '/v1/secstore/edges' as const;
    case 'instrument_register':
      return '/v1/secstore/registers/instrument' as const;
    case 'alias_register':
      return '/v1/secstore/registers/alias' as const;
    default:
      return '/v1/secstore/nodes' as const;
  }
}

function invalidatesFor(resource: CurationResource) {
  // A node append can change what an edge read means and what the worklist
  // shows, so the blunt set is the correct one. `api.ts` carries the why.
  switch (resource.store) {
    case 'edge':
      return ['edges', 'nodes', 'validity'] as const;
    case 'instrument_register':
    case 'alias_register':
      return ['registers'] as const;
    default:
      return ['nodes', 'validity'] as const;
  }
}

/**
 * Drops the columns the append guard owns.
 *
 * Asserted rather than assumed: no form here offers one, but the guard rejects a
 * supplied `ingest_xid` outright and verifies `content_hash` against its own
 * computation, so a body that grew one would fail at the write boundary with an
 * error that points at the column rather than at whatever added it.
 */
function stripEngineAssigned(value: unknown): Record<string, unknown> {
  const out: Record<string, unknown> = {};

  if (typeof value !== 'object' || value === null) {
    return out;
  }

  for (const [key, item] of Object.entries(value)) {
    if (!(ENGINE_ASSIGNED_COLUMNS as readonly string[]).includes(key)) {
      out[key] = item;
    }
  }

  return out;
}

const stack = css({
  display: 'flex',
  flexDirection: 'column',
  gap: '5',
  maxWidth: '6xl',
});
const heading = css({ fontSize: 'xl', fontWeight: 'semibold' });
const subheading = css({ fontSize: 'sm', color: 'text.muted' });
const note = css({ fontSize: 'xs', color: 'text.muted', marginBottom: '3' });
