import { CodeBlock, Panel } from '@archon-research/design-system';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from '@tanstack/react-router';
import { createContext, useContext, useEffect, useMemo } from 'react';

import { css } from '#styled-system/css';

import { ReferencePicker } from '../form/ReferencePicker.tsx';
import { SchemaForm, SchemaFormActions } from '../form/SchemaForm.tsx';
import type { FieldBinding } from '../form/useSchemaForm.ts';
import { useSchemaForm } from '../form/useSchemaForm.ts';
import { api } from '../lib/api.ts';
import { edgeWrite } from '../schema/edges.ts';
import { type RecordType, relTypeSpec } from '../schema/vocabularies.ts';

/**
 * The relationship form.
 *
 * The one resource that does not use the generic create view, for a reason that
 * is the whole argument for having escape hatches: an edge's *legal endpoints
 * depend on a sibling field*. `rel_type_vocabulary` says ISSUED_BY runs
 * SECURITY → ENTITY and BELONGS_TO runs SECURITY/ENTITY/ACCOUNT → CONCEPT, so
 * until a type is chosen there is no answer to "what may the source be".
 *
 * Static `ui()` metadata cannot express that, and the generic form's endpoint
 * pickers were therefore unscoped — they offered all 352 concepts and every
 * currency as a candidate source, and the mistake was only caught by the
 * endpoint-kind check after the fact. Here the two pickers are overrides that
 * read the chosen type from context, which turns a post-hoc validation error
 * into a candidate set that was never wrong.
 *
 * Everything else — the sections, the provenance block, the weight fields, the
 * cross-field rules, the append preview — is still the generated form.
 */

const EndpointScopeContext = createContext<{
  srcKinds: readonly RecordType[];
  dstKinds: readonly RecordType[];
}>({ srcKinds: [], dstKinds: [] });

function ScopedEndpoint({
  binding,
  side,
}: {
  binding: FieldBinding;
  side: 'src' | 'dst';
}) {
  const scope = useContext(EndpointScopeContext);
  const kinds = side === 'src' ? scope.srcKinds : scope.dstKinds;

  if (kinds.length === 0) {
    return (
      <div className={hint}>
        Choose a relationship type first — it decides which kinds are legal
        here.
      </div>
    );
  }

  return (
    <ReferencePicker
      binding={{ ...binding, plan: { ...binding.plan, targetKinds: kinds } }}
    />
  );
}

function ScopedSource({ binding }: { binding: FieldBinding }) {
  return <ScopedEndpoint binding={binding} side="src" />;
}

function ScopedDestination({ binding }: { binding: FieldBinding }) {
  return <ScopedEndpoint binding={binding} side="dst" />;
}

export function EdgeCreateView() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const today = useMemo(() => new Date().toISOString().slice(0, 10), []);

  const append = useMutation(
    api.mutationOptions('post', '/v1/secstore/edges', {
      invalidates: ['edges', 'nodes', 'validity'],
    }),
  );

  const form = useSchemaForm({
    schema: edgeWrite,
    initial: {
      valid_from: today,
      valid_to: 'infinity',
      edge_seq: 1,
      payload: '{}',
      change_reason_code: 'CURATED_SOURCE',
    },
    onSubmit: async (value) => {
      await append.mutateAsync({ body: value });
      await queryClient.invalidateQueries();
      await navigate({ to: '/$resourceKey', params: { resourceKey: 'edges' } });
    },
  });

  const relType = form.values['rel_type'];
  const spec = useMemo(
    () => (typeof relType === 'string' ? relTypeSpec(relType) : undefined),
    [relType],
  );

  const scope = useMemo(
    () => ({
      srcKinds: spec?.srcKinds ?? [],
      dstKinds: spec?.dstKinds ?? [],
    }),
    [spec],
  );

  // The basis is declared by the type rather than chosen, so it is filled from
  // the vocabulary and left visible as confirmation. In an effect, not in
  // render: writing form state during render is what makes a "why did my field
  // reset" bug, and the schema's own check would reject a mismatched basis
  // anyway — this only saves the curator from being told off for a value they
  // were never asked to pick.
  const declaredBasis = spec?.weightBasis ?? null;
  const { setFieldValue } = form;
  useEffect(() => {
    if (declaredBasis !== null) {
      setFieldValue('weight_basis', declaredBasis);
    }
  }, [declaredBasis, setFieldValue]);

  return (
    <div className={stack}>
      <div>
        <h1 className={heading}>New relationship</h1>
        <p className={subheading}>
          One form for all 13 ratified types. Choosing a type narrows both
          endpoint pickers to the kinds the vocabulary permits and fixes the
          weight basis.
        </p>
      </div>

      <EndpointScopeContext value={scope}>
        <SchemaForm
          form={form}
          hidden={['edge_seq']}
          overrides={{ src_id: ScopedSource, dst_id: ScopedDestination }}
        >
          <Panel title="The append" density="compact">
            {spec !== undefined && (
              <p className={note}>
                {spec.relType}: {spec.srcKinds.join('/')} →{' '}
                {spec.dstKinds.join('/')}, cardinality {spec.cardinality},{' '}
                {spec.weightBasis === null
                  ? 'unweighted'
                  : `weighted in ${spec.weightBasis}`}
                . {spec.description}
              </p>
            )}
            <CodeBlock>
              {JSON.stringify(
                form.parsed ?? { status: 'incomplete', errors: form.allErrors },
                null,
                2,
              )}
            </CodeBlock>
          </Panel>

          <SchemaFormActions form={form} label="Append relationship" />
        </SchemaForm>
      </EndpointScopeContext>
    </div>
  );
}

const stack = css({
  display: 'flex',
  flexDirection: 'column',
  gap: '5',
  maxWidth: '6xl',
});
const heading = css({ fontSize: 'xl', fontWeight: 'semibold' });
const subheading = css({
  fontSize: 'sm',
  color: 'text.muted',
  maxWidth: '4xl',
});
const note = css({ fontSize: 'xs', color: 'text.muted', marginBottom: '3' });
const hint = css({ fontSize: 'xs', color: 'text.muted' });
