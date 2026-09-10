import { Panel, SurfaceMessage } from '@archon-research/design-system';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { createContext, useContext, useMemo, useState } from 'react';

import { css } from '#styled-system/css';

import { ReferencePicker } from '../form/ReferencePicker.tsx';
import { SchemaForm, SchemaFormActions } from '../form/SchemaForm.tsx';
import type { FieldBinding } from '../form/useSchemaForm.ts';
import { useSchemaForm } from '../form/useSchemaForm.ts';
import { api } from '../lib/api.ts';
import type { EdgeWrite } from '../schema/edges.ts';
import { evaluateShapes, type ShapeGap } from '../schema/shapes.ts';
import {
  classifySecurity,
  type PlannableClassification,
  planClassification,
} from '../schema/workflows.ts';

/**
 * Classify a security — the worksheet row as a screen.
 *
 * This is the composite case, and it exercises the three things a generated
 * create form does not:
 *
 * 1. **Cascading narrowing.** The type picker's scope is the asset class the
 *    user just chose, so its `narrowerThan` is computed from sibling form state
 *    and injected through the override slot.
 * 2. **Live shape evaluation.** The active shapes depend on the concepts being
 *    chosen *in this form*, so the gaps are recomputed as the classification
 *    changes rather than read back after the write.
 * 3. **A fan-out of appends**, shown as a plan before it runs.
 */
export function ClassifyView() {
  const queryClient = useQueryClient();
  const today = useMemo(() => new Date().toISOString().slice(0, 10), []);
  const [applied, setApplied] = useState<string[]>([]);

  const appendEdge = useMutation(
    api.mutationOptions('post', '/v1/secstore/edges', {
      invalidates: ['edges', 'nodes', 'validity'],
    }),
  );

  const form = useSchemaForm({
    schema: classifySecurity,
    initial: { valid_from: today, change_reason_code: 'RECLASSIFICATION' },
    onSubmit: async (value) => {
      const plan = planClassification(value);
      const done: string[] = [];

      // Sequential, and it stops at the first failure. `workflows.ts` carries
      // why that is acceptable here and what the real fix is.
      for (const step of plan) {
        const body: EdgeWrite = {
          rel_type: step.relType,
          src_id: step.srcId,
          dst_id: step.dstId,
          edge_seq: 1,
          valid_from: value.valid_from,
          valid_to: 'infinity',
          payload: {},
          change_reason_code: value.change_reason_code,
          change_reason: value.change_reason,
          ...(value.approved_by !== undefined && {
            approved_by: value.approved_by,
          }),
          ...(step.weight !== undefined &&
            step.weightBasis !== undefined && {
              rel_weight: step.weight,
              weight_basis: step.weightBasis,
            }),
        };

        await appendEdge.mutateAsync({ body });

        done.push(step.label);
        setApplied([...done]);
      }

      await queryClient.invalidateQueries();
    },
  });

  const values = form.values;
  const assetClass =
    typeof values['asset_class'] === 'string' ? values['asset_class'] : '';

  const draft = useMemo(
    (): PlannableClassification => pickIds(values),
    [values],
  );
  const gaps = useLiveShapeGaps(draft);
  const plan = planClassification(draft);

  return (
    <div className={stack}>
      <div>
        <h1 className={heading}>Classify a security</h1>
        <p className={subheading}>
          One decision, several appends. The classification columns of the
          worksheet are BELONGS_TO edges into the seeded taxonomy; the issuer is
          an ISSUED_BY edge; the underlying is the look-through spine.
        </p>
      </div>

      <NarrowScopeContext value={assetClass}>
        <SchemaForm
          form={form}
          gaps={gaps}
          overrides={{ security_type: NarrowedTypePicker }}
        >
          <Panel title={`Planned appends (${plan.length})`} density="compact">
            {plan.length === 0 ? (
              <p className={hint}>
                Nothing to append yet. A security plus at least an asset class
                is the minimum.
              </p>
            ) : (
              <ol className={planList}>
                {plan.map((step) => (
                  <li key={step.label} className={planItem}>
                    <span>{step.label}</span>
                    <code className={code}>
                      {step.srcId} → {step.dstId}
                      {step.weight !== undefined &&
                        ` @ ${step.weight} ${step.weightBasis}`}
                    </code>
                  </li>
                ))}
              </ol>
            )}
            <p className={hint}>
              Issued one at a time: there is no batch endpoint, so a failure
              part way leaves the earlier appends in place. Each is
              independently valid.
            </p>
          </Panel>

          {applied.length > 0 && (
            <SurfaceMessage
              tone="muted"
              title="Applied"
              body={applied.join(' · ')}
            />
          )}

          <SchemaFormActions form={form} label="Apply classification" />
        </SchemaForm>
      </NarrowScopeContext>
    </div>
  );
}

/**
 * The concept subtree the type picker is scoped to.
 *
 * A context rather than a closure, because the override has to be a *stable*
 * component: defined inside `ClassifyView` it would be a new component type on
 * every keystroke, so React would unmount and remount the picker and the search
 * box would lose focus mid-word. The scope is the varying data, so the scope is
 * what travels — through context — and the component itself is declared once.
 */
const NarrowScopeContext = createContext('');

function NarrowedTypePicker({ binding }: { binding: FieldBinding }) {
  const scope = useContext(NarrowScopeContext);

  if (scope === '') {
    return (
      <div className={hint}>
        Choose an asset class first — the type list is the subtree beneath it.
      </div>
    );
  }

  return (
    <ReferencePicker
      binding={{ ...binding, plan: { ...binding.plan, narrowerThan: scope } }}
    />
  );
}

/**
 * The shape gaps for the security being classified, as the form stands.
 *
 * Evaluated against what the *form* would produce, not against what the store
 * currently holds — otherwise a curator who has just picked STABLECOIN would not
 * see the peg and underlying obligations that choice brings until after
 * submitting.
 */
function useLiveShapeGaps(value: PlannableClassification): readonly ShapeGap[] {
  const securityId = value.security_id;

  const node = useQuery({
    ...api.queryOptions(
      'get',
      '/v1/secstore/nodes/{node_id}',
      { params: { path: { node_id: securityId ?? '' } } },
      { tags: ['nodes'] },
    ),
    enabled: securityId !== undefined && securityId !== '',
  });

  const existingEdges = useQuery({
    ...api.queryOptions(
      'get',
      '/v1/secstore/edges',
      { params: { query: { src_id: securityId ?? '' } } },
      { tags: ['edges'] },
    ),
    enabled: securityId !== undefined && securityId !== '',
  });

  return useMemo(() => {
    if (node.data === undefined) {
      return [];
    }

    // The closure the form is *proposing*: the concepts picked here, which is
    // what the shapes will activate on once the appends land. Ancestors are not
    // walked client-side — the picker already scoped each level to its parent,
    // so the chain is the three chosen ids.
    const closure = [
      value.asset_class,
      value.security_type,
      value.security_subtype,
    ].filter((id): id is string => id !== undefined);

    const edges = [
      ...(existingEdges.data ?? []).map((e) => ({
        relType: e.rel_type,
        direction: 'out' as const,
      })),
      ...planClassification(value).map((step) => ({
        relType: step.relType,
        direction: 'out' as const,
      })),
    ];

    return evaluateShapes('SECURITY', closure, node.data.attrs, edges);
  }, [value, node.data, existingEdges.data]);
}

/** The id-valued fields, as strings, with blanks treated as absent. */
function pickIds(
  values: Readonly<Record<string, unknown>>,
): PlannableClassification {
  const read = (key: string) => {
    const raw = values[key];

    return typeof raw === 'string' && raw !== '' ? raw : undefined;
  };

  return {
    security_id: read('security_id'),
    asset_class: read('asset_class'),
    security_type: read('security_type'),
    security_subtype: read('security_subtype'),
    issuer_entity_id: read('issuer_entity_id'),
    underlying_security_id: read('underlying_security_id'),
  };
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
const hint = css({ fontSize: 'xs', color: 'text.muted' });
const planList = css({
  display: 'flex',
  flexDirection: 'column',
  gap: '2',
  marginBottom: '3',
  fontSize: 'sm',
});
const planItem = css({ display: 'flex', flexDirection: 'column', gap: '0.5' });
const code = css({ fontFamily: 'mono', fontSize: 'xs', color: 'text.muted' });
