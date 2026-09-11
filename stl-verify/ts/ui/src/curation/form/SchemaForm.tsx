import { Button, SurfaceMessage } from '@archon-research/design-system';
import type { ReactNode } from 'react';

import { css } from '#styled-system/css';

import type { ShapeGap } from '../schema/shapes.ts';
import { PageSection } from '../ui/PageFrame.tsx';
import { type FieldComponent, fieldManifest } from './fields.tsx';
import type { FieldPlan } from './introspect.ts';
import type { SchemaForm as SchemaFormState } from './useSchemaForm.ts';

/**
 * The generated form.
 *
 * The override mechanism is the part worth explaining, because "generated form"
 * usually means "form you have to abandon the moment one field is special". The
 * escape hatches here are graded, so the cost of deviating is proportional to
 * how far you deviate:
 *
 * - **`ui()` on the schema** — a different widget, label or help line. No React
 *   at all; the schema says it and every renderer honours it.
 * - **`overrides`** — one field rendered by a component of your own, still
 *   inside the generated layout, still bound through `getField`. This is the
 *   level most real deviations land at.
 * - **`hidden`** — a field the form supplies itself rather than asking for
 *   (`actor` from the session, `edge_seq` at its default).
 * - **`renderSection`** — one section laid out differently, e.g. the three
 *   classification pickers that have to sit in a row because each narrows the
 *   next.
 * - **`children`** — anything after the fields; the append preview and the
 *   submit row live here.
 *
 * Below that last rung, use `useSchemaForm` directly and lay the fields out by
 * hand. The hook is the contract, not this component — which is the difference
 * between a form generator you can leave and one you have to fight.
 */

export type SchemaFormProps<T> = {
  form: SchemaFormState<T>;
  /** Replaces the component for named fields. */
  overrides?: Readonly<Record<string, FieldComponent>>;
  /** Fields the caller supplies rather than asking the curator for. */
  hidden?: readonly string[];
  /** Unmet shape obligations, rendered by severity rather than as errors. */
  gaps?: readonly ShapeGap[];
  /** Replaces the layout of one section. */
  renderSection?: (
    section: { group: string; fields: readonly FieldPlan[] },
    renderField: (name: string) => ReactNode,
  ) => ReactNode | undefined;
  children?: ReactNode;
};

const grid = css({
  display: 'grid',
  gridTemplateColumns: { base: '1fr', md: 'repeat(2, minmax(0, 1fr))' },
  gap: '4',
});

const stack = css({
  display: 'flex',
  flexDirection: 'column',
  gap: '5',
});

/** `SurfaceMessage` takes a string body, so the gaps arrive as one sentence. */
function gapSentence(gaps: readonly ShapeGap[]): string {
  return gaps.map((g) => g.message).join(' · ');
}

export function SchemaForm<T>({
  form,
  overrides,
  hidden,
  gaps,
  renderSection,
  children,
}: SchemaFormProps<T>) {
  const isHidden = (name: string) => hidden?.includes(name) === true;

  const renderField = (name: string): ReactNode => {
    const binding = form.getField(name);
    const Component = overrides?.[name] ?? fieldManifest[binding.plan.widget];

    return <Component key={name} binding={binding} />;
  };

  // REQUIRED gaps block, so they read as errors. EXPECTED gaps do not: the row
  // stores and lands on the stewardship worklist, and saying "invalid" about a
  // row the store accepts would train curators to ignore the warning.
  const blocking = (gaps ?? []).filter((g) => g.severity === 'REQUIRED');
  const expected = (gaps ?? []).filter((g) => g.severity === 'EXPECTED');
  const advisory = (gaps ?? []).filter((g) => g.severity === 'ADVISORY');

  return (
    <form
      className={stack}
      onSubmit={(event) => {
        event.preventDefault();
        void form.handleSubmit();
      }}
      noValidate
    >
      {form.sections.map((section) => {
        const fields = section.fields.filter((f) => !isHidden(f.name));
        if (fields.length === 0) {
          return null;
        }

        const custom = renderSection?.(section, renderField);
        if (custom !== undefined) {
          return (
            <PageSection key={section.group} title={section.group}>
              {custom}
            </PageSection>
          );
        }

        return (
          <PageSection key={section.group} title={section.group}>
            <div className={grid}>{fields.map((f) => renderField(f.name))}</div>
          </PageSection>
        );
      })}

      {blocking.length > 0 && (
        <SurfaceMessage
          tone="critical"
          title="Required by an active shape"
          body={gapSentence(blocking)}
        />
      )}

      {expected.length > 0 && (
        <SurfaceMessage
          tone="dashed"
          title="This row will store, flagged and out of metrics"
          body={gapSentence(expected)}
        />
      )}

      {advisory.length > 0 && (
        <SurfaceMessage
          tone="muted"
          title="Advisory"
          body={gapSentence(advisory)}
        />
      )}

      {form.submitError !== undefined && (
        <SurfaceMessage
          tone="critical"
          title="The append was rejected"
          body={form.submitError}
        />
      )}

      {children}
    </form>
  );
}

/** The submit row every generated form ends with. */
export function SchemaFormActions<T>({
  form,
  label,
  blocked,
}: {
  form: SchemaFormState<T>;
  label: string;
  /** A REQUIRED shape gap, which stops the write even though zod is satisfied. */
  blocked?: boolean;
}) {
  const errorCount = Object.keys(form.allErrors).length;

  return (
    <div className={actionRowClassName}>
      <Button
        type="submit"
        emphasis="solid"
        colorPalette="blue"
        disabled={form.isSubmitting || blocked === true}
        className={actionButtonClassName}
      >
        {form.isSubmitting ? 'Appending…' : label}
      </Button>
      <Button
        type="button"
        onClick={form.reset}
        disabled={!form.isDirty}
        className={actionButtonClassName}
      >
        Reset
      </Button>
      {form.submitAttempted && !form.isValid && (
        <span className={actionHintClassName}>
          {errorCount} field{errorCount === 1 ? '' : 's'} need attention
        </span>
      )}
    </div>
  );
}

const actionRowClassName = css({
  display: 'flex',
  alignItems: 'center',
  gap: '3',
  flexWrap: 'wrap',
});

/**
 * Buttons size to their label rather than to the line.
 *
 * `variant="item"` was wrong for Reset — that variant is a full-width list row,
 * so it stretched across the remaining space — and nothing stopped the submit
 * label wrapping to two lines when the row got tight. Both are fixed here
 * rather than at a call site, because every generated form ends in this row.
 */
const actionButtonClassName = css({
  flex: 'none',
  width: 'auto',
  whiteSpace: 'nowrap',
});

const actionHintClassName = css({ fontSize: 'xs', color: 'text.critical' });
