import { Field, SearchInput } from '@archon-research/design-system';
import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';

import { css } from '#styled-system/css';

import { api } from '../lib/api.ts';
import type { RecordType } from '../schema/vocabularies.ts';
import { asText } from './text.ts';
import type { FieldBinding } from './useSchemaForm.ts';

/**
 * A reference field: pick a node id by searching for the thing it names.
 *
 * This is where the relational half of the requirement lives, and it is the
 * reason a plain `<select>` was never going to do. The taxonomy that wave 1
 * seeds is 352 concepts — 152 of them security types — so the classification
 * pickers are searches, not dropdowns, and the search has to be *narrowed* or it
 * offers a curator the 151 wrong answers alongside the right one:
 *
 * - `targetKinds` restricts to the kinds `rel_type_vocabulary` declares legal
 *   for the endpoint, so an ISSUED_BY destination search never returns a
 *   security.
 * - `conceptClass` restricts to one class, because BELONGS_TO is `1_per_class`
 *   and the class is what makes two memberships distinguishable.
 * - `narrowerThan` restricts to the subtree under a concept the *form* chose,
 *   which is how picking DIGITAL_ASSET as the asset class cuts the type picker
 *   from 152 values to the dozen legal beneath it.
 *
 * The narrowing is a server-side walk of `NARROWER_THAN` (`dim_cluster` is the
 * pivot that makes it a lookup), not a client filter, because the closure is a
 * graph traversal and the client holds no graph.
 *
 * Built on `SearchInput` rather than an Ark Combobox: the design system already
 * ships the debounced, option-listing, empty-messaging control this needs, and
 * reaching past it to `@ark-ui/react` would add an undeclared dependency knip is
 * right to reject. What `SearchInput` does *not* give is a rendered label for an
 * already-chosen value, which is why the resolved label is shown beneath rather
 * than in the box — an upstream gap worth filing against uikit.
 */
export function ReferencePicker({ binding }: { binding: FieldBinding }) {
  const { plan, value, error, setValue, onBlur } = binding;
  const [term, setTerm] = useState('');

  const kinds = plan.targetKinds ?? [];
  // One kind is a server-side filter; several means the search cannot be scoped
  // and the prefix on each result is what tells them apart.
  const singleKind: RecordType | undefined =
    kinds.length === 1 ? kinds[0] : undefined;

  const search = useQuery(
    api.queryOptions(
      'get',
      '/v1/secstore/nodes',
      {
        params: {
          query: {
            ...(singleKind !== undefined && { record_type: singleKind }),
            ...(term !== '' && { q: term }),
            ...(plan.conceptClass !== undefined && {
              concept_class: plan.conceptClass,
            }),
            ...(plan.narrowerThan !== undefined && {
              narrower_than: plan.narrowerThan,
            }),
            limit: 20,
          },
        },
      },
      {
        tags: ['nodes'],
        // Reference data at governance cadence: a curator's search does not
        // need a fresh read on every keystroke, and the taxonomy has not moved
        // since the migration seeded it.
        staleTime: 60_000,
      },
    ),
  );

  // The chosen id's label, resolved separately from the search so it survives
  // the search term being cleared.
  const chosen = useQuery({
    ...api.queryOptions(
      'get',
      '/v1/secstore/nodes/{node_id}',
      { params: { path: { node_id: asText(value) } } },
      { tags: ['nodes'], staleTime: 60_000 },
    ),
    enabled: typeof value === 'string' && value !== '',
  });

  const options = (search.data ?? []).map((node) => ({
    value: node.id,
    label: `${labelOf(node.attrs)} — ${node.id}`,
  }));

  const resolvedLabel =
    chosen.data === undefined ? undefined : labelOf(chosen.data.attrs);

  return (
    <Field.Root required={plan.required} invalid={error !== undefined}>
      <Field.Label>{plan.label}</Field.Label>
      <SearchInput
        value={term}
        onValueChange={setTerm}
        options={options}
        onSelectOption={(option) => {
          setValue(option.value);
          setTerm('');
          onBlur();
        }}
        loading={search.isFetching}
        placeholder={
          typeof value === 'string' && value !== ''
            ? value
            : (plan.placeholder ?? searchHint(kinds))
        }
        emptyMessage={
          term === ''
            ? 'Type to search'
            : 'Nothing matches inside the allowed set'
        }
        onBlur={onBlur}
      />
      {error !== undefined && <span className={errorTextStyle}>{error}</span>}
      {error === undefined && value !== '' && value !== undefined && (
        <span className={chosenStyle}>
          {resolvedLabel === undefined
            ? asText(value)
            : `${resolvedLabel} (${asText(value)})`}
          {chosen.isError && ' — does not resolve to a current node'}
        </span>
      )}
      {error === undefined &&
        (value === '' || value === undefined) &&
        plan.help !== undefined && (
          <span className={helpStyle}>{plan.help}</span>
        )}
    </Field.Root>
  );
}

/** The human name on a node, whichever attribute carries it for its kind. */
function labelOf(attrs: Record<string, unknown>): string {
  for (const key of [
    'label',
    'short_name',
    'security_name',
    'ticker',
    'legal_name',
  ]) {
    const value = attrs[key];
    if (typeof value === 'string' && value !== '') {
      return value;
    }
  }

  return '(unnamed)';
}

function searchHint(kinds: readonly RecordType[]): string {
  return kinds.length === 0
    ? 'Search for a node'
    : `Search ${kinds.map((k) => k.toLowerCase()).join(' or ')}`;
}

const errorTextStyle = css({
  fontSize: 'xs',
  color: 'text.critical',
  marginTop: '1',
});
const helpStyle = css({ fontSize: 'xs', color: 'text.muted', marginTop: '1' });
const chosenStyle = css({
  fontSize: 'xs',
  color: 'text.default',
  marginTop: '1',
  fontFamily: 'mono',
});
