import { Menu } from '@archon-research/design-system';
import { useRouter } from '@tanstack/react-router';
import { Check, Settings2 } from 'lucide-react';
import { Fragment } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { canRestateProvenance, useProvenanceView } from '../lib/provenance';
import type { Provenance } from '../types/allocation';

type SettingsOption = {
  value: string;
  label: string;
  description?: string;
  /** Offered but not selectable — this prime cannot be served that way. */
  disabled?: boolean;
};

export type SettingsSection = {
  id: string;
  label: string;
  value: string;
  options: SettingsOption[];
  onChange: (value: string) => void;
};

const triggerClassName = css({
  display: 'inline-flex',
  alignItems: 'center',
  justifyContent: 'center',
  height: '2.25rem',
  width: '2.25rem',
  flexShrink: 0,
  borderWidth: '1px',
  borderStyle: 'solid',
  borderColor: 'border.subtle',
  borderRadius: 'md',
  background: 'surface.default',
  color: 'text.muted',
  cursor: 'pointer',
  _hover: { color: 'text.strong', borderColor: 'border.default' },
  '&[data-state="open"]': { color: 'text.strong' },
});

const contentClassName = css({
  minWidth: '15rem',
  borderWidth: '1px',
  borderStyle: 'solid',
  borderColor: 'border.subtle',
  borderRadius: 'md',
  background: 'surface.default',
  boxShadow: 'overlay',
  paddingBlock: '1.5',
  overflow: 'hidden',
  zIndex: '50',
  _focusVisible: { outline: 'none' },
});

const groupLabelClassName = css({
  paddingInline: '3',
  paddingBlock: '1',
  fontSize: 'xs',
  fontWeight: 'semibold',
  textTransform: 'uppercase',
  letterSpacing: 'wide',
  color: 'text.muted',
});

const radioItemClassName = css({
  display: 'grid',
  gridTemplateColumns: '1rem 1fr',
  gap: '2',
  alignItems: 'start',
  paddingInline: '3',
  paddingBlock: '1.5',
  fontSize: 'sm',
  color: 'text.default',
  cursor: 'pointer',
  _hover: { background: 'surface.subtle' },
  '&[data-highlighted]': { background: 'surface.subtle' },
});

const indicatorClassName = css({
  display: 'inline-flex',
  alignItems: 'center',
  justifyContent: 'center',
  height: '1rem',
  color: 'text.link',
});

const optionDescriptionClassName = css({
  fontSize: 'xs',
  color: 'text.muted',
  lineHeight: 'short',
});

const separatorClassName = css({
  height: '1px',
  marginBlock: '1.5',
  marginInline: '0',
  borderWidth: '0',
  background: 'border.subtle',
});

/**
 * Settings that belong to the whole view rather than to one card, in labelled
 * sections. Separators are interleaved rather than declared, so adding a section
 * is one array entry and a lone section never renders a dangling rule.
 */
export function SettingsMenu({ sections }: { sections: SettingsSection[] }) {
  return (
    <Menu.Root>
      <Menu.Trigger
        aria-label="View settings"
        className={triggerClassName}
        type="button"
      >
        <Settings2 aria-hidden size={16} />
      </Menu.Trigger>
      <Menu.Positioner>
        <Menu.Content className={contentClassName}>
          {sections.map((section, index) => (
            <Fragment key={section.id}>
              {index > 0 ? (
                <Menu.Separator className={separatorClassName} />
              ) : null}
              <Menu.ItemGroup>
                <Menu.ItemGroupLabel className={groupLabelClassName}>
                  {section.label}
                </Menu.ItemGroupLabel>
                <Menu.RadioItemGroup
                  value={section.value}
                  onValueChange={(details) => section.onChange(details.value)}
                >
                  {section.options.map((option) => (
                    <Menu.RadioItem
                      key={option.value}
                      value={option.value}
                      disabled={option.disabled ?? false}
                      className={radioItemClassName}
                    >
                      <span className={indicatorClassName}>
                        <Menu.ItemIndicator>
                          <Check aria-hidden size={14} />
                        </Menu.ItemIndicator>
                      </span>
                      <span
                        className={flex({ direction: 'column', gap: '0.5' })}
                      >
                        <Menu.ItemText>{option.label}</Menu.ItemText>
                        {option.description ? (
                          <span className={optionDescriptionClassName}>
                            {option.description}
                          </span>
                        ) : null}
                      </span>
                    </Menu.RadioItem>
                  ))}
                </Menu.RadioItemGroup>
              </Menu.ItemGroup>
            </Fragment>
          ))}
        </Menu.Content>
      </Menu.Positioner>
    </Menu.Root>
  );
}

const DATA_SOURCE_SECTION_ID = 'data-source';

/**
 * The data-source section, which selects the provenance every endpoint answers
 * from.
 *
 * The href comes from the router so the param is spelled and ordered the way the
 * route tree would spell it, and the entry-time cleanup has nothing to rewrite.
 * It is then loaded as a document rather than navigated to: the flag is read
 * once per session on purpose (see `shared/lib/provenance`), so a client-side flip
 * would leave already-fetched series on the old provenance while new ones
 * arrive on the new one — a page mixing both, which is what `source` exists to
 * make impossible.
 */
export function useDataSourceSection(
  available: readonly Provenance[] = ['indexed', 'reference', 'both'],
): SettingsSection {
  const router = useRouter();
  const { provenance: shown } = useProvenanceView();

  // Shown disabled rather than hidden: a selector whose contents change with
  // the prime reads as a bug, and "greyed out" says something the absence of an
  // option does not.
  const option = (
    value: Provenance,
    label: string,
    description: string,
  ): SettingsOption => ({
    value,
    label,
    description,
    disabled: !available.includes(value),
  });

  return {
    id: DATA_SOURCE_SECTION_ID,
    label: 'Data source',
    value: shown,
    options: [
      option(
        'both',
        'Composite',
        'Verify and Legacy together, each position named once',
      ),
      option(
        'indexed',
        'Verify indexed',
        "Computed from Verify's own on-chain data",
      ),
      option(
        'reference',
        'Legacy reference',
        'As published by the legacy feed',
      ),
    ],
    onChange: (value) => {
      if (value === shown) {
        return;
      }

      const search = (previous: Record<string, unknown>) => ({
        ...previous,
        // The superseded spelling is dropped on the way out, so a link carrying
        // both cannot arrive contradicting itself.
        reference: undefined,
        source: value === 'both' ? undefined : value,
      });

      // A composite response holds both provenances, so narrowing it is a
      // projection of data already on the page: navigate and let the views
      // re-read the URL. Reloading would re-fetch what we have, and the
      // composite fetch is the slow one.
      if (canRestateProvenance) {
        void router.navigate({ to: '.', search });
        return;
      }

      // Nothing held to narrow, so this needs a fresh session: `PROVENANCE` is
      // fixed for the session on purpose, so that a cached series and the
      // request refreshing it cannot disagree about what they contain.
      globalThis.location.assign(
        router.buildLocation({ to: '.', search }).href,
      );
    },
  };
}
