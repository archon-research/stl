import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import type { Star } from '../../types/allocation';

type TopBarProps = {
  selectedStar: Star | null;
  starCount: number;
};

function formatAddress(address: string): string {
  if (address.length <= 18) {
    return address;
  }

  return `${address.slice(0, 10)}…${address.slice(-6)}`;
}

function StatusPill({ label }: { label: string }) {
  return (
    <span
      className={css({
        display: 'inline-flex',
        alignItems: 'center',
        px: '3',
        py: '1.5',
        borderRadius: 'full',
        borderWidth: '1px',
        borderStyle: 'solid',
        borderColor: 'border.subtle',
        bg: 'surface.subtle',
        fontSize: 'xs',
        color: 'text.muted',
      })}
    >
      {label}
    </span>
  );
}

export function TopBar({ selectedStar, starCount }: TopBarProps) {
  return (
    <div
      className={flex({
        align: 'center',
        justify: 'space-between',
        gap: '4',
        wrap: 'wrap',
      })}
    >
      <div className={css({ display: 'grid', gap: '1' })}>
        <p
          className={css({
            m: 0,
            fontSize: 'xs',
            textTransform: 'uppercase',
            letterSpacing: '0.16em',
            color: 'text.muted',
          })}
        >
          Workspace shell
        </p>
        <div className={flex({ align: 'baseline', gap: '3', wrap: 'wrap' })}>
          <h1
            className={css({
              m: 0,
              fontSize: 'lg',
              lineHeight: 'tight',
              color: 'text.strong',
            })}
          >
            {selectedStar ? selectedStar.name : 'Choose a star'}
          </h1>
          {selectedStar ? (
            <span className={css({ fontSize: 'xs', color: 'text.muted' })}>
              {formatAddress(selectedStar.address)}
            </span>
          ) : null}
        </div>
      </div>

      <div className={flex({ align: 'center', gap: '2', wrap: 'wrap' })}>
        <StatusPill label={`${starCount} stars`} />
        <StatusPill label="URL state active" />
        <StatusPill label="Shared http client" />
      </div>
    </div>
  );
}
