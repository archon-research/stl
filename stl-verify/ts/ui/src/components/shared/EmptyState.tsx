import { Inbox } from 'lucide-react';

import { css } from '#styled-system/css';

type EmptyStateProps = {
  title: string;
  description: string;
  icon?: React.ReactNode;
  action?: React.ReactNode;
  size?: 'default' | 'compact';
  stretch?: boolean;
};

export function EmptyState({
  title,
  description,
  icon = <Inbox size={24} />,
  action,
  size = 'default',
  stretch = false,
}: EmptyStateProps) {
  const isCompact = size === 'compact';

  return (
    <div
      className={css({
        display: 'grid',
        gap: isCompact ? '2.5' : '3',
        justifyItems: 'center',
        textAlign: 'center',
        borderRadius: 'md',
        borderStyle: 'solid',
        borderWidth: '1px',
        borderColor: 'border.subtle',
        bg: 'surface.subtle',
        p: isCompact ? { base: '4', md: '5' } : { base: '8', md: '12' },
        width: stretch ? 'full' : undefined,
        maxWidth: stretch ? 'none' : '32rem',
        mx: stretch ? '0' : 'auto',
      })}
    >
      <div
        className={css({
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          width: isCompact ? '10' : '12',
          height: isCompact ? '10' : '12',
          borderRadius: 'full',
          bg: { _dark: 'gray.800', base: 'gray.100' },
          color: 'text.muted',
        })}
      >
        {icon}
      </div>
      <div className={css({ display: 'grid', gap: '2' })}>
        <h3
          className={css({
            m: 0,
            fontSize: isCompact ? 'md' : 'lg',
            fontWeight: 'semibold',
            color: 'text.strong',
          })}
        >
          {title}
        </h3>
        <p
          className={css({
            m: 0,
            fontSize: isCompact ? 'xs' : 'sm',
            color: 'text.muted',
            lineHeight: '1.6',
          })}
        >
          {description}
        </p>
      </div>
      {action ? <div className={css({ mt: '2' })}>{action}</div> : null}
    </div>
  );
}
