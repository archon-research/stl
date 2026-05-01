import { Inbox } from 'lucide-react';

import { css } from '#styled-system/css';

type EmptyStateProps = {
  title: string;
  description: string;
  icon?: React.ReactNode;
  action?: React.ReactNode;
};

export function EmptyState({
  title,
  description,
  icon = <Inbox size={24} />,
  action,
}: EmptyStateProps) {
  return (
    <div
      className={css({
        display: 'grid',
        gap: '3',
        justifyItems: 'center',
        textAlign: 'center',
        borderRadius: 'md',
        borderStyle: 'solid',
        borderWidth: '1px',
        borderColor: 'border.subtle',
        bg: 'surface.subtle',
        p: { base: '8', md: '12' },
        maxWidth: '32rem',
        mx: 'auto',
      })}
    >
      <div
        className={css({
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          width: '12',
          height: '12',
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
            fontSize: 'lg',
            fontWeight: 'semibold',
            color: 'text.strong',
          })}
        >
          {title}
        </h3>
        <p
          className={css({
            m: 0,
            fontSize: 'sm',
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
