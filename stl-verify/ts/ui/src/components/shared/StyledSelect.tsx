import type { ReactNode, SelectHTMLAttributes } from 'react';

import { css } from '#styled-system/css';

type StyledSelectProps = SelectHTMLAttributes<HTMLSelectElement> & {
  children: ReactNode;
  className?: string;
};

function SelectChevron() {
  return (
    <svg
      aria-hidden="true"
      viewBox="0 0 16 16"
      className={css({
        position: 'absolute',
        top: '50%',
        right: '3',
        width: '4',
        height: '4',
        color: 'text.muted',
        pointerEvents: 'none',
        transform: 'translateY(-50%)',
      })}
    >
      <path
        d="M4 6.5L8 10l4-3.5"
        fill="none"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.5"
      />
    </svg>
  );
}

export function StyledSelect({
  children,
  className,
  ...props
}: StyledSelectProps) {
  return (
    <div
      className={css({
        position: 'relative',
        display: 'inline-flex',
        alignItems: 'center',
        width: '100%',
      })}
    >
      <select
        {...props}
        className={
          css({
            width: '100%',
            minWidth: '0',
            h: '9',
            borderWidth: '1px',
            borderStyle: 'solid',
            borderColor: 'border.subtle',
            borderRadius: 'md',
            pl: '3',
            pr: '10',
            bg: 'surface.default',
            color: 'text.default',
            textStyle: 'bodySm',
            outline: 'none',
            appearance: 'none',
            _focusVisible: {
              borderColor: 'border.default',
              boxShadow: '0 0 0 1px token(colors.border.default)',
            },
            _disabled: {
              opacity: 0.65,
              cursor: 'not-allowed',
            },
          }) + (className ? ` ${className}` : '')
        }
      >
        {children}
      </select>
      <SelectChevron />
    </div>
  );
}
