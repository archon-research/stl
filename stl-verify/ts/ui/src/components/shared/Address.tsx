import { css } from '#styled-system/css';

type AddressProps = {
  value: string;
  truncate?: boolean;
  truncateLength?: { start: number; end: number };
  className?: string;
};

function formatAddress(
  value: string,
  truncateLength: { start: number; end: number } = { start: 8, end: 4 },
): string {
  const totalLength = truncateLength.start + truncateLength.end + 3;
  if (value.length <= totalLength) {
    return value;
  }

  return `${value.slice(0, truncateLength.start)}...${value.slice(-truncateLength.end)}`;
}

export function Address({
  value,
  truncate = true,
  truncateLength = { start: 8, end: 4 },
  className,
}: AddressProps) {
  const displayValue = truncate ? formatAddress(value, truncateLength) : value;

  return (
    <span
      title={value}
      className={
        className ??
        css({
          fontFamily: 'mono',
          fontSize: 'xs',
          color: 'text.muted',
        })
      }
    >
      {displayValue}
    </span>
  );
}
