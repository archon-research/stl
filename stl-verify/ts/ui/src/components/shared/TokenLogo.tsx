import { useEffect, useMemo, useState } from 'react';

import { css } from '#styled-system/css';

import { buildTokenLogoUrl } from '../../lib/logo-cdn';

type TokenLogoProps = {
  address: string | null | undefined;
  chainId: number;
  isSelected?: boolean;
  size?: '6' | '7' | '8' | '9' | '10';
  symbol: string;
};

const TOKEN_LOGO_SIZE_MAP: Record<
  NonNullable<TokenLogoProps['size']>,
  number
> = {
  6: 16,
  7: 18,
  8: 20,
  9: 24,
  10: 28,
};

function resolveTokenLogoSize(
  size: NonNullable<TokenLogoProps['size']>,
): number {
  return TOKEN_LOGO_SIZE_MAP[size];
}

type TokenLogoFallbackProps = {
  isSelected?: boolean;
  size: number;
  symbol: string;
};

function TokenLogoFallback({
  isSelected = false,
  size,
  symbol,
}: TokenLogoFallbackProps) {
  return (
    <div
      style={{ width: `${size}px`, height: `${size}px` }}
      className={css({
        borderRadius: 'full',
        bg: isSelected ? 'interactive.accent' : 'surface.subtle',
        color: isSelected ? 'white' : 'text.strong',
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontSize: '2xs',
        fontWeight: 'semibold',
        flexShrink: 0,
        overflow: 'hidden',
      })}
    >
      {symbol.slice(0, 2).toUpperCase()}
    </div>
  );
}

export function TokenLogo({
  address,
  chainId,
  isSelected = false,
  size = '10',
  symbol,
}: TokenLogoProps) {
  const [hasImageError, setHasImageError] = useState(false);
  const resolvedSize = resolveTokenLogoSize(size);
  const logoUrl = useMemo(
    () => buildTokenLogoUrl(chainId, address),
    [address, chainId],
  );

  useEffect(() => {
    setHasImageError(false);
  }, [logoUrl]);

  if (!logoUrl || hasImageError) {
    return (
      <TokenLogoFallback
        isSelected={isSelected}
        size={resolvedSize}
        symbol={symbol}
      />
    );
  }

  return (
    <div
      style={{ width: `${resolvedSize}px`, height: `${resolvedSize}px` }}
      className={css({
        borderRadius: 'full',
        overflow: 'hidden',
        flexShrink: 0,
        bg: 'surface.subtle',
        borderWidth: '1px',
        borderStyle: 'solid',
        borderColor: isSelected ? 'interactive.accent' : 'border.subtle',
      })}
    >
      <img
        alt={`${symbol} logo`}
        className={css({
          width: 'full',
          height: 'full',
          objectFit: 'cover',
          display: 'block',
        })}
        onError={() => setHasImageError(true)}
        src={logoUrl}
      />
    </div>
  );
}
