import { useMemo } from 'react';

import { buildTokenLogoUrl } from '../../lib/logo-cdn';
import { LogoAvatar } from './LogoAvatar';

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

export function TokenLogo({
  address,
  chainId,
  isSelected = false,
  size = '10',
  symbol,
}: TokenLogoProps) {
  const resolvedSize = resolveTokenLogoSize(size);
  const logoUrl = useMemo(
    () => buildTokenLogoUrl(chainId, address),
    [address, chainId],
  );

  return (
    <LogoAvatar
      alt={`${symbol} logo`}
      fallbackColor="text.strong"
      fallbackText={symbol.slice(0, 2).toUpperCase()}
      imageUrl={logoUrl}
      isSelected={isSelected}
      sizePx={resolvedSize}
    />
  );
}
