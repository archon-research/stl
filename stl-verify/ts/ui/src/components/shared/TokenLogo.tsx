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

const TOKEN_LOGO_SIZE_PX: Record<NonNullable<TokenLogoProps['size']>, number> = {
  6: 16,
  7: 18,
  8: 20,
  9: 24,
  10: 28,
};

export function TokenLogo({
  address,
  chainId,
  isSelected = false,
  size = '10',
  symbol,
}: TokenLogoProps) {
  const resolvedSize = TOKEN_LOGO_SIZE_PX[size];
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
