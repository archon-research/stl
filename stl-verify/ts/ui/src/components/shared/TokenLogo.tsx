import { useEffect, useMemo, useState } from 'react';

import { css } from '#styled-system/css';

type TokenLogoProps = {
  address: string;
  chainId: number;
  isSelected?: boolean;
  symbol: string;
};

// 1inch Tokens Data API CDN
// Replaces deprecated Trust Wallet Assets CDN
// Format: https://tokens-data.1inch.io/images/{tokenAddress}.png
// See: https://github.com/1inch/token-icons for details
// Supports 40k+ tokens across multiple chains
const LOGO_CDN_BASE = 'https://tokens-data.1inch.io/images';

function buildTokenLogoUrl(_chainId: number, address: string): string | null {
  // 1inch CDN works consistently across all EVM chains for the same token address
  // Chain ID is not needed for the logo URL lookup
  return `${LOGO_CDN_BASE}/${address.toLowerCase()}.png`;
}

function TokenLogoFallback({
  isSelected = false,
  symbol,
}: Pick<TokenLogoProps, 'isSelected' | 'symbol'>) {
  return (
    <div
      className={css({
        width: '10',
        height: '10',
        borderRadius: 'full',
        bg: isSelected ? 'interactive.accent' : 'surface.subtle',
        color: isSelected ? 'white' : 'text.strong',
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontSize: 'xs',
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
  symbol,
}: TokenLogoProps) {
  const [hasImageError, setHasImageError] = useState(false);
  const logoUrl = useMemo(
    () => buildTokenLogoUrl(chainId, address),
    [address, chainId],
  );

  useEffect(() => {
    setHasImageError(false);
  }, [logoUrl]);

  if (!logoUrl || hasImageError) {
    return <TokenLogoFallback isSelected={isSelected} symbol={symbol} />;
  }

  return (
    <div
      className={css({
        width: '10',
        height: '10',
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
