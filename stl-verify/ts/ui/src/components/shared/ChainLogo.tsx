import { useEffect, useMemo, useState } from 'react';

import { css } from '#styled-system/css';

import { buildChainLogoUrl } from '../../lib/logo-cdn';

type ChainLogoProps = {
  chainId: number;
  label?: string;
  size?: '4' | '5' | '6' | '7' | '8' | '9' | '10';
};

function fallbackText(label: string | undefined, chainId: number): string {
  if (label && label.trim().length > 0) {
    return label.trim().slice(0, 1).toUpperCase();
  }

  return String(chainId).slice(0, 1);
}

export function ChainLogo({ chainId, label, size = '5' }: ChainLogoProps) {
  const [hasImageError, setHasImageError] = useState(false);
  const imageUrl = useMemo(() => buildChainLogoUrl(chainId), [chainId]);

  useEffect(() => {
    setHasImageError(false);
  }, [imageUrl]);

  return (
    <div
      className={css({
        width: size,
        height: size,
        borderRadius: 'full',
        overflow: 'hidden',
        bg: 'surface.subtle',
        borderWidth: '1px',
        borderStyle: 'solid',
        borderColor: 'border.subtle',
        flexShrink: 0,
      })}
    >
      {!imageUrl || hasImageError ? (
        <div
          className={css({
            width: 'full',
            height: 'full',
            display: 'inline-flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: 'text.default',
            fontSize: '2xs',
            fontWeight: 'semibold',
          })}
        >
          {fallbackText(label, chainId)}
        </div>
      ) : (
        <img
          alt={`${label ?? `Chain ${chainId}`} logo`}
          className={css({
            width: 'full',
            height: 'full',
            objectFit: 'cover',
            display: 'block',
          })}
          onError={() => setHasImageError(true)}
          src={imageUrl}
        />
      )}
    </div>
  );
}
