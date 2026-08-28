import { useMemo } from 'react';

import { buildChainLogoUrl } from '../lib/logo-cdn';
import { LogoAvatar } from './LogoAvatar';

type ChainLogoProps = {
  chainId: number | null;
  label?: string;
  size?: '4' | '5' | '6' | '7' | '8' | '9' | '10';
};

function fallbackText(
  label: string | undefined,
  chainId: number | null,
): string {
  if (label && label.trim().length > 0) {
    return label.trim().slice(0, 1).toUpperCase();
  }

  return chainId === null ? '?' : String(chainId).slice(0, 1);
}

export function ChainLogo({ chainId, label, size = '5' }: ChainLogoProps) {
  const imageUrl = useMemo(() => buildChainLogoUrl(chainId), [chainId]);

  return (
    <LogoAvatar
      alt={`${label ?? (chainId === null ? 'Unknown chain' : `Chain ${chainId}`)} logo`}
      fallbackText={fallbackText(label, chainId)}
      imageUrl={imageUrl}
      size={size}
    />
  );
}
