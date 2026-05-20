import { useMemo } from 'react';

import { buildProtocolLogoUrl } from '../../lib/logo-cdn';
import { LogoAvatar } from './LogoAvatar';

type ProtocolLogoProps = {
  protocolName: string;
  isSelected?: boolean;
  size?: '4' | '5' | '6' | '7' | '8' | '9' | '10' | '11';
};

function fallbackText(protocolName: string): string {
  const compact = protocolName.trim();
  if (!compact) {
    return '?';
  }

  return compact.slice(0, 2).toUpperCase();
}

export function ProtocolLogo({
  protocolName,
  isSelected = false,
  size = '5',
}: ProtocolLogoProps) {
  const imageUrl = useMemo(
    () => buildProtocolLogoUrl(protocolName),
    [protocolName],
  );

  return (
    <LogoAvatar
      alt={`${protocolName} logo`}
      fallbackText={fallbackText(protocolName)}
      imageUrl={imageUrl}
      isSelected={isSelected}
      size={size}
    />
  );
}
