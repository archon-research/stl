import { useEffect, useMemo, useState } from 'react';

import { css } from '#styled-system/css';

import { buildProtocolLogoUrl } from '../../lib/logo-cdn';

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
  const [hasImageError, setHasImageError] = useState(false);
  const imageUrl = useMemo(() => buildProtocolLogoUrl(protocolName), [protocolName]);

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
        bg: isSelected ? 'interactive.accent' : 'surface.subtle',
        borderWidth: '1px',
        borderStyle: 'solid',
        borderColor: isSelected ? 'interactive.accent' : 'border.subtle',
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
            color: isSelected ? 'white' : 'text.default',
            fontSize: '2xs',
            fontWeight: 'semibold',
          })}
        >
          {fallbackText(protocolName)}
        </div>
      ) : (
        <img
          alt={`${protocolName} logo`}
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
