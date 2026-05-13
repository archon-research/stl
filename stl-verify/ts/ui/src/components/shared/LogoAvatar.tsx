import { useEffect, useState } from 'react';

import { css } from '#styled-system/css';

type LogoAvatarProps = {
  alt: string;
  fallbackText: string;
  imageUrl: string | null;
  isSelected?: boolean;
  size?: string;
  sizePx?: number;
  fallbackColor?: 'text.default' | 'text.strong';
};

export function LogoAvatar({
  alt,
  fallbackText,
  imageUrl,
  isSelected = false,
  size = '5',
  sizePx,
  fallbackColor = 'text.default',
}: LogoAvatarProps) {
  const [hasImageError, setHasImageError] = useState(false);

  useEffect(() => {
    setHasImageError(false);
  }, [imageUrl]);

  const sizingStyle = sizePx
    ? { width: `${sizePx}px`, height: `${sizePx}px` }
    : undefined;

  return (
    <div
      style={sizingStyle}
      className={css({
        width: sizePx ? undefined : size,
        height: sizePx ? undefined : size,
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
            color: isSelected ? 'white' : fallbackColor,
            fontSize: '2xs',
            fontWeight: 'semibold',
            userSelect: 'none',
          })}
        >
          {fallbackText}
        </div>
      ) : (
        <img
          alt={alt}
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
