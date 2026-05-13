import { useEffect, useState } from 'react';

import { css } from '#styled-system/css';
import { logging } from '#src/lib/logging';

type PandaSizeToken = '4' | '5' | '6' | '7' | '8' | '9' | '10' | '11';

type LogoAvatarBaseProps = {
  alt: string;
  fallbackText: string;
  imageUrl: string | null;
  isSelected?: boolean;
  fallbackColor?: 'text.default' | 'text.strong';
};

type LogoAvatarProps =
  | (LogoAvatarBaseProps & { size?: PandaSizeToken; sizePx?: never })
  | (LogoAvatarBaseProps & { size?: never; sizePx: number });

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
          onError={() => {
            setHasImageError(true);
            logging.warn('Logo image failed to load', {
              imageUrl,
              alt,
              fallbackText,
            });
          }}
          src={imageUrl}
        />
      )}
    </div>
  );
}
