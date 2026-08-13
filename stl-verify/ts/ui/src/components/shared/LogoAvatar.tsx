import { Avatar } from '@archon-research/design-system';
import { useEffect, useMemo, useState } from 'react';

import { logging } from '#src/lib/logging';
import { css } from '#styled-system/css';

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

type AvatarStatusChangeDetails = { status: 'loading' | 'loaded' | 'error' };

// Cache known-bad logo URLs to avoid repeated failed requests and duplicate warnings.
const failedLogoUrls = new Set<string>();

export function LogoAvatar({
  alt,
  fallbackText,
  imageUrl,
  isSelected = false,
  size = '5',
  sizePx,
  fallbackColor = 'text.default',
}: LogoAvatarProps) {
  const normalizedImageUrl = useMemo(() => imageUrl ?? null, [imageUrl]);
  const [hasImageError, setHasImageError] = useState<boolean>(
    normalizedImageUrl !== null && failedLogoUrls.has(normalizedImageUrl),
  );

  useEffect(() => {
    if (!normalizedImageUrl) {
      setHasImageError(true);
      return;
    }

    setHasImageError(failedLogoUrls.has(normalizedImageUrl));
  }, [normalizedImageUrl]);

  const sizingStyle = sizePx
    ? { width: `${sizePx}px`, height: `${sizePx}px` }
    : undefined;

  const shouldRenderImage = normalizedImageUrl !== null && !hasImageError;

  return (
    <Avatar.Root
      onStatusChange={(details: AvatarStatusChangeDetails) => {
        if (details.status === 'error' && normalizedImageUrl) {
          const isFirstFailure = !failedLogoUrls.has(normalizedImageUrl);
          failedLogoUrls.add(normalizedImageUrl);
          setHasImageError(true);

          if (!isFirstFailure) {
            return;
          }

          logging.warn('Logo image failed to load', {
            imageUrl: normalizedImageUrl,
            alt,
            fallbackText,
          });
        }
      }}
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
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
      })}
    >
      <Avatar.Fallback
        className={css({
          width: 'full',
          height: 'full',
          display: 'inline-flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: isSelected ? 'white' : fallbackColor,
          fontSize: '2xs',
          fontWeight: 'semibold',
          lineHeight: '1',
          textAlign: 'center',
          userSelect: 'none',
        })}
      >
        {fallbackText}
      </Avatar.Fallback>
      {shouldRenderImage ? (
        <Avatar.Image
          alt={alt}
          src={normalizedImageUrl}
          className={css({
            width: 'full',
            height: 'full',
            objectFit: 'cover',
            display: 'block',
          })}
        />
      ) : null}
    </Avatar.Root>
  );
}
