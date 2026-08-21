import { Avatar } from '@archon-research/design-system';
import { useEffect, useMemo, useState } from 'react';

import { logging } from '#src/lib/logging';
import { css, cx } from '#styled-system/css';

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

// One literal css() call per size: a token handed to css() as a variable emits
// no rule at all (DESIGN.md, silently-dropped CSS), so `width: size` only ever
// worked for sizes some other call site happened to declare literally.
const sizeClassNames: Record<PandaSizeToken, string> = {
  '4': css({ width: '4', height: '4' }),
  '5': css({ width: '5', height: '5' }),
  '6': css({ width: '6', height: '6' }),
  '7': css({ width: '7', height: '7' }),
  '8': css({ width: '8', height: '8' }),
  '9': css({ width: '9', height: '9' }),
  '10': css({ width: '10', height: '10' }),
  '11': css({ width: '11', height: '11' }),
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
      className={cx(
        sizePx ? undefined : sizeClassNames[size],
        css({
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
        }),
      )}
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
