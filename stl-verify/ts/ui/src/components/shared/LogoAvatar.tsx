import { Avatar } from '@archon-research/design-system';
import { useMemo, useState } from 'react';

import { logging } from '#src/lib/logging';
import { css, cx } from '#styled-system/css';

type PandaSizeToken = '4' | '5' | '6' | '7' | '8' | '9' | '10' | '11';

type LogoAvatarBaseProps = {
  alt: string;
  fallbackText: string;
  imageUrl: string | null;
  /** Tried when `imageUrl` is null or fails, before the text fallback. */
  fallbackImageUrl?: string | null;
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

const sizeTokenPx: Record<PandaSizeToken, number> = {
  '4': 16,
  '5': 20,
  '6': 24,
  '7': 28,
  '8': 32,
  '9': 36,
  '10': 40,
  '11': 44,
};

/**
 * Initials sized to the disc rather than a fixed step: a fixed `2xs` nearly
 * touches the border of a 16px avatar, so the text fallback read as a smaller,
 * cramped sibling of a resolved logo in the same column.
 */
function fallbackFontSizePx(boxPx: number): number {
  return Math.max(7, Math.round(boxPx * 0.4));
}

export function LogoAvatar({
  alt,
  fallbackText,
  imageUrl,
  fallbackImageUrl = null,
  isSelected = false,
  size = '5',
  sizePx,
  fallbackColor = 'text.default',
}: LogoAvatarProps) {
  // `failedLogoUrls` is a module-level cache, so mutations are invisible to
  // React; this counter makes a load failure re-run the candidate pick.
  const [failCount, setFailCount] = useState(0);
  const activeImageUrl = useMemo(() => {
    void failCount;
    return (
      [imageUrl, fallbackImageUrl].find(
        (url): url is string => url != null && !failedLogoUrls.has(url),
      ) ?? null
    );
  }, [imageUrl, fallbackImageUrl, failCount]);

  const boxPx = sizePx ?? sizeTokenPx[size];
  const sizingStyle = sizePx
    ? { width: `${sizePx}px`, height: `${sizePx}px` }
    : undefined;

  const shouldRenderImage = activeImageUrl !== null;

  return (
    <Avatar.Root
      onStatusChange={(details: AvatarStatusChangeDetails) => {
        if (details.status === 'error' && activeImageUrl) {
          const isFirstFailure = !failedLogoUrls.has(activeImageUrl);
          failedLogoUrls.add(activeImageUrl);
          setFailCount((count) => count + 1);

          if (!isFirstFailure) {
            return;
          }

          logging.warn('Logo image failed to load', {
            imageUrl: activeImageUrl,
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
          fontWeight: 'semibold',
          lineHeight: '1',
          textAlign: 'center',
          userSelect: 'none',
        })}
        style={{ fontSize: `${fallbackFontSizePx(boxPx)}px` }}
      >
        {fallbackText}
      </Avatar.Fallback>
      {shouldRenderImage ? (
        <Avatar.Image
          alt={alt}
          src={activeImageUrl}
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
