import { Avatar } from '@archon-research/design-system';

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

export function LogoAvatar({
  alt,
  fallbackText,
  imageUrl,
  isSelected = false,
  size = '5',
  sizePx,
  fallbackColor = 'text.default',
}: LogoAvatarProps) {
  const sizingStyle = sizePx
    ? { width: `${sizePx}px`, height: `${sizePx}px` }
    : undefined;

  return (
    <Avatar.Root
      onStatusChange={(details: AvatarStatusChangeDetails) => {
        if (details.status === 'error' && imageUrl) {
          logging.warn('Logo image failed to load', {
            imageUrl,
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
          userSelect: 'none',
        })}
      >
        {fallbackText}
      </Avatar.Fallback>
      {imageUrl ? (
        <Avatar.Image
          alt={alt}
          src={imageUrl}
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
