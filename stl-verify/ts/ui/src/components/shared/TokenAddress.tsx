import { Copy, ExternalLink } from 'lucide-react';
import { useRef, useState } from 'react';
import type React from 'react';

import { css } from '#styled-system/css';

import { getExplorerUrl } from '../../lib/dashboard';

type TokenAddressProps = {
  address: string;
  chainId?: number;
  /** Optional inline styles applied to the button (use for font-size overrides etc.) */
  style?: React.CSSProperties;
  /** Optional custom className to override defaults */
  className?: string;
};

/**
 * Truncates address with ellipsis in the middle for responsive layouts.
 * E.g., "0x1234567890abcdef1234567890abcdef12345678" → "0x1234...5678"
 */
function truncateMiddle(address: string, maxLength = 12): string {
  if (address.length <= maxLength) {
    return address;
  }
  const start = Math.ceil(maxLength / 2);
  const end = address.length - Math.floor(maxLength / 2) + 2;
  return `${address.slice(0, start)}...${address.slice(end)}`;
}

export function TokenAddress({ address, chainId = 1, style, className }: TokenAddressProps) {
  const [isMenuOpen, setIsMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  const explorerUrl = getExplorerUrl(chainId, address);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(address);
    } catch {
      // Fallback: create a temporary input
      const textarea = document.createElement('textarea');
      textarea.value = address;
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand('copy');
      document.body.removeChild(textarea);
    }
    setIsMenuOpen(false);
  };

  const handleViewExplorer = () => {
    if (explorerUrl) {
      window.open(explorerUrl, '_blank', 'noopener,noreferrer');
    }
    setIsMenuOpen(false);
  };

  return (
    <div
      className={css({
        position: 'relative',
        display: 'inline-flex',
        alignItems: 'center',
      })}
      ref={menuRef}
    >
      {/* Clickable address display */}
      <button
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          setIsMenuOpen(!isMenuOpen);
        }}
        title={address}
        style={style}
        className={
          className ??
          css({
            flex: '1 1 0',
            minWidth: '0', // Allows flex shrinking to work
            fontFamily: 'mono',
            fontSize: '2xs',
            color: { base: 'blue.500', _dark: 'blue.400' },
            bg: 'transparent',
            border: 'none',
            cursor: 'pointer',
            padding: '0',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            textAlign: 'left',
            _hover: {
              color: 'interactive.accent',
            },
            _focus: {
              outline: '2px solid',
              outlineColor: 'interactive.accent',
              outlineOffset: '1px',
            },
          })
        }
      >
        {truncateMiddle(address, 20)}
      </button>

      {/* Context Menu */}
      {isMenuOpen && (
        <div
          className={css({
            position: 'absolute',
            top: '100%',
            right: '0',
            mt: '1',
            bg: 'surface.default',
            borderRadius: 'md',
            borderStyle: 'solid',
            borderWidth: '1px',
            borderColor: 'border.subtle',
            boxShadow: '0 4px 12px rgba(15, 23, 42, 0.1)',
            zIndex: '50',
            minWidth: '200px',
            overflow: 'hidden',
          })}
        >
          <button
            type="button"
            onClick={handleViewExplorer}
            disabled={!explorerUrl}
            className={css({
              display: 'flex',
              alignItems: 'center',
              gap: '2',
              width: '100%',
              px: '3',
              py: '2',
              fontSize: 'sm',
              fontWeight: 'medium',
              color: explorerUrl ? 'text.strong' : 'text.muted',
              bg: 'transparent',
              border: 'none',
              cursor: explorerUrl ? 'pointer' : 'not-allowed',
              _hover: explorerUrl
                ? {
                    bg: 'surface.subtle',
                  }
                : undefined,
            })}
          >
            <ExternalLink size={16} />
            View on Etherscan
          </button>

          <button
            type="button"
            onClick={handleCopy}
            className={css({
              display: 'flex',
              alignItems: 'center',
              gap: '2',
              width: '100%',
              px: '3',
              py: '2',
              fontSize: 'sm',
              fontWeight: 'medium',
              color: 'text.strong',
              bg: 'transparent',
              border: 'none',
              cursor: 'pointer',
              _hover: {
                bg: 'surface.subtle',
              },
            })}
          >
            <Copy size={16} />
            Copy address
          </button>
        </div>
      )}

      {/* Close menu when clicking outside */}
      {isMenuOpen && (
        <div
          className={css({
            position: 'fixed',
            inset: '0',
            zIndex: '40',
          })}
          onClick={() => setIsMenuOpen(false)}
          onKeyDown={(e) => {
            if (e.key === 'Escape') {
              setIsMenuOpen(false);
            }
          }}
          role="presentation"
        />
      )}
    </div>
  );
}
