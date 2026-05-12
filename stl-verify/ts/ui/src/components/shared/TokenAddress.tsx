import { Copy, ExternalLink } from 'lucide-react';
import { useEffect, useState } from 'react';
import type React from 'react';

import { css } from '#styled-system/css';

import { getExplorerUrl } from '../../lib/dashboard';

type TokenAddressProps = {
  address: string | null | undefined;
  chainId?: number;
  /** Type of address: 'address' for contract/EOA, 'tx' for transaction hash */
  type?: 'address' | 'tx';
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
  const charsToShow = maxLength - 3; // Account for "..."
  const frontChars = Math.ceil(charsToShow / 2);
  const backChars = Math.floor(charsToShow / 2);
  return `${address.slice(0, frontChars)}...${address.slice(-backChars)}`;
}

export function TokenAddress({
  address,
  chainId = 1,
  type = 'address',
  style,
  className,
}: TokenAddressProps) {
  const [isMenuOpen, setIsMenuOpen] = useState(false);

  if (!address) {
    return (
      <span
        style={style}
        className={css({
          fontFamily: 'mono',
          fontSize: type === 'tx' ? 'xs' : '2xs',
          color: 'text.muted',
        })}
      >
        —
      </span>
    );
  }

  const explorerUrl = getExplorerUrl(chainId, address, type);

  useEffect(() => {
    if (!isMenuOpen) {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setIsMenuOpen(false);
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [isMenuOpen]);

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
    >
      {/* Clickable address display */}
      <button
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          setIsMenuOpen(!isMenuOpen);
        }}
        title={address}
        aria-haspopup="menu"
        aria-expanded={isMenuOpen}
        style={style}
        className={
          className ??
          css({
            flex: '1 1 0',
            minWidth: '0', // Allows flex shrinking to work
            fontFamily: 'mono',
            fontSize: type === 'tx' ? 'xs' : '2xs',
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
            View on explorer
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
          role="presentation"
        />
      )}
    </div>
  );
}
