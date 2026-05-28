import { Menu } from '@archon-research/design-system';
import { Copy, ExternalLink } from 'lucide-react';
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
  const monoStyle: React.CSSProperties = {
    ...style,
    fontFamily: 'var(--fonts-mono), monospace',
  };

  const emptyClassName = css({
    fontFamily: 'mono',
    fontSize: type === 'tx' ? 'xs' : '2xs',
    color: 'text.muted',
  });

  const triggerClassName = css({
    flex: '1 1 0',
    minWidth: '0',
    minHeight: '11',
    fontFamily: 'mono',
    fontSize: type === 'tx' ? 'xs' : '2xs',
    color: { base: 'blue.500', _dark: 'blue.400' },
    bg: 'transparent',
    border: 'none',
    cursor: 'pointer',
    paddingInline: '1',
    paddingBlock: '1',
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
  });

  if (!address) {
    return (
      <span
        style={monoStyle}
        className={className ? `${emptyClassName} ${className}` : emptyClassName}
      >
        —
      </span>
    );
  }

  const explorerUrl = getExplorerUrl(chainId, address, type);

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
  };

  const handleViewExplorer = () => {
    if (explorerUrl) {
      window.open(explorerUrl, '_blank', 'noopener,noreferrer');
    }
  };

  return (
    <div
      className={css({
        position: 'relative',
        display: 'inline-flex',
        alignItems: 'center',
      })}
    >
      <Menu.Root positioning={{ placement: 'bottom-end' }}>
        <Menu.Trigger asChild>
          <button
            type="button"
            title={address}
            style={monoStyle}
            className={
              className ? `${triggerClassName} ${className}` : triggerClassName
            }
          >
            {truncateMiddle(address, 20)}
          </button>
        </Menu.Trigger>
        <Menu.Positioner>
          <Menu.Content
            className={css({
              bg: 'surface.default',
              borderRadius: 'md',
              borderStyle: 'solid',
              borderWidth: '1px',
              borderColor: 'border.subtle',
              boxShadow: 'lg',
              minWidth: '200px',
              overflow: 'hidden',
              zIndex: '50',
            })}
          >
            <Menu.Item
              value="view-explorer"
              disabled={!explorerUrl}
              onSelect={handleViewExplorer}
              className={css({
                display: 'flex',
                alignItems: 'center',
                gap: '2',
                width: '100%',
                px: '3',
                py: '2.5',
                fontSize: 'sm',
                fontWeight: 'medium',
                color: explorerUrl ? 'text.strong' : 'text.muted',
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
            </Menu.Item>

            <Menu.Item
              value="copy-address"
              onSelect={handleCopy}
              className={css({
                display: 'flex',
                alignItems: 'center',
                gap: '2',
                width: '100%',
                px: '3',
                py: '2.5',
                fontSize: 'sm',
                fontWeight: 'medium',
                color: 'text.strong',
                cursor: 'pointer',
                _hover: {
                  bg: 'surface.subtle',
                },
              })}
            >
              <Copy size={16} />
              Copy address
            </Menu.Item>
          </Menu.Content>
        </Menu.Positioner>
      </Menu.Root>
    </div>
  );
}
