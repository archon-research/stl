import {
  Badge,
  type BadgeColorPalette,
  ErrorState,
  Panel,
  SkeletonStack,
} from '@archon-research/design-system';
import { useQuery } from '@tanstack/react-query';
import { ChevronDown, ChevronUp } from 'lucide-react';

import { css } from '#styled-system/css';

import {
  formatDateTime,
  formatFreshnessLabel,
  formatUsdPrice,
} from '../../shared/lib/dashboard';
import { toQueryErrorMessage } from '../../shared/lib/errors';
import {
  dataSourcesQuery,
  DISABLED_ADDRESS,
  DISABLED_CHAIN_ID,
  tokenPriceQuery,
  tokenQuery,
  tokensQuery,
} from '../../shared/lib/queries';
import type { DataSource } from '../../shared/types/allocation';
import { LazyRegion, lazyChunk } from '../../shared/ui/LazyRegion';

const NO_SOURCES: DataSource[] = [];

// The panel is collapsed on first paint and its markdown is a module constant,
// so the renderer can arrive with the body instead of with the app shell.
const MethodologyMarkdown = lazyChunk(
  async () => (await import('./MethodologyMarkdown')).default,
);

// Anything not listed reads as neutral: the set of access models is open-ended
// (it comes from the data-sources API), so an unknown value must not be styled
// as though it carried a judgement.
const ACCESS_MODEL_PALETTE: Record<string, BadgeColorPalette> = {
  open: 'green',
  public: 'amber',
};

// Panel owns the frame and the section-label title; the metadata rows inside it
// still need their own stacking and type step, because `panel__body` is a plain
// block.
const metadataBodyClassName = css({
  display: 'grid',
  gap: '1.5',
  fontSize: 'xs',
  color: 'text.default',
});

type MethodologyPanelProps = {
  isOpen: boolean;
  onToggle: () => void;
  selectedTokenAddress?: string | null;
  selectedTokenSymbol?: string | null;
  selectedChainId?: number | null;
};

const METHODOLOGY_MARKDOWN = `## Internal Data (STL)
- Onchain allocation positions from Ethereum mainnet
- Risk calculations using Spark lending protocol parameters
- Oracle prices from Chainlink and Pyth networks

## Data Quality Notes
- Prices may lag 5–10 minutes depending on oracle update frequency
- Risk calculations are updated on each new block (Ethereum mainnet only)
- Activity/event feed surfaces indexed allocation events and supports URL filtering
`;

export function MethodologyPanel({
  isOpen,
  onToggle,
  selectedTokenAddress,
  selectedTokenSymbol,
  selectedChainId,
}: MethodologyPanelProps) {
  // Every read below is gated on the panel being open: it is collapsed on first
  // paint, and none of this belongs in the app's first wave of requests.
  const dataSourcesResult = useQuery({
    ...dataSourcesQuery(),
    enabled: isOpen,
  });
  const sources = dataSourcesResult.data ?? NO_SOURCES;
  const isLoading = isOpen && dataSourcesResult.isPending;
  const error = toQueryErrorMessage(dataSourcesResult.error);

  const chainId = selectedChainId ?? null;
  // Falsy rather than nullish: an empty address is as unaskable as an absent
  // one, and it would still build a request path that looks well-formed.
  const tokenAddress = selectedTokenAddress || null;
  const canLoadToken = isOpen && chainId !== null && tokenAddress !== null;

  const tokenResult = useQuery({
    ...tokenQuery(
      chainId ?? DISABLED_CHAIN_ID,
      tokenAddress ?? DISABLED_ADDRESS,
    ),
    enabled: canLoadToken,
  });
  const tokenPriceResult = useQuery({
    ...tokenPriceQuery(
      chainId ?? DISABLED_CHAIN_ID,
      tokenAddress ?? DISABLED_ADDRESS,
    ),
    enabled: canLoadToken,
  });
  // The catalogue slice this token's symbol belongs to, read only for its size.
  const catalogResult = useQuery({
    ...tokensQuery({
      chain_id: chainId ?? DISABLED_CHAIN_ID,
      symbol: selectedTokenSymbol ?? undefined,
      limit: 200,
    }),
    enabled: canLoadToken,
  });

  const selectedToken = tokenResult.data ?? null;
  const tokenPrice = tokenPriceResult.data ?? null;
  const catalogPreviewCount = catalogResult.data?.length ?? 0;
  const isTokenLoading =
    canLoadToken && (tokenResult.isPending || tokenPriceResult.isPending);
  // The catalogue count degrades to zero on its own, so only the two reads the
  // block is actually about can put it into its error state.
  const tokenError =
    toQueryErrorMessage(tokenResult.error) ??
    toQueryErrorMessage(tokenPriceResult.error);

  return (
    <div
      className={css({
        borderRadius: 'lg',
        // The scale has no 1px step: a hairline is a device-pixel rule
        // rather than a spacing decision.
        border: '[1px solid token(colors.border.hairline)]',
        bg: 'surface.default',
        overflow: 'hidden',
      })}
    >
      {/* Header */}
      <button
        type="button"
        onClick={onToggle}
        className={css({
          width: 'full',
          padding: '4',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          borderBottom: isOpen
            ? '[1px solid token(colors.border.hairline)]'
            : 'none',
          bg: 'surface.subtle',
          cursor: 'pointer',
          fontSize: 'sm',
          fontWeight: 'semibold',
          color: 'text.strong',
          transitionProperty: 'colors',
          transitionDuration: 'normal',
          _hover: { bg: 'surface.default' },
        })}
      >
        <span>Data Sources & Methodology</span>
        {isOpen ? (
          <ChevronUp className={css({ width: '4', height: '4' })} />
        ) : (
          <ChevronDown className={css({ width: '4', height: '4' })} />
        )}
      </button>

      {/* Content */}
      {isOpen && (
        <div
          // Below the smallest size step, and a scroll cap rather than a
          // spacing decision.
          className={css({ maxHeight: '[600px]', overflowY: 'auto' })}
        >
          {isLoading ? <SkeletonStack count={3} /> : null}

          {error ? (
            <div className={css({ p: '4' })}>
              <ErrorState
                title="Failed to load data sources"
                description="An error occurred while loading data-source transparency metadata."
                errorMessage={error}
                tone="critical"
                size="inline"
              />
            </div>
          ) : null}

          <div className={css({ p: '4', display: 'grid', gap: '6' })}>
            {/* Methodology text */}
            <div className={css({ display: 'grid', gap: '3' })}>
              <h3
                className={css({
                  fontSize: 'sm',
                  fontWeight: 'semibold',
                  color: 'text.strong',
                  mb: '2',
                })}
              >
                Methodology
              </h3>
              <div
                className={css({
                  fontSize: 'sm',
                  color: 'text.default',
                  // Off the scale, and set for prose rather than for a control.
                  lineHeight: '[1.7]',
                  '& p': { mb: '2' },
                  '& ul, & ol': { pl: '5', mb: '2' },
                  '& li': { mb: '1' },
                  '& h2, & h3': {
                    mt: '3',
                    mb: '2',
                    fontWeight: 'semibold',
                    color: 'text.strong',
                  },
                  '& code': {
                    fontFamily: 'mono',
                    fontSize: 'xs',
                    bg: 'surface.subtle',
                    px: '1',
                    borderRadius: 'sm',
                  },
                  '& a': {
                    color: 'text.link',
                    textDecoration: 'underline',
                  },
                })}
              >
                <LazyRegion
                  title="Methodology unavailable"
                  subject="methodology renderer"
                  impact="The rest of the panel is unaffected."
                  pending={<SkeletonStack count={3} />}
                >
                  <MethodologyMarkdown markdown={METHODOLOGY_MARKDOWN} />
                </LazyRegion>
              </div>
            </div>

            <div className={css({ display: 'grid', gap: '3' })}>
              <h3
                className={css({
                  fontSize: 'sm',
                  fontWeight: 'semibold',
                  color: 'text.strong',
                  mb: '2',
                })}
              >
                Token Catalog & Price
              </h3>

              {!selectedTokenAddress ? (
                <p
                  className={css({
                    m: '0',
                    fontSize: 'xs',
                    color: 'text.default',
                  })}
                >
                  Select a receipt token to view matching token-catalog metadata
                  and latest indexed token price.
                </p>
              ) : null}

              {selectedTokenAddress && isTokenLoading ? (
                <SkeletonStack count={2} />
              ) : null}

              {selectedTokenAddress && tokenError ? (
                <p
                  className={css({
                    m: '0',
                    fontSize: 'xs',
                    color: 'text.warning',
                  })}
                >
                  Failed to load token transparency metadata: {tokenError}
                </p>
              ) : null}

              {selectedTokenAddress && !isTokenLoading && selectedToken ? (
                <Panel
                  surface="recessed"
                  density="compact"
                  title="Catalog Token"
                >
                  <div className={metadataBodyClassName}>
                    <div>
                      {selectedToken.symbol ?? 'Unknown'} (ID {selectedToken.id}
                      )
                    </div>
                    <div>Address: {selectedToken.address}</div>
                    <div>Chain: {selectedToken.chain_id}</div>
                    <div>Decimals: {selectedToken.decimals ?? 'Unknown'}</div>
                    <div>
                      Catalog updated:{' '}
                      {formatDateTime(selectedToken.updated_at)}
                    </div>
                    <div>
                      Metadata keys:{' '}
                      {selectedToken.metadata
                        ? Object.keys(selectedToken.metadata).join(', ') ||
                          'None'
                        : 'None'}
                    </div>
                    <div>
                      Matching catalog rows (chain/symbol preview):{' '}
                      {catalogPreviewCount}
                    </div>
                  </div>
                </Panel>
              ) : null}

              {selectedTokenAddress && !isTokenLoading && tokenPrice ? (
                <Panel surface="raised" density="compact" title="Latest Price">
                  <div className={metadataBodyClassName}>
                    <div>
                      {tokenPrice.is_stale || tokenPrice.price_usd == null
                        ? 'Price unavailable'
                        : formatUsdPrice(tokenPrice.price_usd)}
                    </div>
                    {!tokenPrice.is_stale && (
                      <>
                        <div>
                          Source:{' '}
                          {tokenPrice.source_display_name ??
                            tokenPrice.source_name}{' '}
                          ({tokenPrice.source_type})
                        </div>
                        <div>Source ID: {tokenPrice.source_id}</div>
                      </>
                    )}
                    {tokenPrice.timestamp != null && (
                      <div>
                        Timestamp: {formatDateTime(tokenPrice.timestamp)} (
                        {formatFreshnessLabel(tokenPrice.timestamp)})
                      </div>
                    )}
                    {tokenPrice.staleness_seconds != null && (
                      <div>Staleness: {tokenPrice.staleness_seconds}s</div>
                    )}
                  </div>
                </Panel>
              ) : null}
            </div>

            {/* Data Sources table */}
            <div className={css({ display: 'grid', gap: '3' })}>
              <h3
                className={css({
                  fontSize: 'sm',
                  fontWeight: 'semibold',
                  color: 'text.strong',
                  mb: '2',
                })}
              >
                Data Sources ({sources.length})
              </h3>

              <div
                className={css({
                  overflowX: 'auto',
                  borderRadius: 'md',
                  border: '[1px solid token(colors.border.hairline)]',
                })}
              >
                <table
                  className={css({
                    width: 'full',
                    borderCollapse: 'collapse',
                    fontSize: 'xs',
                  })}
                >
                  <thead>
                    <tr className={css({ bg: 'surface.subtle' })}>
                      {['Source', 'Host', 'Role', 'Access'].map((h) => (
                        <th
                          key={h}
                          className={css({
                            // Was `padding: '2 3'`, which is no token: Panda
                            // unitised it and the header cells shipped at 2px.
                            py: '2',
                            px: '3',
                            textAlign: 'left',
                            fontWeight: 'semibold',
                            color: 'text.muted',
                            borderBottom:
                              '[1px solid token(colors.border.hairline)]',
                          })}
                        >
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sources.map((source) => (
                      <tr
                        key={source.name}
                        className={css({ _hover: { bg: 'surface.subtle' } })}
                      >
                        <td
                          className={css({
                            padding: '3',
                            borderBottom:
                              '[1px solid token(colors.border.hairline)]',
                            fontWeight: 'semibold',
                            color: 'text.strong',
                          })}
                        >
                          {source.name}
                        </td>
                        <td
                          className={css({
                            padding: '3',
                            borderBottom:
                              '[1px solid token(colors.border.hairline)]',
                            color: 'text.default',
                          })}
                        >
                          {source.host}
                        </td>
                        <td
                          className={css({
                            padding: '3',
                            borderBottom:
                              '[1px solid token(colors.border.hairline)]',
                            color: 'text.default',
                          })}
                        >
                          {source.role}
                        </td>
                        <td
                          className={css({
                            padding: '3',
                            borderBottom:
                              '[1px solid token(colors.border.hairline)]',
                          })}
                        >
                          <Badge
                            colorPalette={
                              ACCESS_MODEL_PALETTE[source.access_model] ??
                              'neutral'
                            }
                            variant="subtle"
                            size="sm"
                          >
                            {source.access_model}
                          </Badge>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* Caveats */}
              {sources.some((s) => s.caveat) && (
                <div
                  className={css({
                    mt: '4',
                    pt: '4',
                    borderTop: '[1px solid token(colors.border.hairline)]',
                    display: 'grid',
                    gap: '2',
                  })}
                >
                  <h4
                    className={css({
                      fontSize: 'xs',
                      fontWeight: 'semibold',
                      color: 'text.strong',
                      textTransform: 'uppercase',
                    })}
                  >
                    Caveats
                  </h4>
                  {sources
                    .filter((s) => s.caveat)
                    .map((source) => (
                      <div
                        key={source.name}
                        className={css({
                          fontSize: 'xs',
                          color: 'text.default',
                          display: 'flex',
                          gap: '2',
                        })}
                      >
                        <span className={css({ fontWeight: 'semibold' })}>
                          {source.name}:
                        </span>
                        <span>{source.caveat}</span>
                      </div>
                    ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
