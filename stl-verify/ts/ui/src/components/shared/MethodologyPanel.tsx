import { SkeletonStack } from '@archon-research/design-system';
import { ChevronDown, ChevronUp } from 'lucide-react';
import { useEffect, useState } from 'react';
import ReactMarkdown from 'react-markdown';

import { css } from '#styled-system/css';

import {
  getDataSources,
  getToken,
  getTokenPrice,
  getTokens,
} from '../../lib/api';
import {
  formatDateTime,
  formatFreshnessLabel,
  formatUsdValue,
} from '../../lib/dashboard';
import { isAbortError, toErrorMessage } from '../../lib/errors';
import { logging } from '../../lib/logging';
import type { DataSource, Token, TokenPrice } from '../../types/allocation';
import { ErrorState } from './index';

type MethodologyPanelProps = {
  isOpen: boolean;
  onToggle: () => void;
  selectedTokenAddress?: string | null;
  selectedTokenSymbol?: string | null;
  selectedChainId?: number;
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
  const [sources, setSources] = useState<DataSource[]>([]);
  const [methodologyText] = useState<string>(METHODOLOGY_MARKDOWN);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [catalogPreviewCount, setCatalogPreviewCount] = useState<number>(0);
  const [selectedToken, setSelectedToken] = useState<Token | null>(null);
  const [tokenPrice, setTokenPrice] = useState<TokenPrice | null>(null);
  const [isTokenLoading, setIsTokenLoading] = useState(false);
  const [tokenError, setTokenError] = useState<string | null>(null);

  useEffect(() => {
    if (!isOpen) {
      return;
    }

    const abortController = new AbortController();

    async function fetchSources() {
      setIsLoading(true);
      setError(null);

      try {
        const response = await getDataSources(abortController.signal);
        setSources(response.sources ?? []);
      } catch (err) {
        if (isAbortError(err)) {
          return;
        }

        const errorMsg = toErrorMessage(err);
        setError(errorMsg);
        logging.error('Failed to fetch data sources', {
          error: err,
          errorMessage: errorMsg,
        });
      } finally {
        setIsLoading(false);
      }
    }

    fetchSources();

    return () => abortController.abort();
  }, [isOpen]);

  useEffect(() => {
    if (!isOpen) {
      return;
    }

    if (
      !selectedTokenAddress ||
      selectedChainId === null ||
      selectedChainId === undefined
    ) {
      setSelectedToken(null);
      setTokenPrice(null);
      setCatalogPreviewCount(0);
      setTokenError(null);
      setIsTokenLoading(false);
      return;
    }

    const chainId = selectedChainId;
    const tokenAddress = selectedTokenAddress;

    const abortController = new AbortController();

    async function fetchTokenTransparency() {
      setIsTokenLoading(true);
      setTokenError(null);
      setSelectedToken(null);
      setTokenPrice(null);
      setCatalogPreviewCount(0);

      const [tokenResult, tokenPriceResult, tokensResult] =
        await Promise.allSettled([
          getToken(chainId, tokenAddress, abortController.signal),
          getTokenPrice(chainId, tokenAddress, abortController.signal),
          getTokens(
            {
              chain_id: chainId,
              symbol: selectedTokenSymbol ?? undefined,
              limit: 200,
            },
            abortController.signal,
          ),
        ]);

      if (abortController.signal.aborted) {
        return;
      }

      if (tokenResult.status === 'fulfilled') {
        setSelectedToken(tokenResult.value);
      } else {
        setSelectedToken(null);
        const errorMsg = toErrorMessage(tokenResult.reason);
        setTokenError(errorMsg);
        logging.error('Failed to fetch selected token from catalog', {
          error: tokenResult.reason,
          errorMessage: errorMsg,
          chainId,
          tokenAddress,
        });
      }

      if (tokenPriceResult.status === 'fulfilled') {
        setTokenPrice(tokenPriceResult.value);
      } else {
        setTokenPrice(null);
        const errorMsg = toErrorMessage(tokenPriceResult.reason);
        setTokenError((previous) => previous ?? errorMsg);
        logging.error('Failed to fetch selected token price', {
          error: tokenPriceResult.reason,
          errorMessage: errorMsg,
          chainId,
          tokenAddress,
        });
      }

      if (tokensResult.status === 'fulfilled') {
        setCatalogPreviewCount(tokensResult.value.length);
      } else {
        logging.warn('Failed to fetch token catalog preview', {
          error: tokensResult.reason,
          selectedChainId,
          selectedTokenSymbol,
        });
        setCatalogPreviewCount(0);
      }

      setIsTokenLoading(false);
    }

    void fetchTokenTransparency();

    return () => abortController.abort();
  }, [isOpen, selectedChainId, selectedTokenAddress, selectedTokenSymbol]);

  return (
    <div
      className={css({
        borderRadius: 'lg',
        border: '1px solid token(colors.surface.subtle)',
        bg: 'surface.default',
        overflow: 'hidden',
      })}
    >
      {/* Header */}
      <button
        type="button"
        onClick={onToggle}
        className={css({
          width: '100%',
          padding: '4',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          borderBottom: isOpen
            ? '1px solid token(colors.surface.subtle)'
            : 'none',
          bg: 'surface.subtle',
          cursor: 'pointer',
          fontSize: 'sm',
          fontWeight: 'semibold',
          color: 'text.strong',
          transition: 'background-color 0.2s',
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
        <div className={css({ maxHeight: '600px', overflowY: 'auto' })}>
          {isLoading ? <SkeletonStack count={3} /> : null}

          {error ? (
            <div className={css({ p: '4' })}>
              <ErrorState
                title="Failed to load data sources"
                description="An error occurred while loading data-source transparency metadata."
                errorMessage={error}
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
                  lineHeight: '1.7',
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
                    color: 'interactive.accent',
                    textDecoration: 'underline',
                  },
                })}
              >
                <ReactMarkdown>{methodologyText}</ReactMarkdown>
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
                    m: 0,
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
                    m: 0,
                    fontSize: 'xs',
                    color: 'text.warning',
                  })}
                >
                  Failed to load token transparency metadata: {tokenError}
                </p>
              ) : null}

              {selectedTokenAddress && !isTokenLoading && selectedToken ? (
                <div
                  className={css({
                    borderWidth: '1px',
                    borderStyle: 'solid',
                    borderColor: 'border.subtle',
                    borderRadius: 'md',
                    bg: 'surface.subtle',
                    p: '3',
                    display: 'grid',
                    gap: '1.5',
                    fontSize: 'xs',
                    color: 'text.default',
                  })}
                >
                  <div>
                    <span
                      className={css({
                        fontWeight: 'semibold',
                        color: 'text.strong',
                      })}
                    >
                      Catalog Token:
                    </span>{' '}
                    {selectedToken.symbol ?? 'Unknown'} (ID {selectedToken.id})
                  </div>
                  <div>Address: {selectedToken.address}</div>
                  <div>Chain: {selectedToken.chain_id}</div>
                  <div>Decimals: {selectedToken.decimals ?? 'Unknown'}</div>
                  <div>
                    Catalog updated: {formatDateTime(selectedToken.updated_at)}
                  </div>
                  <div>
                    Metadata keys:{' '}
                    {selectedToken.metadata
                      ? Object.keys(selectedToken.metadata).join(', ') || 'None'
                      : 'None'}
                  </div>
                  <div>
                    Matching catalog rows (chain/symbol preview):{' '}
                    {catalogPreviewCount}
                  </div>
                </div>
              ) : null}

              {selectedTokenAddress && !isTokenLoading && tokenPrice ? (
                <div
                  className={css({
                    borderWidth: '1px',
                    borderStyle: 'solid',
                    borderColor: 'border.subtle',
                    borderRadius: 'md',
                    bg: 'surface.default',
                    p: '3',
                    display: 'grid',
                    gap: '1.5',
                    fontSize: 'xs',
                    color: 'text.default',
                  })}
                >
                  <div>
                    <span
                      className={css({
                        fontWeight: 'semibold',
                        color: 'text.strong',
                      })}
                    >
                      Latest Price:
                    </span>{' '}
                    {tokenPrice.is_stale || tokenPrice.price_usd == null
                      ? 'Price unavailable'
                      : formatUsdValue(tokenPrice.price_usd)}
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
                  border: '1px solid token(colors.surface.subtle)',
                })}
              >
                <table
                  className={css({
                    width: '100%',
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
                            padding: '2 3',
                            textAlign: 'left',
                            fontWeight: 'semibold',
                            color: 'text.muted',
                            borderBottom:
                              '1px solid token(colors.surface.subtle)',
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
                              '1px solid token(colors.surface.subtle)',
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
                              '1px solid token(colors.surface.subtle)',
                            color: 'text.default',
                          })}
                        >
                          {source.host}
                        </td>
                        <td
                          className={css({
                            padding: '3',
                            borderBottom:
                              '1px solid token(colors.surface.subtle)',
                            color: 'text.default',
                          })}
                        >
                          {source.role}
                        </td>
                        <td
                          className={css({
                            padding: '3',
                            borderBottom:
                              '1px solid token(colors.surface.subtle)',
                          })}
                        >
                          <span
                            className={css({
                              display: 'inline-flex',
                              px: '2',
                              py: '1',
                              borderRadius: 'md',
                              fontSize: 'xs',
                              fontWeight: 'semibold',
                              bg:
                                source.access_model === 'open'
                                  ? 'bg.success'
                                  : source.access_model === 'public'
                                    ? 'bg.warning'
                                    : 'bg.subtle',
                              color:
                                source.access_model === 'open'
                                  ? 'text.success'
                                  : source.access_model === 'public'
                                    ? 'text.warning'
                                    : 'text.default',
                            })}
                          >
                            {source.access_model}
                          </span>
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
                    borderTop: '1px solid token(colors.surface.subtle)',
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
