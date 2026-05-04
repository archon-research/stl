import { SkeletonStack } from '@archon-research/design-system';
import { ChevronDown, ChevronUp } from 'lucide-react';
import { useEffect, useState } from 'react';
import ReactMarkdown from 'react-markdown';

import { css } from '#styled-system/css';

import { getDataSources } from '../../lib/api';
import { isAbortError, toErrorMessage } from '../../lib/errors';
import { logging } from '../../lib/logging';
import type { DataSource } from '../../types/allocation';
import { ErrorState } from './index';

type MethodologyPanelProps = {
  isOpen: boolean;
  onToggle: () => void;
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

export function MethodologyPanel({ isOpen, onToggle }: MethodologyPanelProps) {
  const [sources, setSources] = useState<DataSource[]>([]);
  const [methodologyText, setMethodologyText] =
    useState<string>(METHODOLOGY_MARKDOWN);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

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
        if (response.methodology_markdown) {
          setMethodologyText(response.methodology_markdown);
        }
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
