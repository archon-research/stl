import {
  SkeletonStack,
} from '@archon-research/design-system';
import { ChevronDown, ChevronUp } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import { useEffect, useState } from 'react';

import { css } from '#styled-system/css';

import { getDataSources } from '../../lib/api';
import { isAbortError, toErrorMessage } from '../../lib/errors';
import { logging } from '../../lib/logging';
import type { DataSourcesResponse, DataSource } from '../../types/allocation';
import { ErrorState } from './index';

type MethodologyPanelProps = {
  isOpen: boolean;
  onToggle: () => void;
};

export function MethodologyPanel({
  isOpen,
  onToggle,
}: MethodologyPanelProps) {
  const [dataSources, setDataSources] = useState<DataSourcesResponse | null>(
    null,
  );
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
        const sources = await getDataSources(abortController.signal);
        setDataSources(sources);
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
        onClick={onToggle}
        className={css({
          width: '100%',
          padding: '4',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          borderBottom: isOpen ? '1px solid token(colors.surface.subtle)' : 'none',
          bg: 'surface.subtle',
          cursor: 'pointer',
          fontSize: 'sm',
          fontWeight: 'semibold',
          color: 'text.strong',
          transition: 'background-color 0.2s',
          _hover: {
            bg: 'surface.default',
          },
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
          className={css({
            maxHeight: '600px',
            overflowY: 'auto',
          })}
        >
          {isLoading && <SkeletonStack count={3} />}

          {error && (
            <div className={css({ p: '4' })}>
              <ErrorState
                title="Failed to Load Methodology"
                description="An error occurred while loading data-source methodology."
                errorMessage={error}
              />
            </div>
          )}

          {dataSources && (
            <div className={css({ p: '4', display: 'grid', gap: '6' })}>
              {/* Methodology Section */}
              {dataSources.methodology_markdown && (
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
                      '& h1, & h2, & h3, & h4': {
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
                    <ReactMarkdown>{dataSources.methodology_markdown}</ReactMarkdown>
                  </div>
                </div>
              )}

              {/* Data Sources Section */}
              {dataSources.sources.length > 0 && (
                <div className={css({ display: 'grid', gap: '3' })}>
                  <h3
                    className={css({
                      fontSize: 'sm',
                      fontWeight: 'semibold',
                      color: 'text.strong',
                      mb: '2',
                    })}
                  >
                    Data Sources ({dataSources.sources.length})
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
                          <th
                            className={css({
                              padding: '2 3',
                              textAlign: 'left',
                              fontWeight: 'semibold',
                              color: 'text.muted',
                              borderBottom: '1px solid token(colors.surface.subtle)',
                            })}
                          >
                            Source
                          </th>
                          <th
                            className={css({
                              padding: '2 3',
                              textAlign: 'left',
                              fontWeight: 'semibold',
                              color: 'text.muted',
                              borderBottom: '1px solid token(colors.surface.subtle)',
                            })}
                          >
                            Host
                          </th>
                          <th
                            className={css({
                              padding: '2 3',
                              textAlign: 'left',
                              fontWeight: 'semibold',
                              color: 'text.muted',
                              borderBottom: '1px solid token(colors.surface.subtle)',
                            })}
                          >
                            Role
                          </th>
                          <th
                            className={css({
                              padding: '2 3',
                              textAlign: 'left',
                              fontWeight: 'semibold',
                              color: 'text.muted',
                              borderBottom: '1px solid token(colors.surface.subtle)',
                            })}
                          >
                            Access
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {dataSources.sources.map((source: DataSource, idx: number) => (
                          <tr
                            key={idx}
                            className={css({
                              _hover: {
                                bg: 'surface.subtle',
                              },
                            })}
                          >
                            <td
                              className={css({
                                padding: '3',
                                borderBottom: '1px solid token(colors.surface.subtle)',
                                fontWeight: 'semibold',
                                color: 'text.strong',
                              })}
                            >
                              {source.name}
                            </td>
                            <td
                              className={css({
                                padding: '3',
                                borderBottom: '1px solid token(colors.surface.subtle)',
                                color: 'text.default',
                              })}
                            >
                              {source.host}
                            </td>
                            <td
                              className={css({
                                padding: '3',
                                borderBottom: '1px solid token(colors.surface.subtle)',
                                color: 'text.default',
                              })}
                            >
                              {source.role}
                            </td>
                            <td
                              className={css({
                                padding: '3',
                                borderBottom: '1px solid token(colors.surface.subtle)',
                                color: 'text.default',
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
                  {dataSources.sources.some((s) => s.caveat) && (
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
                      {dataSources.sources
                        .filter((s: DataSource) => s.caveat)
                        .map((source: DataSource, idx: number) => (
                          <div
                            key={idx}
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
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
