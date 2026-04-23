import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import {
  type ChainLabelLookup,
  formatTokenAmount,
  getAllocationKey,
  getChainLabel,
  getProtocolLabel,
} from '../../lib/dashboard';
import type { Allocation, Prime } from '../../types/allocation';
import type { LocalProtocolRow } from '../../types/local-data';

function formatAddress(value: string): string {
  if (value.length <= 14) {
    return value;
  }

  return `${value.slice(0, 8)}...${value.slice(-4)}`;
}

type AllocationGridProps = {
  allocations: Allocation[];
  chainLabels: ChainLabelLookup;
  errorMessage: string | null;
  filteredAllocations: Allocation[];
  isLoading: boolean;
  localProtocols: LocalProtocolRow[];
  onSelectAllocation: (allocationKey: string) => void;
  selectedAllocationKey: string | null;
  selectedPrime: Prime | null;
};

function SurfaceMessage({ body, title }: { body: string; title: string }) {
  return (
    <div
      className={css({
        borderRadius: 'md',
        borderStyle: 'solid',
        borderWidth: '1px',
        borderColor: 'border.subtle',
        bg: 'surface.default',
        p: '5',
      })}
    >
      <p
        className={css({
          m: 0,
          fontSize: 'sm',
          fontWeight: 'semibold',
          color: 'text.strong',
        })}
      >
        {title}
      </p>
      <p
        className={css({ m: 0, mt: '2', fontSize: 'sm', color: 'text.muted' })}
      >
        {body}
      </p>
    </div>
  );
}

function SkeletonRows() {
  return Array.from({ length: 6 }, (_row, rowIndex) => (
    <tr
      key={rowIndex}
      className={css({
        borderBottomWidth: '1px',
        borderBottomStyle: 'solid',
        borderBottomColor: 'border.subtle',
      })}
    >
      {Array.from({ length: 3 }, (_cell, cellIndex) => (
        <td key={cellIndex} className={css({ px: '4', py: '3.5' })}>
          <div
            className={css({
              height: cellIndex === 0 ? '12' : '8',
              borderRadius: 'sm',
              bg: 'surface.subtle',
              opacity: 0.85,
            })}
          />
        </td>
      ))}
    </tr>
  ));
}

export function AllocationGrid({
  allocations,
  chainLabels,
  errorMessage,
  filteredAllocations,
  isLoading,
  localProtocols,
  onSelectAllocation,
  selectedAllocationKey,
  selectedPrime,
}: AllocationGridProps) {
  return (
    <div
      className={css({
        minHeight: '100%',
        bg: 'surface.subtle',
        px: { base: '5', md: '7' },
        py: { base: '6', md: '7' },
      })}
    >
      <section
        className={css({
          borderRadius: 'md',
          borderStyle: 'solid',
          borderWidth: '1px',
          borderColor: 'border.subtle',
          bg: 'surface.default',
          p: { base: '5', md: '6' },
          boxShadow: '0 24px 80px rgba(15, 23, 42, 0.08)',
        })}
      >
        <div className={css({ display: 'grid', gap: '4' })}>
          <span
            className={css({
              display: 'inline-flex',
              width: 'fit-content',
              alignItems: 'center',
              borderRadius: 'full',
              bg: 'surface.subtle',
              px: '3',
              py: '1',
              fontSize: 'xs',
              fontWeight: 'semibold',
              letterSpacing: '0.14em',
              textTransform: 'uppercase',
              color: 'text.muted',
            })}
          >
            Allocations
          </span>
          <div
            className={flex({
              align: 'flex-start',
              justify: 'space-between',
              gap: '5',
              wrap: 'wrap',
            })}
          >
            <div
              className={css({ display: 'grid', gap: '1', minWidth: '16rem' })}
            >
              <h1
                className={css({
                  m: 0,
                  fontSize: { base: '2xl', md: '3xl' },
                  lineHeight: 'tight',
                  color: 'text.strong',
                })}
              >
                {selectedPrime ? selectedPrime.name : 'Select a prime'}
              </h1>
              {selectedPrime ? (
                <p
                  className={css({
                    m: 0,
                    fontSize: 'sm',
                    color: 'text.muted',
                  })}
                >
                  {selectedPrime.id}
                </p>
              ) : null}
            </div>
            <p
              className={css({
                m: 0,
                maxWidth: '2xl',
                flex: '1 1 24rem',
                fontSize: 'sm',
                lineHeight: '1.7',
                color: 'text.default',
              })}
            >
              Current receipt-token holdings for the selected prime. Filter by
              network or protocol above, then focus a row to inspect its risk in
              the lower panel.
            </p>
          </div>
        </div>

        <div className={css({ mt: '6' })}>
          {!selectedPrime && !isLoading ? (
            <SurfaceMessage
              title="Choose a prime to load positions"
              body="The main grid activates once a prime is selected from the sidebar."
            />
          ) : null}

          {selectedPrime && errorMessage ? (
            <SurfaceMessage
              title="Unable to load allocations"
              body={errorMessage}
            />
          ) : null}

          {selectedPrime &&
          !errorMessage &&
          !isLoading &&
          allocations.length === 0 ? (
            <SurfaceMessage
              title="No allocations returned"
              body="The selected prime did not return any allocation rows from the API."
            />
          ) : null}

          {selectedPrime &&
          !errorMessage &&
          !isLoading &&
          allocations.length > 0 &&
          filteredAllocations.length === 0 ? (
            <SurfaceMessage
              title="No rows match the active filters"
              body="Clear one of the filters in the top bar to restore the allocation grid."
            />
          ) : null}

          {selectedPrime &&
          !errorMessage &&
          (isLoading || filteredAllocations.length > 0) ? (
            <div
              className={css({
                overflowX: 'auto',
                borderRadius: 'md',
                borderStyle: 'solid',
                borderWidth: '1px',
                borderColor: 'border.subtle',
              })}
            >
              <table
                className={css({
                  width: '100%',
                  minWidth: '48rem',
                  borderCollapse: 'collapse',
                  bg: 'surface.default',
                })}
              >
                <thead>
                  <tr className={css({ bg: 'surface.subtle' })}>
                    {['Asset', 'Underlying', 'Balance'].map((label) => (
                      <th
                        key={label}
                        className={css({
                          px: '4',
                          py: '3',
                          textAlign: 'left',
                          fontSize: 'xs',
                          fontWeight: 'semibold',
                          letterSpacing: '0.08em',
                          textTransform: 'uppercase',
                          color: 'text.muted',
                        })}
                      >
                        {label}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {isLoading
                    ? SkeletonRows()
                    : filteredAllocations.map((allocation) => {
                        const allocationKey = getAllocationKey(allocation);
                        const isSelected =
                          allocationKey === selectedAllocationKey;

                        return (
                          <tr
                            key={allocationKey}
                            aria-selected={isSelected}
                            tabIndex={0}
                            onClick={() => onSelectAllocation(allocationKey)}
                            onKeyDown={(event) => {
                              if (event.key === 'Enter' || event.key === ' ') {
                                event.preventDefault();
                                onSelectAllocation(allocationKey);
                              }
                            }}
                            className={css({
                              cursor: 'pointer',
                              bg: isSelected
                                ? 'interactive.selected'
                                : 'surface.default',
                              transitionDuration: 'fast',
                              transitionProperty: 'background-color',
                              _hover: { bg: 'interactive.hover' },
                              _focusVisible: { bg: 'interactive.hover' },
                            })}
                          >
                            <td
                              className={css({
                                borderBottomWidth: '1px',
                                borderBottomStyle: 'solid',
                                borderBottomColor: 'border.subtle',
                                px: '4',
                                py: '3.5',
                              })}
                            >
                              <div
                                className={flex({ align: 'center', gap: '3' })}
                              >
                                <div
                                  className={css({
                                    width: '10',
                                    height: '10',
                                    borderRadius: 'full',
                                    bg: isSelected
                                      ? 'interactive.accent'
                                      : 'surface.subtle',
                                    color: isSelected ? 'white' : 'text.strong',
                                    display: 'inline-flex',
                                    alignItems: 'center',
                                    justifyContent: 'center',
                                    fontSize: 'xs',
                                    fontWeight: 'semibold',
                                    flexShrink: 0,
                                  })}
                                >
                                  {allocation.symbol.slice(0, 2).toUpperCase()}
                                </div>
                                <div
                                  className={css({ display: 'grid', gap: '1' })}
                                >
                                  <p
                                    className={css({
                                      m: 0,
                                      fontSize: 'sm',
                                      fontWeight: 'semibold',
                                      color: 'text.strong',
                                    })}
                                  >
                                    {allocation.symbol}
                                  </p>
                                  <div
                                    className={flex({
                                      gap: '1.5',
                                      wrap: 'wrap',
                                    })}
                                  >
                                    <span
                                      className={css({
                                        fontSize: 'xs',
                                        color: 'text.muted',
                                      })}
                                    >
                                      {getProtocolLabel(
                                        allocation.protocol_name,
                                        localProtocols,
                                        allocation.chain_id,
                                      )}
                                    </span>
                                    <span
                                      className={css({
                                        fontSize: 'xs',
                                        color: 'text.muted',
                                      })}
                                    >
                                      {getChainLabel(
                                        allocation.chain_id,
                                        chainLabels,
                                      )}
                                    </span>
                                  </div>
                                </div>
                              </div>
                            </td>
                            <td
                              className={css({
                                borderBottomWidth: '1px',
                                borderBottomStyle: 'solid',
                                borderBottomColor: 'border.subtle',
                                px: '4',
                                py: '3.5',
                              })}
                            >
                              <p
                                className={css({
                                  m: 0,
                                  fontSize: 'sm',
                                  fontWeight: 'semibold',
                                  color: 'text.strong',
                                })}
                              >
                                {allocation.underlying_symbol}
                              </p>
                              <p
                                className={css({
                                  m: 0,
                                  mt: '1',
                                  fontSize: 'xs',
                                  color: 'text.muted',
                                })}
                              >
                                {formatAddress(
                                  allocation.underlying_token_address,
                                )}
                              </p>
                            </td>
                            <td
                              className={css({
                                borderBottomWidth: '1px',
                                borderBottomStyle: 'solid',
                                borderBottomColor: 'border.subtle',
                                px: '4',
                                py: '3.5',
                              })}
                            >
                              <p
                                className={css({
                                  m: 0,
                                  fontSize: 'sm',
                                  fontWeight: 'semibold',
                                  color: 'text.strong',
                                })}
                              >
                                {formatTokenAmount(allocation.balance)}{' '}
                                {allocation.symbol}
                              </p>
                              <p
                                className={css({
                                  m: 0,
                                  mt: '1',
                                  fontSize: 'xs',
                                  color: 'text.muted',
                                })}
                              >
                                {formatAddress(
                                  allocation.receipt_token_address,
                                )}
                              </p>
                            </td>
                          </tr>
                        );
                      })}
                </tbody>
              </table>
            </div>
          ) : null}
        </div>
      </section>
    </div>
  );
}
