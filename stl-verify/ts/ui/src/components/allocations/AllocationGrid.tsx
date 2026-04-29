import { SearchInput, SurfaceMessage } from '@archon-research/design-system';
import {
  type ColumnDef,
  type SortingState,
} from '@tanstack/react-table';
import { useEffect, useMemo, useState } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { Address } from '../shared';
import { DataTable, useDataTable } from '../../data-table';
import {
  type ChainLabelLookup,
  formatTokenAmount,
  getAllocationKey,
  getChainLabel,
  getProtocolLabel,
} from '../../lib/dashboard';
import type { Allocation, Prime } from '../../types/allocation';
import type { LocalProtocolRow } from '../../types/local-data';

type AllocationGridProps = {
  allocations: Allocation[];
  chainLabels: ChainLabelLookup;
  errorMessage: string | null;
  filteredAllocations: Allocation[];
  isLoading: boolean;
  localProtocols: LocalProtocolRow[];
  onSelectAllocation: (allocationKey: string) => void;
  onSearchChange: (value: string) => void;
  onSortingChange: (sorting: SortingState | ((old: SortingState) => SortingState)) => void;
  searchValue: string;
  selectedAllocationKey: string | null;
  selectedPrime: Prime | null;
  sorting: SortingState;
};

export function AllocationGrid({
  allocations,
  chainLabels,
  errorMessage,
  filteredAllocations,
  isLoading,
  localProtocols,
  onSelectAllocation,
  onSearchChange,
  onSortingChange,
  searchValue,
  selectedAllocationKey,
  selectedPrime,
  sorting,
}: AllocationGridProps) {
  const [localSearchValue, setLocalSearchValue] = useState(searchValue);

  useEffect(() => {
    setLocalSearchValue(searchValue);
  }, [searchValue]);

  useEffect(() => {
    if (localSearchValue === searchValue) {
      return;
    }

    const timeoutId = window.setTimeout(() => {
      onSearchChange(localSearchValue);
    }, 300);

    return () => window.clearTimeout(timeoutId);
  }, [localSearchValue, onSearchChange, searchValue]);

  const columns = useMemo<ColumnDef<Allocation>[]>(
    () => [
      {
        id: 'symbol',
        header: 'Asset',
        accessorFn: (allocation) => allocation.symbol,
        cell: ({ row }) => {
          const allocation = row.original;
          const isSelected =
            getAllocationKey(allocation) === selectedAllocationKey;

          return (
            <div className={flex({ align: 'center', gap: '3' })}>
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
              <div className={css({ display: 'grid', gap: '1' })}>
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
                <div className={flex({ gap: '1.5', wrap: 'wrap' })}>
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
                    {getChainLabel(allocation.chain_id, chainLabels)}
                  </span>
                </div>
              </div>
            </div>
          );
        },
      },
      {
        id: 'underlying_symbol',
        header: 'Underlying',
        accessorFn: (allocation) => allocation.underlying_symbol,
        cell: ({ row }) => {
          const allocation = row.original;

          return (
            <div>
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
              <Address value={allocation.underlying_token_address} />
            </div>
          );
        },
      },
      {
        id: 'balance',
        header: 'Balance',
        accessorFn: (allocation) => Number(allocation.balance),
        cell: ({ row }) => {
          const allocation = row.original;

          return (
            <div>
              <p
                className={css({
                  m: 0,
                  fontSize: 'sm',
                  fontWeight: 'semibold',
                  color: 'text.strong',
                })}
              >
                {formatTokenAmount(allocation.balance)} {allocation.symbol}
              </p>
              <Address value={allocation.receipt_token_address} />
            </div>
          );
        },
      },
    ],
    [chainLabels, localProtocols, selectedAllocationKey],
  );

  const table = useDataTable(filteredAllocations, columns, {
    enableSorting: true,
    onSortingChange,
    sorting,
  });

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
              bg: { _dark: 'gray.800', base: 'gray.100' },
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
              align: 'flex-end',
              justify: 'space-between',
              gap: '5',
              wrap: 'wrap',
            })}
          >
            <div
              className={css({ display: 'grid', gap: '1', minWidth: '18rem' })}
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
            <div
              className={css({
                flex: '0 1 24rem',
                minWidth: { base: '100%', md: '22rem' },
                marginLeft: 'auto',
                alignSelf: 'flex-end',
              })}
            >
              <SearchInput
                aria-label="Search allocations"
                disabled={!selectedPrime}
                onValueChange={setLocalSearchValue}
                placeholder="Search assets, protocols, chains"
                value={localSearchValue}
              />
            </div>
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
            <DataTable
              table={table}
              isLoading={isLoading}
              onRowClick={(allocation) => onSelectAllocation(getAllocationKey(allocation))}
              getRowKey={getAllocationKey}
              selectedRowKey={selectedAllocationKey}
            />
          ) : null}
        </div>
      </section>
    </div>
  );
}
