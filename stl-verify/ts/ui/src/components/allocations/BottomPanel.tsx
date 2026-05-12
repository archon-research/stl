import {
  SearchInput,
  StyledSelect,
  Toggle,
  ToggleGroup,
} from '@archon-research/design-system';
import { useEffect, useMemo, useRef, useState, type ChangeEvent } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';
import { segmentedControl } from '#styled-system/recipes';

import {
  getAllocationKey,
  getCategoryLabel,
  getProtocolLabel,
  sortAllocations,
} from '../../lib/dashboard';
import { PARAMS, useUrlParam } from '../../lib/url-params';
import type {
  Allocation,
  AllocationCategory,
  Prime,
} from '../../types/allocation';
import type { LocalProtocolRow } from '../../types/local-data';
import { EmptyState, ErrorState } from '../shared';
import { ActivityFeed } from './tabs/ActivityFeed';
import { BadDebtTab } from './tabs/BadDebtTab';
import { RiskBreakdownTab } from './tabs/RiskBreakdownTab';

type BottomPanelProps = {
  allocations: Allocation[];
  errorMessage: string | null;
  isDrawerOpen: boolean;
  isLoading: boolean;
  localProtocols: LocalProtocolRow[];
  selectedAllocation: Allocation | null;
  selectedPrime: Prime | null;
};

type ActiveTab = 'risk' | 'bad-debt' | 'activity';

const segmentedControlStyles = segmentedControl();
const toggleGroupClassName = `${segmentedControlStyles.group} ${css({ p: '0.25', gap: '0.5' })}`;
const toggleClassName = `${segmentedControlStyles.item} ${css({ minHeight: '7', px: '2', fontSize: 'xs' })}`;

export function BottomPanel({
  allocations,
  errorMessage,
  isDrawerOpen,
  isLoading,
  localProtocols,
  selectedAllocation,
  selectedPrime,
}: BottomPanelProps) {
  const [receiptTokenParam, setReceiptTokenParam] = useUrlParam(
    PARAMS.receiptToken,
  );
  const [tabParam, setTabParam] = useUrlParam(PARAMS.tab);
  const [categoryParam, setCategoryParam] = useUrlParam(PARAMS.category);
  const [localRiskSearchValue, setLocalRiskSearchValue] = useState('');
  const [riskSearchValue, setRiskSearchValue] = useState('');
  const [categoryFilter, setCategoryFilter] = useState<AllocationCategory | ''>(
    categoryParam === 'allocation' ||
      categoryParam === 'pol' ||
      categoryParam === 'psm3' ||
      categoryParam === 'asset'
      ? categoryParam
      : '',
  );

  const previousPrimeIdRef = useRef<string | null>(selectedPrime?.id ?? null);
  const previousSelectedAllocationIdRef = useRef<string | null>(
    selectedAllocation ? getAllocationKey(selectedAllocation) : null,
  );

  const activeTab: ActiveTab =
    tabParam === 'bad-debt'
      ? 'bad-debt'
      : tabParam === 'activity'
        ? 'activity'
        : 'risk';

  useEffect(() => {
    const primeId = selectedPrime?.id ?? null;

    if (previousPrimeIdRef.current && previousPrimeIdRef.current !== primeId) {
      setReceiptTokenParam(null);
      setCategoryFilter('');
      setCategoryParam(null);
    }

    previousPrimeIdRef.current = primeId;
  }, [selectedPrime?.id, setCategoryParam, setReceiptTokenParam]);

  useEffect(() => {
    const normalized =
      categoryParam === 'allocation' ||
      categoryParam === 'pol' ||
      categoryParam === 'psm3' ||
      categoryParam === 'asset'
        ? categoryParam
        : '';

    if (normalized !== categoryFilter) {
      setCategoryFilter(normalized);
    }
  }, [categoryFilter, categoryParam]);

  const sortedAllocations = useMemo(
    () => sortAllocations(allocations),
    [allocations],
  );

  // Filter allocations by selected category
  const filteredAllocations = useMemo(() => {
    if (!categoryFilter) {
      return sortedAllocations;
    }
    return sortedAllocations.filter((a) => a.category === categoryFilter);
  }, [sortedAllocations, categoryFilter]);

  useEffect(() => {
    if (sortedAllocations.length === 0) {
      if (receiptTokenParam !== null) {
        setReceiptTokenParam(null);
      }
      return;
    }

    if (filteredAllocations.length === 0) {
      if (receiptTokenParam !== null) {
        setReceiptTokenParam(null);
      }
      return;
    }

    if (
      receiptTokenParam &&
      filteredAllocations.some(
        (allocation) => getAllocationKey(allocation) === receiptTokenParam,
      )
    ) {
      return;
    }

    const selectedKey = selectedAllocation
      ? getAllocationKey(selectedAllocation)
      : null;
    const selectedInFiltered =
      selectedKey !== null &&
      filteredAllocations.some(
        (allocation) => getAllocationKey(allocation) === selectedKey,
      )
        ? selectedAllocation
        : null;

    const fallback = selectedInFiltered ?? filteredAllocations[0];
    if (fallback) {
      setReceiptTokenParam(getAllocationKey(fallback));
    }
  }, [
    receiptTokenParam,
    selectedAllocation,
    setReceiptTokenParam,
    filteredAllocations,
    sortedAllocations.length,
  ]);

  // Sync the URL-backed receipt-token param to the grid's current selection
  // only when that selection *changes*. The ref guards against clobbering a
  // manual dropdown pick on unrelated re-renders (e.g. the user changes the
  // dropdown → receiptTokenParam changes → this effect would otherwise fire
  // and overwrite the pick back to the grid row's id).
  useEffect(() => {
    const currentKey = selectedAllocation
      ? getAllocationKey(selectedAllocation)
      : null;

    if (currentKey === previousSelectedAllocationIdRef.current) {
      return;
    }

    previousSelectedAllocationIdRef.current = currentKey;

    if (currentKey === null) {
      return;
    }

    if (receiptTokenParam !== currentKey) {
      setReceiptTokenParam(currentKey);
    }
  }, [receiptTokenParam, selectedAllocation, setReceiptTokenParam]);

  const focusedAllocation =
    filteredAllocations.find(
      (allocation) => getAllocationKey(allocation) === receiptTokenParam,
    ) ?? null;

  useEffect(() => {
    if (activeTab === 'bad-debt') {
      setLocalRiskSearchValue('');
      setRiskSearchValue('');
      return;
    }

    const timeoutId = window.setTimeout(() => {
      setRiskSearchValue(localRiskSearchValue);
    }, 300);

    return () => window.clearTimeout(timeoutId);
  }, [activeTab, localRiskSearchValue]);

  useEffect(() => {
    setLocalRiskSearchValue('');
    setRiskSearchValue('');
  }, [receiptTokenParam]);

  return (
    <div
      className={css({
        display: 'grid',
        gap: '4',
        bg: 'surface.default',
        px: { base: '5', md: '7' },
        py: { base: '5', md: '6' },
      })}
    >
      <div
        className={flex({
          align: 'center',
          justify: 'flex-start',
          gap: '2',
          wrap: 'wrap',
        })}
      >
        <ToggleGroup
          value={[activeTab]}
          onValueChange={(value: readonly string[]) => {
            const nextValue = value[0];

            if (
              nextValue === 'risk' ||
              nextValue === 'bad-debt' ||
              nextValue === 'activity'
            ) {
              setTabParam(nextValue);
            }
          }}
          aria-label="Risk views"
          className={toggleGroupClassName}
        >
          <Toggle value="risk" className={toggleClassName}>
            Risk breakdown
          </Toggle>
          <Toggle value="bad-debt" className={toggleClassName}>
            Bad debt
          </Toggle>
          <Toggle value="activity" className={toggleClassName}>
            Activity
          </Toggle>
        </ToggleGroup>
      </div>

      <div
        className={css({
          display: 'grid',
          gridTemplateColumns: {
            base: '1fr',
            md: 'repeat(2, minmax(14rem, 1fr)) minmax(18rem, 1fr)',
          },
          gap: '4',
          alignItems: 'end',
        })}
      >
        <label
          htmlFor="category-select"
          className={css({
            display: 'grid',
            gap: '1',
          })}
        >
          <span
            className={css({
              fontSize: 'xs',
              textTransform: 'uppercase',
              letterSpacing: '0.14em',
              color: 'text.muted',
            })}
          >
            Category
          </span>
          <StyledSelect
            id="category-select"
            value={categoryFilter}
            onChange={(event: ChangeEvent<HTMLSelectElement>) => {
              const nextCategory =
                (event.target.value as AllocationCategory) || '';
              setCategoryFilter(nextCategory);
              setCategoryParam(nextCategory || null);
            }}
            disabled={
              !selectedPrime ||
              isLoading ||
              errorMessage !== null ||
              sortedAllocations.length === 0
            }
          >
            <option value="">All Categories</option>
            <option value="allocation">Allocation</option>
            <option value="pol">Protocol Owned Liquidity</option>
            <option value="psm3">PSM3</option>
            <option value="asset">Asset</option>
          </StyledSelect>
        </label>

        <label
          className={css({
            display: 'grid',
            gap: '1',
          })}
        >
          <span
            className={css({
              fontSize: 'xs',
              textTransform: 'uppercase',
              letterSpacing: '0.14em',
              color: 'text.muted',
            })}
          >
            Receipt token
          </span>
          <StyledSelect
            value={receiptTokenParam ?? ''}
            onChange={(event: ChangeEvent<HTMLSelectElement>) =>
              setReceiptTokenParam(event.target.value || null)
            }
            disabled={
              !selectedPrime ||
              isLoading ||
              errorMessage !== null ||
              filteredAllocations.length === 0
            }
          >
            <option value="">Choose a receipt token</option>
            {filteredAllocations.map((allocation) => {
              const key = getAllocationKey(allocation);
              return (
                <option key={key} value={key}>
                  {`${allocation.symbol} · ${getProtocolLabel(allocation.protocol_name, localProtocols, allocation.chain_id)}`}
                </option>
              );
            })}
          </StyledSelect>
        </label>

        {activeTab === 'risk' || activeTab === 'activity' ? (
          <div
            className={css({
              width: '100%',
            })}
          >
            <SearchInput
              aria-label={
                activeTab === 'risk'
                  ? 'Search risk breakdown'
                  : 'Search activity feed'
              }
              disabled={
                !focusedAllocation || isLoading || errorMessage !== null
              }
              onValueChange={setLocalRiskSearchValue}
              placeholder={
                activeTab === 'risk'
                  ? 'Search backing assets'
                  : 'Search activity'
              }
              value={localRiskSearchValue}
            />
          </div>
        ) : null}
      </div>

      <div
        className={css({ display: 'grid', gap: '4', alignContent: 'start' })}
      >
        {!selectedPrime ? (
          <EmptyState
            title="Choose a prime to inspect risk"
            description="The detail drawer becomes available after a prime is selected."
          />
        ) : null}

        {selectedPrime && errorMessage ? (
          <ErrorState
            title="Unable to load receipt tokens"
            description="An error occurred while fetching receipt token data."
            errorMessage={errorMessage}
          />
        ) : null}

        {selectedPrime && !errorMessage && isLoading ? (
          <EmptyState
            title="Loading receipt tokens"
            description="Waiting for the selected prime's receipt token holdings."
          />
        ) : null}

        {selectedPrime &&
        !errorMessage &&
        !isLoading &&
        sortedAllocations.length === 0 ? (
          <EmptyState
            title="No receipt tokens returned"
            description="The selected prime did not return any receipt token holdings from the API."
          />
        ) : null}

        {selectedPrime &&
        !errorMessage &&
        !isLoading &&
        sortedAllocations.length > 0 &&
        filteredAllocations.length === 0 ? (
          <EmptyState
            title="No receipt tokens in category"
            description={`No allocations found in the "${getCategoryLabel(categoryFilter, 'All Categories')}" category.`}
          />
        ) : null}

        {selectedPrime &&
        !errorMessage &&
        !isLoading &&
        filteredAllocations.length > 0 ? (
          activeTab === 'risk' ? (
            <RiskBreakdownTab
              isEnabled={isDrawerOpen && activeTab === 'risk'}
              searchQuery={riskSearchValue}
              selectedReceiptToken={focusedAllocation}
            />
          ) : activeTab === 'bad-debt' ? (
            <BadDebtTab selectedReceiptToken={focusedAllocation} />
          ) : (
            <ActivityFeed
              isEnabled={isDrawerOpen && activeTab === 'activity'}
              searchQuery={riskSearchValue}
              selectedPrime={selectedPrime}
            />
          )
        ) : null}
      </div>
    </div>
  );
}
