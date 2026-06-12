import {
  AsyncStateRenderer,
  EmptyState,
  ErrorState,
  SearchInput,
  StyledSelect,
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
import { ActivityFeed } from './tabs/ActivityFeed';
import { RiskBreakdownTab } from './tabs/RiskBreakdownTab';
import { RrcTab } from './tabs/RrcTab';

type BottomPanelProps = {
  allocations: Allocation[];
  errorMessage: string | null;
  isDrawerOpen: boolean;
  isLoading: boolean;
  localProtocols: LocalProtocolRow[];
  selectedAllocation: Allocation | null;
  selectedPrime: Prime | null;
};

type ActiveTab = 'risk' | 'rrc' | 'activity';

function parseCategoryParam(value: string | null): AllocationCategory | '' {
  if (
    value === 'allocation' ||
    value === 'pol' ||
    value === 'psm3' ||
    value === 'asset'
  ) {
    return value;
  }
  return '';
}

const segmentedControlStyles = segmentedControl();
const toggleGroupClassName = `${segmentedControlStyles.group} ${css({ p: '0.25', gap: '0.5' })}`;
const toggleClassName = `${segmentedControlStyles.item} ${css({
  minHeight: '8',
  px: '2.5',
  fontSize: 'sm',
})}`;

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
  const [activityActionParam, setActivityActionParam] = useUrlParam(
    PARAMS.activityAction,
  );
  const [localRiskSearchValue, setLocalRiskSearchValue] = useState('');
  const [riskSearchValue, setRiskSearchValue] = useState('');
  const [categoryFilter, setCategoryFilter] = useState<AllocationCategory | ''>(
    parseCategoryParam(categoryParam),
  );

  const previousPrimeIdRef = useRef<string | null>(selectedPrime?.id ?? null);
  const previousSelectedAllocationIdRef = useRef<string | null>(
    selectedAllocation ? getAllocationKey(selectedAllocation) : null,
  );

  const activeTab: ActiveTab =
    tabParam === 'rrc' ? 'rrc' : tabParam === 'activity' ? 'activity' : 'risk';
  const activityActionFilter =
    activityActionParam === 'in' ||
    activityActionParam === 'out' ||
    activityActionParam === 'sweep'
      ? activityActionParam
      : '';

  useEffect(() => {
    const primeId = selectedPrime?.id ?? null;

    if (previousPrimeIdRef.current && previousPrimeIdRef.current !== primeId) {
      setReceiptTokenParam(null);
      setCategoryFilter('');
      setCategoryParam(null);
      setActivityActionParam(null);
    }

    previousPrimeIdRef.current = primeId;
  }, [
    selectedPrime?.id,
    setActivityActionParam,
    setCategoryParam,
    setReceiptTokenParam,
  ]);

  useEffect(() => {
    const normalized = parseCategoryParam(categoryParam);

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

  const categoryEmptyDescription = `No allocations found in the "${getCategoryLabel(categoryFilter, 'All Categories')}" category.`;

  const emptyStateView =
    sortedAllocations.length === 0 ? (
      <EmptyState
        title="No receipt tokens returned"
        description="The selected prime did not return any receipt token holdings from the API."
      />
    ) : (
      <EmptyState
        title="No receipt tokens in category"
        description={categoryEmptyDescription}
        stretch
      />
    );

  useEffect(() => {
    if (activeTab === 'rrc') {
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
        <ToggleGroup.Root
          value={[activeTab]}
          onValueChange={(details: { value: string[] }) => {
            const nextValue = details.value[0];

            if (
              nextValue === 'risk' ||
              nextValue === 'rrc' ||
              nextValue === 'activity'
            ) {
              setTabParam(nextValue);
            }
          }}
          aria-label="Risk views"
          className={toggleGroupClassName}
        >
          <ToggleGroup.Item value="risk" className={toggleClassName}>
            Risk breakdown
          </ToggleGroup.Item>
          <ToggleGroup.Item value="rrc" className={toggleClassName}>
            Required risk capital
          </ToggleGroup.Item>
          <ToggleGroup.Item value="activity" className={toggleClassName}>
            Activity
          </ToggleGroup.Item>
        </ToggleGroup.Root>
      </div>

      <div
        className={css({
          display: 'grid',
          gridTemplateColumns: {
            base: '1fr',
            md: 'repeat(3, minmax(12rem, 1fr)) minmax(18rem, 1fr)',
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
              letterSpacing: '0.1em',
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
              letterSpacing: '0.1em',
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

        {activeTab === 'activity' ? (
          <label
            htmlFor="activity-action-filter"
            className={css({
              display: 'grid',
              gap: '1',
            })}
          >
            <span
              className={css({
                fontSize: 'xs',
                textTransform: 'uppercase',
                letterSpacing: '0.1em',
                color: 'text.muted',
              })}
            >
              Action
            </span>
            <StyledSelect
              id="activity-action-filter"
              value={activityActionFilter}
              onChange={(event: ChangeEvent<HTMLSelectElement>) =>
                setActivityActionParam(event.target.value || null)
              }
              disabled={
                !focusedAllocation || isLoading || errorMessage !== null
              }
            >
              <option value="">All actions</option>
              <option value="in">In</option>
              <option value="out">Out</option>
              <option value="sweep">Sweep</option>
            </StyledSelect>
          </label>
        ) : null}

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
        ) : (
          <AsyncStateRenderer
            isLoading={isLoading}
            error={errorMessage}
            isEmpty={filteredAllocations.length === 0}
            loadingView={
              <EmptyState
                title="Loading receipt tokens"
                description="Waiting for the selected prime's receipt token holdings."
                stretch
              />
            }
            errorView={
              <ErrorState
                title="Unable to load receipt tokens"
                description="An error occurred while fetching receipt token data."
                errorMessage={errorMessage ?? undefined}
              />
            }
            emptyView={emptyStateView}
          >
            {activeTab === 'risk' ? (
              <RiskBreakdownTab
                isEnabled={isDrawerOpen && activeTab === 'risk'}
                searchQuery={riskSearchValue}
                selectedReceiptToken={focusedAllocation}
              />
            ) : activeTab === 'rrc' ? (
              <RrcTab
                isEnabled={isDrawerOpen && activeTab === 'rrc'}
                selectedReceiptToken={focusedAllocation}
                selectedPrime={selectedPrime}
              />
            ) : (
              <ActivityFeed
                isEnabled={isDrawerOpen && activeTab === 'activity'}
                actionFilter={activityActionFilter || undefined}
                mode="drawer"
                searchQuery={riskSearchValue}
                selectedReceiptToken={focusedAllocation}
                selectedPrime={selectedPrime}
              />
            )}
          </AsyncStateRenderer>
        )}
      </div>
    </div>
  );
}
