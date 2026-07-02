import {
  AsyncStateRenderer,
  EmptyState,
  ErrorState,
  SearchInput,
  StyledSelect,
  ToggleGroup,
} from '@archon-research/design-system';
import { ArrowUpRight } from 'lucide-react';
import { useEffect, useMemo, useRef, useState, type ChangeEvent } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';
import { segmentedControl } from '#styled-system/recipes';

import {
  type ChainLabelLookup,
  getAllocationKey,
  getCategoryLabel,
  sortAllocations,
} from '../../lib/dashboard';
import { navigateWithParams, PARAMS, useUrlParam } from '../../lib/url-params';
import type {
  Allocation,
  AllocationCategory,
  Prime,
} from '../../types/allocation';
import { ActivityFeed } from './tabs/ActivityFeed';
import { RiskBreakdownTab } from './tabs/RiskBreakdownTab';
import { RrcTab } from './tabs/RrcTab';

type BottomPanelProps = {
  allocations: Allocation[];
  chainLabels: ChainLabelLookup;
  errorMessage: string | null;
  isDrawerOpen: boolean;
  isLoading: boolean;
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
  chainLabels,
  errorMessage,
  isDrawerOpen,
  isLoading,
  selectedAllocation,
  selectedPrime,
}: BottomPanelProps) {
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
      setCategoryFilter('');
      setCategoryParam(null);
      setActivityActionParam(null);
    }

    previousPrimeIdRef.current = primeId;
  }, [selectedPrime?.id, setActivityActionParam, setCategoryParam]);

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

  // The drawer follows the clicked allocation. When the category filter excludes
  // it (or nothing is selected), fall back to the first allocation in view so a
  // tab always has something to render.
  const focusedAllocation = useMemo(() => {
    if (
      selectedAllocation &&
      filteredAllocations.some(
        (allocation) =>
          getAllocationKey(allocation) === getAllocationKey(selectedAllocation),
      )
    ) {
      return selectedAllocation;
    }
    return filteredAllocations[0] ?? null;
  }, [selectedAllocation, filteredAllocations]);

  const focusedAllocationKey = focusedAllocation
    ? getAllocationKey(focusedAllocation)
    : null;

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
  }, [focusedAllocationKey]);

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
          justify: 'space-between',
          gap: '3',
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

        {activeTab === 'activity' ? (
          <button
            type="button"
            disabled={!focusedAllocation}
            onClick={() =>
              navigateWithParams('/activities', {
                [PARAMS.prime]: selectedPrime?.id ?? null,
                [PARAMS.network]: focusedAllocation
                  ? String(focusedAllocation.chain_id)
                  : null,
                [PARAMS.token]: focusedAllocation?.symbol ?? null,
                [PARAMS.activityAction]: activityActionFilter || null,
                [PARAMS.showAllPrimes]: '0',
              })
            }
            className={css({
              display: 'inline-flex',
              alignItems: 'center',
              gap: '1',
              bg: 'transparent',
              border: 'none',
              p: 0,
              fontSize: 'sm',
              fontWeight: 'medium',
              color: 'interactive.accent',
              cursor: 'pointer',
              whiteSpace: 'nowrap',
              _hover: { textDecoration: 'underline' },
              _disabled: {
                color: 'text.subtle',
                cursor: 'not-allowed',
                textDecoration: 'none',
              },
            })}
          >
            View in Activities
            <ArrowUpRight className={css({ width: '4', height: '4' })} />
          </button>
        ) : null}
      </div>

      <div
        className={css({
          display: 'flex',
          flexWrap: 'wrap',
          gap: '4',
          alignItems: 'end',
        })}
      >
        <label
          htmlFor="category-select"
          className={css({
            display: 'grid',
            gap: '1',
            flex: '1 1 12rem',
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

        {activeTab === 'activity' ? (
          <label
            htmlFor="activity-action-filter"
            className={css({
              display: 'grid',
              gap: '1',
              flex: '1 1 12rem',
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
              flex: '2 1 18rem',
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
                chainLabels={chainLabels}
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
