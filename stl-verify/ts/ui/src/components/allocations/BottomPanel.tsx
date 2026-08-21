import {
  AsyncStateRenderer,
  EmptyState,
  ErrorState,
  SearchInput,
  SkeletonStack,
  StyledSelect,
  ToggleGroup,
} from '@archon-research/design-system';
import { toSearchOption } from '@archon-research/router-kit';
import { useNavigate, useSearch } from '@tanstack/react-router';
import { ArrowUpRight } from 'lucide-react';
import { useEffect, useMemo, useState, type ChangeEvent } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';
import { segmentedControl } from '#styled-system/recipes';

import {
  type ChainLabelLookup,
  getAllocationKey,
  getCategoryLabel,
  getPrimeGroupKey,
  sortAllocations,
} from '../../lib/dashboard';
import {
  ACTIVITY_ACTIONS,
  type ActivityAction,
  ALLOCATION_CATEGORIES,
  DRAWER_TABS,
  type DrawerTab,
} from '../../router/search-params';
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

type DrawerSearchPatch = {
  tab?: DrawerTab | undefined;
  category?: AllocationCategory | undefined;
  daa?: ActivityAction | undefined;
};

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
  // Not strict: the drawer stays mounted on the activities route, where the
  // allocation search does not exist.
  const search = useSearch({ from: '/allocation', shouldThrow: false });
  const navigate = useNavigate();
  const [localRiskSearchValue, setLocalRiskSearchValue] = useState('');
  const [riskSearchValue, setRiskSearchValue] = useState('');

  const activeTab: DrawerTab = search?.tab ?? 'risk';
  const categoryFilter: AllocationCategory | '' = search?.category ?? '';
  const activityActionFilter = search?.daa ?? '';

  const updateDrawerSearch = (patch: DrawerSearchPatch) => {
    void navigate({
      to: '.',
      search: (previous) => ({ ...previous, ...patch }),
      replace: true,
    });
  };

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
            const nextTab = toSearchOption(details.value[0], DRAWER_TABS);

            if (nextTab) {
              updateDrawerSearch({ tab: nextTab });
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
              // A fresh search, not a patch: nothing from the allocation view
              // rides along — range/from/to included, so the feed opens on its own
              // default window.
              void navigate({
                to: '/activities',
                search: {
                  prime: selectedPrime
                    ? getPrimeGroupKey(selectedPrime)
                    : undefined,
                  network: focusedAllocation
                    ? String(focusedAllocation.chain_id)
                    : undefined,
                  token: focusedAllocation?.symbol ?? undefined,
                  aa: activityActionFilter || undefined,
                  allp: '0',
                },
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
              color: 'text.link',
              cursor: 'pointer',
              whiteSpace: 'nowrap',
              _hover: { textDecoration: 'underline' },
              _disabled: {
                color: 'text.muted',
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
            onChange={(event: ChangeEvent<HTMLSelectElement>) =>
              updateDrawerSearch({
                category: toSearchOption(
                  event.target.value,
                  ALLOCATION_CATEGORIES,
                ),
              })
            }
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
            <option value="custody">Custody</option>
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
                updateDrawerSearch({
                  daa: toSearchOption(event.target.value, ACTIVITY_ACTIONS),
                })
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
            // A skeleton, not an EmptyState: this is the shape of the tab body
            // that is coming, and an "empty" panel titled "Loading" reads as a
            // terminal state rather than a pending one.
            loadingView={<SkeletonStack count={3} />}
            errorView={
              <ErrorState
                title="Unable to load receipt tokens"
                description="An error occurred while fetching receipt token data."
                errorMessage={errorMessage ?? undefined}
                tone="critical"
                size="inline"
              />
            }
            emptyView={emptyStateView}
          >
            {activeTab === 'risk' ? (
              <RiskBreakdownTab
                isEnabled={isDrawerOpen && activeTab === 'risk'}
                searchQuery={riskSearchValue}
                selectedReceiptToken={focusedAllocation}
                selectedPrime={selectedPrime}
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
