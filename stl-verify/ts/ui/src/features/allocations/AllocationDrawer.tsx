import { SkeletonStack } from '@archon-research/design-system';
import { lazy, Suspense, useState } from 'react';

import { css } from '#styled-system/css';

import {
  formatTokenAmount,
  formatUsdValue,
  getChainLabel,
  getProtocolLabel,
  type ChainLabelLookup,
} from '../../shared/lib/dashboard';
import type {
  Allocation,
  Prime,
  PrimeRiskCapital,
} from '../../shared/types/allocation';
import type { LocalProtocolRow } from '../../shared/types/local-data';
import { ChainLogo, ProtocolLogo, TokenLogo } from '../../shared/ui';
import { RiskDetailDrawer } from './RiskDetailDrawer';

/**
 * The drawer's body: three tabs, the activity feed and the backing-collateral
 * table, and the only user of `@tanstack/react-table`'s heavier features.
 *
 * The frame around it stays eager because it is always mounted, but nothing in
 * here is reachable until a row is clicked, so it downloads on the pointer
 * reaching the grid (see `preloadAllocationDetail`) rather than on first paint.
 */
const BottomPanel = lazy(async () => ({
  default: (await import('./BottomPanel')).BottomPanel,
}));

type AllocationDrawerProps = {
  allocations: Allocation[];
  chainLabels: ChainLabelLookup;
  errorMessage: string | null;
  isLoading: boolean;
  isOpen: boolean;
  localProtocols: LocalProtocolRow[];
  onClose: () => void;
  riskCapital: PrimeRiskCapital | null;
  selectedAllocation: Allocation | null;
  selectedPrime: Prime | null;
};

const headingClassName = css({
  display: 'inline-flex',
  alignItems: 'center',
  gap: '1.5',
  minWidth: 0,
});

const subtitleClassName = css({
  display: 'inline-flex',
  alignItems: 'center',
  gap: '1.5',
  flexWrap: 'wrap',
  rowGap: '1',
});

const subtitlePartClassName = css({
  display: 'inline-flex',
  alignItems: 'center',
  gap: '1',
  whiteSpace: 'nowrap',
});

const subtitleSeparatorClassName = css({
  color: 'text.muted',
  fontSize: 'xs',
});

/**
 * The risk drawer, titled by the allocation it is about.
 *
 * The drawer stays mounted while closed — its tabs gate their own fetches on
 * `isOpen` — so this renders whether or not a row is selected.
 *
 * It renders inside the layout's scrolling content pane rather than beside it,
 * which is only safe while no ancestor sets `transform`/`filter`/`contain`: any
 * of those would make itself the containing block for the `position: fixed`
 * panel and clip the overlay into the pane.
 */
export function AllocationDrawer({
  allocations,
  chainLabels,
  errorMessage,
  isLoading,
  isOpen,
  localProtocols,
  onClose,
  riskCapital,
  selectedAllocation,
  selectedPrime,
}: AllocationDrawerProps) {
  // Mounted from the first open and never unmounted after: the body is a
  // dynamic import, and dropping it on close would blank the panel for the
  // whole slide-out. Before that first open there is nothing to show, so its
  // chunk is never fetched -- `preloadAllocationDetail` warms it on the pointer
  // reaching the grid, which is well ahead of the click.
  const [hasOpened, setHasOpened] = useState(isOpen);
  if (isOpen && !hasOpened) {
    setHasOpened(true);
  }

  const protocolLabel = selectedAllocation
    ? getProtocolLabel(
        selectedAllocation.protocol_name,
        localProtocols,
        selectedAllocation.chain_id,
      )
    : null;

  const chainLabel = selectedAllocation
    ? getChainLabel(
        selectedAllocation.chain_id,
        chainLabels,
        selectedAllocation.network,
      )
    : null;

  return (
    <RiskDetailDrawer
      detail={
        selectedAllocation
          ? `${formatTokenAmount(selectedAllocation.balance)} ${selectedAllocation.symbol} · ${formatUsdValue(selectedAllocation.amount_usd ?? null)}`
          : undefined
      }
      isOpen={isOpen}
      onClose={onClose}
      subtitle={
        selectedAllocation ? (
          <span className={subtitleClassName}>
            <span className={subtitlePartClassName}>
              <ProtocolLogo
                protocolName={protocolLabel ?? 'Unknown'}
                size="4"
              />
              {protocolLabel}
            </span>
            <span className={subtitleSeparatorClassName}>·</span>
            <span className={subtitlePartClassName}>
              <ChainLogo
                chainId={selectedAllocation.chain_id}
                label={chainLabel ?? undefined}
                size="4"
              />
              {chainLabel}
            </span>
          </span>
        ) : undefined
      }
      title={
        selectedAllocation ? (
          <span className={headingClassName}>
            <TokenLogo
              address={selectedAllocation.receipt_token_address}
              chainId={selectedAllocation.chain_id}
              size="7"
              symbol={selectedAllocation.symbol}
            />
            <span>{selectedAllocation.symbol}</span>
          </span>
        ) : (
          'Risk details'
        )
      }
    >
      {!hasOpened ? null : (
        <Suspense
          fallback={
            <div className={css({ px: { base: '5', md: '7' }, py: '6' })}>
              <SkeletonStack count={3} />
            </div>
          }
        >
          <BottomPanel
            allocations={allocations}
            chainLabels={chainLabels}
            errorMessage={errorMessage}
            isDrawerOpen={isOpen}
            isLoading={isLoading}
            selectedAllocation={selectedAllocation}
            selectedPrime={selectedPrime}
            riskCapital={riskCapital}
          />
        </Suspense>
      )}
    </RiskDetailDrawer>
  );
}
