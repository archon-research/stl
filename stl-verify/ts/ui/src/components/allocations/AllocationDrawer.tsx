import { css } from '#styled-system/css';

import {
  formatTokenAmount,
  formatUsdValue,
  getChainLabel,
  getProtocolLabel,
  type ChainLabelLookup,
} from '../../lib/dashboard';
import type {
  Allocation,
  Prime,
  PrimeRiskCapital,
} from '../../types/allocation';
import type { LocalProtocolRow } from '../../types/local-data';
import { ChainLogo, ProtocolLogo, TokenLogo } from '../shared';
import { BottomPanel } from './BottomPanel';
import { RiskDetailDrawer } from './RiskDetailDrawer';

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
    </RiskDetailDrawer>
  );
}
