import {
  SurfaceMessage,
  StyledSelect,
  Toggle,
  ToggleGroup,
} from '@archon-research/design-system';
import { useEffect, useMemo, useRef, type ChangeEvent } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';
import { segmentedControl } from '#styled-system/recipes';

import {
  type ChainLabelLookup,
  findProtocolMetadata,
  formatTokenAmount,
  getChainLabel,
  getProtocolLabel,
  sortAllocations,
} from '../../lib/dashboard';
import { PARAMS, useUrlParam } from '../../lib/url-params';
import type { Allocation, Prime } from '../../types/allocation';
import type { LocalProtocolRow } from '../../types/local-data';
import { BadDebtTab } from './tabs/BadDebtTab';
import { RiskBreakdownTab } from './tabs/RiskBreakdownTab';

type BottomPanelProps = {
  allocations: Allocation[];
  chainLabels: ChainLabelLookup;
  errorMessage: string | null;
  isLoading: boolean;
  localProtocols: LocalProtocolRow[];
  selectedAllocation: Allocation | null;
  selectedPrime: Prime | null;
};

type ActiveTab = 'risk' | 'bad-debt';

const segmentedControlStyles = segmentedControl();
const toggleGroupClassName = `${segmentedControlStyles.group} ${css({ p: '0.5', gap: '1' })}`;
const toggleClassName = `${segmentedControlStyles.item} ${css({ minHeight: '8', px: '2.5', fontSize: 'xs' })}`;

function formatAddress(value: string): string {
  const address = value.startsWith('0x') ? value : `0x${value}`;

  if (address.length <= 18) {
    return address;
  }

  return `${address.slice(0, 10)}...${address.slice(-6)}`;
}

export function BottomPanel({
  allocations,
  chainLabels,
  errorMessage,
  isLoading,
  localProtocols,
  selectedAllocation,
  selectedPrime,
}: BottomPanelProps) {
  const [receiptTokenParam, setReceiptTokenParam] = useUrlParam(
    PARAMS.receiptToken,
  );
  const [tabParam, setTabParam] = useUrlParam(PARAMS.tab);

  const previousPrimeIdRef = useRef<string | null>(selectedPrime?.id ?? null);
  const previousSelectedAllocationIdRef = useRef<number | null>(
    selectedAllocation?.receipt_token_id ?? null,
  );

  const activeTab: ActiveTab = tabParam === 'bad-debt' ? 'bad-debt' : 'risk';

  useEffect(() => {
    const primeId = selectedPrime?.id ?? null;

    if (previousPrimeIdRef.current && previousPrimeIdRef.current !== primeId) {
      setReceiptTokenParam(null);
    }

    previousPrimeIdRef.current = primeId;
  }, [selectedPrime?.id, setReceiptTokenParam]);

  const sortedAllocations = useMemo(
    () => sortAllocations(allocations),
    [allocations],
  );

  useEffect(() => {
    if (sortedAllocations.length === 0) {
      if (receiptTokenParam !== null) {
        setReceiptTokenParam(null);
      }
      return;
    }

    if (
      receiptTokenParam &&
      sortedAllocations.some(
        (allocation) =>
          String(allocation.receipt_token_id) === receiptTokenParam,
      )
    ) {
      return;
    }

    const fallback = selectedAllocation ?? sortedAllocations[0];
    setReceiptTokenParam(String(fallback.receipt_token_id));
  }, [
    receiptTokenParam,
    selectedAllocation,
    setReceiptTokenParam,
    sortedAllocations,
  ]);

  // Sync the URL-backed receipt-token param to the grid's current selection
  // only when that selection *changes*. The ref guards against clobbering a
  // manual dropdown pick on unrelated re-renders (e.g. the user changes the
  // dropdown → receiptTokenParam changes → this effect would otherwise fire
  // and overwrite the pick back to the grid row's id).
  useEffect(() => {
    const currentId = selectedAllocation?.receipt_token_id ?? null;

    if (currentId === previousSelectedAllocationIdRef.current) {
      return;
    }

    previousSelectedAllocationIdRef.current = currentId;

    if (currentId === null) {
      return;
    }

    const nextTokenId = String(currentId);
    if (receiptTokenParam !== nextTokenId) {
      setReceiptTokenParam(nextTokenId);
    }
  }, [receiptTokenParam, selectedAllocation, setReceiptTokenParam]);

  const focusedAllocation =
    sortedAllocations.find(
      (allocation) => String(allocation.receipt_token_id) === receiptTokenParam,
    ) ?? null;

  const focusedProtocol = useMemo(
    () =>
      focusedAllocation
        ? findProtocolMetadata(
            focusedAllocation.protocol_name,
            localProtocols,
            focusedAllocation.chain_id,
          )
        : null,
    [focusedAllocation, localProtocols],
  );

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
          justify: 'flex-end',
          gap: '4',
          wrap: 'wrap',
        })}
      >
        <ToggleGroup
          value={[activeTab]}
          onValueChange={(value: readonly string[]) => {
            const nextValue = value[0];

            if (nextValue === 'risk' || nextValue === 'bad-debt') {
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
        </ToggleGroup>
      </div>

      <div
        className={css({
          borderRadius: 'md',
          borderStyle: 'solid',
          borderWidth: '1px',
          borderColor: 'border.subtle',
          bg: 'surface.subtle',
          p: '4',
        })}
      >
        <div className={flex({ align: 'flex-start', gap: '4', wrap: 'wrap' })}>
          <label
            className={css({ display: 'grid', gap: '1', minWidth: '18rem' })}
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
                sortedAllocations.length === 0
              }
            >
              <option value="">Choose a receipt token</option>
              {sortedAllocations.map((allocation) => (
                <option
                  key={allocation.receipt_token_id}
                  value={allocation.receipt_token_id}
                >
                  {`${allocation.symbol} · ${getProtocolLabel(allocation.protocol_name, localProtocols, allocation.chain_id)}`}
                </option>
              ))}
            </StyledSelect>
          </label>

          <div
            className={css({ display: 'grid', gap: '1', minWidth: '18rem' })}
          >
            <p
              className={css({
                m: 0,
                fontSize: 'xs',
                textTransform: 'uppercase',
                letterSpacing: '0.14em',
                color: 'text.muted',
              })}
            >
              Focused allocation
            </p>
            <p
              className={css({
                m: 0,
                fontSize: 'sm',
                fontWeight: 'semibold',
                color: 'text.strong',
              })}
            >
              {focusedAllocation
                ? `${focusedAllocation.symbol} · ${getProtocolLabel(focusedAllocation.protocol_name, localProtocols, focusedAllocation.chain_id)}`
                : 'No allocation row selected'}
            </p>
            <p className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}>
              {focusedAllocation
                ? `${formatTokenAmount(focusedAllocation.balance)} ${focusedAllocation.symbol} · ${getChainLabel(focusedAllocation.chain_id, chainLabels)}`
                : 'Pick a grid row to drive this panel, or choose a receipt token directly.'}
            </p>
            {focusedProtocol ? (
              <p className={css({ m: 0, fontSize: 'xs', color: 'text.muted' })}>
                {`Protocol address ${formatAddress(focusedProtocol.encode)}`}
              </p>
            ) : null}
          </div>
        </div>
      </div>

      <div
        className={css({ display: 'grid', gap: '4', alignContent: 'start' })}
      >
        {!selectedPrime ? (
          <SurfaceMessage
            title="Choose a prime to inspect risk"
            body="The lower panel comes alive after a prime is selected."
          />
        ) : null}

        {selectedPrime && errorMessage ? (
          <SurfaceMessage
            title="Unable to load receipt tokens."
            body={errorMessage}
          />
        ) : null}

        {selectedPrime && !errorMessage && isLoading ? (
          <SurfaceMessage
            title="Loading receipt tokens"
            body="Waiting for the selected prime's receipt token holdings."
          />
        ) : null}

        {selectedPrime &&
        !errorMessage &&
        !isLoading &&
        sortedAllocations.length === 0 ? (
          <SurfaceMessage
            title="No receipt tokens returned"
            body="The selected prime did not return any receipt token holdings from the API."
          />
        ) : null}

        {selectedPrime &&
        !errorMessage &&
        !isLoading &&
        sortedAllocations.length > 0 ? (
          activeTab === 'risk' ? (
            <RiskBreakdownTab selectedReceiptToken={focusedAllocation} />
          ) : (
            <BadDebtTab selectedReceiptToken={focusedAllocation} />
          )
        ) : null}
      </div>
    </div>
  );
}
