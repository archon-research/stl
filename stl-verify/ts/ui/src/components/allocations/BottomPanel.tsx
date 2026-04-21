import {
  StyledSelect,
  Toggle,
  ToggleGroup,
} from '@archon-research/design-system';
import { useEffect, useMemo, useRef, useState } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { getReceiptTokens } from '../../lib/api';
import {
  formatTokenAmount,
  getAllocationKey,
  getChainLabel,
  getProtocolLabel,
  matchReceiptToken,
  sortReceiptTokens,
} from '../../lib/dashboard';
import { PARAMS, useUrlParam } from '../../lib/url-params';
import type {
  AllocationPosition,
  ReceiptTokenPosition,
  Star,
} from '../../types/allocation';
import { BadDebtTab } from './tabs/BadDebtTab';
import { RiskBreakdownTab } from './tabs/RiskBreakdownTab';

type BottomPanelProps = {
  selectedAllocation: AllocationPosition | null;
  selectedStar: Star | null;
};

type ActiveTab = 'risk' | 'bad-debt';

const toggleGroupStyles = css({
  alignItems: 'center',
  bg: 'transparent',
  borderColor: 'border.default',
  borderRadius: 'sm',
  borderStyle: 'solid',
  borderWidth: '1px',
  display: 'inline-flex',
  gap: '0.5',
  p: '0.5',
});

const toggleStyles = css({
  '&[data-pressed]': {
    bg: 'interactive.selected',
    color: 'text.default',
  },
  _hover: {
    bg: 'interactive.hover',
    color: 'text.default',
  },
  borderRadius: 'xs',
  display: 'inline-flex',
  alignItems: 'center',
  justifyContent: 'center',
  color: 'text.muted',
  cursor: 'pointer',
  fontSize: 'sm',
  lineHeight: 'normal',
  h: '7',
  px: '3',
  py: '1',
  transitionDuration: 'fast',
  transitionProperty: 'background-color, color, border-color, box-shadow',
});

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'AbortError';
}

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Unknown request failure.';
}

function EmptyPanelState({ body, title }: { body: string; title: string }) {
  return (
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
        className={css({
          m: 0,
          mt: '1.5',
          fontSize: 'sm',
          color: 'text.muted',
        })}
      >
        {body}
      </p>
    </div>
  );
}

export function BottomPanel({
  selectedAllocation,
  selectedStar,
}: BottomPanelProps) {
  const [receiptTokens, setReceiptTokens] = useState<ReceiptTokenPosition[]>(
    [],
  );
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [reloadKey, setReloadKey] = useState(0);
  const [receiptTokenParam, setReceiptTokenParam] = useUrlParam(
    PARAMS.receiptToken,
  );
  const [tabParam, setTabParam] = useUrlParam(PARAMS.tab);

  const previousStarIdRef = useRef<string | null>(selectedStar?.id ?? null);
  const previousAllocationKeyRef = useRef<string | null>(null);

  const activeTab: ActiveTab = tabParam === 'bad-debt' ? 'bad-debt' : 'risk';

  useEffect(() => {
    const starId = selectedStar?.id ?? null;

    if (previousStarIdRef.current && previousStarIdRef.current !== starId) {
      setReceiptTokenParam(null);
    }

    previousStarIdRef.current = starId;
  }, [selectedStar?.id, setReceiptTokenParam]);

  useEffect(() => {
    if (!selectedStar?.id) {
      setReceiptTokens([]);
      setErrorMessage(null);
      setIsLoading(false);
      return;
    }

    const controller = new AbortController();

    setIsLoading(true);
    setErrorMessage(null);

    void getReceiptTokens(selectedStar.id, controller.signal)
      .then((response) => {
        setReceiptTokens(response);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        setErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsLoading(false);
        }
      });

    return () => controller.abort();
  }, [reloadKey, selectedStar]);

  const sortedReceiptTokens = useMemo(
    () => sortReceiptTokens(receiptTokens),
    [receiptTokens],
  );

  const matchingReceiptToken = useMemo(
    () => matchReceiptToken(selectedAllocation, sortedReceiptTokens),
    [selectedAllocation, sortedReceiptTokens],
  );

  useEffect(() => {
    if (sortedReceiptTokens.length === 0) {
      if (receiptTokenParam !== null) {
        setReceiptTokenParam(null);
      }
      return;
    }

    if (
      receiptTokenParam &&
      sortedReceiptTokens.some(
        (token) => String(token.receipt_token_id) === receiptTokenParam,
      )
    ) {
      return;
    }

    const fallback = matchingReceiptToken ?? sortedReceiptTokens[0];
    setReceiptTokenParam(String(fallback.receipt_token_id));
  }, [
    matchingReceiptToken,
    receiptTokenParam,
    setReceiptTokenParam,
    sortedReceiptTokens,
  ]);

  useEffect(() => {
    const allocationKey = selectedAllocation
      ? getAllocationKey(selectedAllocation)
      : null;

    if (allocationKey === previousAllocationKeyRef.current) {
      return;
    }

    previousAllocationKeyRef.current = allocationKey;

    if (!matchingReceiptToken) {
      return;
    }

    const nextTokenId = String(matchingReceiptToken.receipt_token_id);
    if (receiptTokenParam !== nextTokenId) {
      setReceiptTokenParam(nextTokenId);
    }
  }, [
    matchingReceiptToken,
    receiptTokenParam,
    selectedAllocation,
    setReceiptTokenParam,
  ]);

  const selectedReceiptToken =
    sortedReceiptTokens.find(
      (token) => String(token.receipt_token_id) === receiptTokenParam,
    ) ?? null;

  return (
    <div
      className={css({
        display: 'grid',
        gridTemplateRows: 'auto auto 1fr',
        gap: '4',
        height: '100%',
        bg: 'surface.default',
        px: { base: '5', md: '6' },
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
          onValueChange={(value) => {
            const nextValue = value[0];

            if (nextValue === 'risk' || nextValue === 'bad-debt') {
              setTabParam(nextValue);
            }
          }}
          aria-label="Risk views"
          className={toggleGroupStyles}
        >
          <Toggle value="risk" className={toggleStyles}>
            Risk breakdown
          </Toggle>
          <Toggle value="bad-debt" className={toggleStyles}>
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
        <div className={flex({ align: 'end', gap: '4', wrap: 'wrap' })}>
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
              onChange={(event) =>
                setReceiptTokenParam(event.target.value || null)
              }
              disabled={
                !selectedStar || isLoading || sortedReceiptTokens.length === 0
              }
            >
              <option value="">Choose a receipt token</option>
              {sortedReceiptTokens.map((token) => (
                <option
                  key={token.receipt_token_id}
                  value={token.receipt_token_id}
                >
                  {`${token.symbol} · ${token.protocol_name}`}
                </option>
              ))}
            </StyledSelect>
          </label>

          <div
            className={css({ display: 'grid', gap: '1', minWidth: '16rem' })}
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
              {selectedAllocation
                ? `${selectedAllocation.token_symbol ?? 'Unknown'} · ${getProtocolLabel(selectedAllocation.name)}`
                : 'No allocation row selected'}
            </p>
            <p className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}>
              {selectedAllocation
                ? `${formatTokenAmount(selectedAllocation.balance)} ${selectedAllocation.token_symbol ?? ''} · ${getChainLabel(selectedAllocation.chain_id)}`
                : 'Pick a grid row to drive this panel, or choose a receipt token directly.'}
            </p>
          </div>
        </div>
      </div>

      <div className={css({ minHeight: 0, overflow: 'auto' })}>
        {!selectedStar ? (
          <EmptyPanelState
            title="Choose a star to inspect risk"
            body="The lower panel comes alive after a star is selected and receipt tokens are loaded for it."
          />
        ) : null}

        {selectedStar && errorMessage ? (
          <div
            className={css({
              borderRadius: 'md',
              borderStyle: 'solid',
              borderWidth: '1px',
              borderColor: 'border.default',
              bg: 'surface.subtle',
              p: '4',
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
              Unable to load receipt tokens.
            </p>
            <p
              className={css({
                m: 0,
                mt: '1.5',
                fontSize: 'sm',
                color: 'text.muted',
              })}
            >
              {errorMessage}
            </p>
            <button
              type="button"
              onClick={() => setReloadKey((value) => value + 1)}
              className={css({
                mt: '4',
                borderRadius: 'md',
                borderStyle: 'solid',
                borderWidth: '1px',
                borderColor: 'border.default',
                bg: 'surface.default',
                color: 'text.strong',
                cursor: 'pointer',
                px: '3.5',
                py: '2',
                _hover: { bg: 'interactive.hover' },
              })}
            >
              Retry request
            </button>
          </div>
        ) : null}

        {selectedStar && !errorMessage && isLoading ? (
          <EmptyPanelState
            title="Loading receipt tokens"
            body="The bottom panel is waiting for the selected star's receipt token inventory."
          />
        ) : null}

        {selectedStar &&
        !errorMessage &&
        !isLoading &&
        sortedReceiptTokens.length === 0 ? (
          <EmptyPanelState
            title="No receipt tokens returned"
            body="The selected star did not return any receipt token rows from the API."
          />
        ) : null}

        {selectedStar &&
        !errorMessage &&
        !isLoading &&
        sortedReceiptTokens.length > 0 ? (
          activeTab === 'risk' ? (
            <RiskBreakdownTab selectedReceiptToken={selectedReceiptToken} />
          ) : (
            <BadDebtTab selectedReceiptToken={selectedReceiptToken} />
          )
        ) : null}
      </div>
    </div>
  );
}
