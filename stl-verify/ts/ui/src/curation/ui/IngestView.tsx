import { Badge, Button, SurfaceMessage } from '@archon-research/design-system';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useMemo, useState } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { api } from '../lib/api.ts';
import { parseFile, type RowResult, validateRows } from '../lib/parse-rows.ts';
import {
  PRICE_POINT_COLUMNS,
  type PricePoint,
  pricePoint,
} from '../schema/timeseries.ts';
import { type Column, DataTable } from './DataTable.tsx';
import { PageFrame, PageSection, PageToolbar } from './PageFrame.tsx';

/**
 * File ingest for dated prices.
 *
 * The whole design follows from one decision: **the unit of validation is a
 * row**. The file is parsed, every row is checked against the schema on its own,
 * and the curator decides row by row what to send. There is no whole-file
 * verdict to fail, no window check, and no cross-row rule — so a file with three
 * bad lines is three problems, not a rejected file, and the other four hundred
 * rows do not wait for them.
 *
 * Validation happens in the browser against the same zod schema the API would
 * use, which is why a 400-row file gives its verdict instantly and without a
 * round trip. What the server still owns is the one thing the client cannot
 * know: whether an observation already exists for that instant and source.
 */
export function IngestView() {
  const queryClient = useQueryClient();
  const [fileName, setFileName] = useState<string | null>(null);
  const [parseError, setParseError] = useState<string | null>(null);
  const [results, setResults] = useState<RowResult<PricePoint>[]>([]);
  const [excluded, setExcluded] = useState<ReadonlySet<number>>(new Set());
  const [receipt, setReceipt] = useState<string | null>(null);

  const submit = useMutation(
    api.mutationOptions('post', '/v1/prices', { invalidates: [] }),
  );

  const valid = useMemo(() => results.filter((r) => r.ok), [results]);
  const invalid = useMemo(() => results.filter((r) => !r.ok), [results]);
  const sending = useMemo(
    () => valid.filter((r) => !excluded.has(r.line)),
    [valid, excluded],
  );

  const readFile = async (file: File) => {
    setReceipt(null);
    setExcluded(new Set());
    setFileName(file.name);

    const parsed = parseFile(file.name, await file.text());
    if ('error' in parsed) {
      setParseError(parsed.error);
      setResults([]);

      return;
    }

    setParseError(null);
    setResults(validateRows(parsed.rows, pricePoint));
  };

  const send = async () => {
    const response = await submit.mutateAsync({
      body: {
        rows: sending.map((r) => (r.ok ? r.value : null)).filter(Boolean),
      },
    });

    await queryClient.invalidateQueries();
    setReceipt(
      `${response.accepted} accepted` +
        (response.rejected.length > 0
          ? `, ${response.rejected.length} rejected by the server: ${response.rejected
              .map((r) => `row ${r.line} (${r.message})`)
              .join('; ')}`
          : ''),
    );
  };

  return (
    <PageFrame
      crumbs={[{ label: 'Workflows' }, { label: 'Import prices' }]}
      title="Import prices"
      description="CSV or JSON. Every row is validated on its own against the price schema — there is no whole-file check, so you can exclude the rows that fail and send the rest."
      meta={
        results.length > 0 ? (
          <>
            <Badge variant="subtle" colorPalette="green">
              {valid.length} valid
            </Badge>
            {invalid.length > 0 && (
              <Badge variant="subtle" colorPalette="red">
                {invalid.length} invalid
              </Badge>
            )}
          </>
        ) : undefined
      }
    >
      <PageToolbar
        filter={
          <label className={fileLabelClassName}>
            <input
              type="file"
              accept=".csv,.json,.txt,text/csv,application/json"
              className={fileInputClassName}
              onChange={(event) => {
                const file = event.target.files?.[0];
                if (file !== undefined) {
                  void readFile(file);
                }
              }}
            />
            <span className={fileButtonClassName}>Choose a file…</span>
            <span className={fileNameClassName}>
              {fileName ?? 'No file selected'}
            </span>
          </label>
        }
        actions={
          <Button
            emphasis="solid"
            colorPalette="blue"
            disabled={sending.length === 0 || submit.isPending}
            onClick={() => void send()}
          >
            {submit.isPending
              ? 'Sending…'
              : `Send ${sending.length} row${sending.length === 1 ? '' : 's'}`}
          </Button>
        }
      />

      {parseError !== null && (
        <SurfaceMessage
          tone="critical"
          title="The file could not be read"
          body={parseError}
        />
      )}

      {receipt !== null && (
        <SurfaceMessage tone="muted" title="Sent" body={receipt} />
      )}

      {results.length === 0 && parseError === null && (
        <PageSection title="Expected columns">
          <p className={hintClassName}>{PRICE_POINT_COLUMNS.join(' · ')}</p>
          <p className={hintClassName}>
            A JSON array of objects with those keys works too, as does a single
            object for one observation.
          </p>
        </PageSection>
      )}

      {results.length > 0 && (
        <PageSection bleed>
          <DataTable
            label="Parsed rows"
            rows={results}
            rowKey={(r) => String(r.line)}
            columns={columns(excluded, setExcluded)}
          />
        </PageSection>
      )}
    </PageFrame>
  );
}

function columns(
  excluded: ReadonlySet<number>,
  setExcluded: (next: ReadonlySet<number>) => void,
): Column<RowResult<PricePoint>>[] {
  return [
    {
      key: 'line',
      header: 'Row',
      numeric: true,
      mono: true,
      render: (r) => r.line,
      sortValue: (r) => r.line,
    },
    {
      key: 'status',
      header: 'Status',
      // Invalid first when sorted, because those are the rows needing a decision.
      sortValue: (r) => (r.ok ? 1 : 0),
      render: (r) =>
        r.ok ? (
          <Badge variant="subtle" colorPalette="green">
            valid
          </Badge>
        ) : (
          <Badge variant="subtle" colorPalette="red">
            invalid
          </Badge>
        ),
    },
    {
      key: 'detail',
      header: 'Value or reason',
      render: (r) =>
        r.ok ? (
          <span className={monoClassName}>
            {r.value.price_usd} USD @ {r.value.observed_at}
          </span>
        ) : (
          <span className={errorListClassName}>
            {r.errors.map((e) => `${e.field}: ${e.message}`).join(' · ')}
          </span>
        ),
      sortValue: (r) => (r.ok ? r.value.observed_at : r.errors[0]?.field),
    },
    {
      key: 'token',
      header: 'Token',
      mono: true,
      render: (r) =>
        r.ok
          ? `${r.value.chain_id}:${r.value.token_address.slice(0, 10)}…`
          : '—',
      sortValue: (r) => (r.ok ? r.value.token_address : null),
    },
    {
      key: 'include',
      header: 'Send',
      render: (r) => {
        if (!r.ok) {
          return <span className={hintClassName}>excluded</span>;
        }

        const isExcluded = excluded.has(r.line);

        return (
          <label className={checkboxLabelClassName}>
            <input
              type="checkbox"
              checked={!isExcluded}
              onChange={() => {
                const next = new Set(excluded);
                if (isExcluded) {
                  next.delete(r.line);
                } else {
                  next.add(r.line);
                }
                setExcluded(next);
              }}
            />
            <span className={visuallyHiddenClassName}>Send row {r.line}</span>
          </label>
        );
      },
    },
  ];
}

const fileLabelClassName = flex({
  align: 'center',
  gap: '3',
  cursor: 'pointer',
  minWidth: '0',
});

/** Hidden but focusable: the styled span beside it is the visible control. */
const fileInputClassName = css({
  position: 'absolute',
  width: '[1px]',
  height: '[1px]',
  opacity: '0',
});

const fileButtonClassName = css({
  display: 'inline-flex',
  alignItems: 'center',
  px: '3',
  py: '2',
  borderWidth: '1px',
  borderStyle: 'solid',
  borderColor: 'border.default',
  borderRadius: 'md',
  bg: 'surface.default',
  fontSize: 'sm',
  color: 'text.default',
  whiteSpace: 'nowrap',
  _hover: { bg: 'interactive.hover' },
});

const fileNameClassName = css({
  fontSize: 'xs',
  color: 'text.muted',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
});

const hintClassName = css({ fontSize: 'xs', color: 'text.muted', margin: '0' });
const monoClassName = css({ fontFamily: 'mono', fontSize: 'xs' });
const errorListClassName = css({ fontSize: 'xs', color: 'text.critical' });
const checkboxLabelClassName = css({ cursor: 'pointer' });
const visuallyHiddenClassName = css({
  position: 'absolute',
  width: '[1px]',
  height: '[1px]',
  overflow: 'hidden',
  clipPath: 'inset(50%)',
  whiteSpace: 'nowrap',
});
