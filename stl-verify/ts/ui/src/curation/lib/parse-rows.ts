import { parse } from 'csv-parse/browser/esm/sync';
import type * as z from 'zod';

import { coerceAll } from '../form/coerce.ts';
import { planObject } from '../form/introspect.ts';

/**
 * File to rows, and rows to per-row validation results.
 *
 * The unit is deliberately **one row**. There is no window check, no
 * whole-file check, no duplicate detection across the batch: each row is parsed,
 * validated against the schema, and stands or falls alone. That keeps the
 * failure mode legible — a bad row is a bad row, not a file that "didn't
 * validate" — and it is what lets a curator exclude three lines and submit the
 * rest instead of fixing a file to get past a gate.
 *
 * CSV goes through `csv-parse`'s browser ESM build rather than a hand-rolled
 * split: quoted fields, embedded commas and embedded newlines are exactly the
 * cases a naive parser gets wrong, and getting them wrong silently shifts every
 * column after the offending one.
 */

export type ParsedFile = {
  rows: Record<string, unknown>[];
  /** Column names as the file presented them, in file order. */
  columns: string[];
  format: 'csv' | 'json';
};

export type ParseFailure = { error: string };

/** One row's verdict: the parsed value if it passed, the reasons if not. */
export type RowResult<T> = {
  /** 1-based, counting the data rows as a reader would — header excluded. */
  line: number;
  raw: Record<string, unknown>;
} & (
  | { ok: true; value: T }
  | { ok: false; errors: { field: string; message: string }[] }
);

/**
 * Parses a file by its content, not its extension.
 *
 * A `.txt` holding JSON and a `.csv` renamed from an export both happen; the
 * first non-space character says which this is more reliably than the name.
 */
export function parseFile(
  name: string,
  text: string,
): ParsedFile | ParseFailure {
  const trimmed = text.trim();
  if (trimmed === '') {
    return { error: 'The file is empty.' };
  }

  const looksJson = trimmed.startsWith('[') || trimmed.startsWith('{');

  return looksJson ? parseJson(trimmed) : parseCsv(trimmed, name);
}

function parseJson(text: string): ParsedFile | ParseFailure {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    return {
      error: `Not valid JSON: ${error instanceof Error ? error.message : String(error)}`,
    };
  }

  // A single object is accepted as a one-row file — the "I only have one price"
  // case, which otherwise forces a curator to wrap it in brackets by hand.
  const list = Array.isArray(parsed) ? parsed : [parsed];

  const rows: Record<string, unknown>[] = [];
  for (const [index, item] of list.entries()) {
    if (typeof item !== 'object' || item === null || Array.isArray(item)) {
      return { error: `Row ${index + 1} is not an object.` };
    }
    rows.push({ ...item });
  }

  return {
    rows,
    columns: [...new Set(rows.flatMap(Object.keys))],
    format: 'json',
  };
}

function parseCsv(text: string, name: string): ParsedFile | ParseFailure {
  try {
    const rows = parse(text, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
      // Ragged rows are a per-row problem, so they arrive as rows with missing
      // fields and fail their own validation rather than aborting the file.
      relax_column_count: true,
      bom: true,
    }) as Record<string, unknown>[];

    if (rows.length === 0) {
      return { error: `${name} has a header but no data rows.` };
    }

    return {
      rows,
      columns: Object.keys(rows[0] ?? {}),
      format: 'csv',
    };
  } catch (error) {
    return {
      error: `Could not read as CSV: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
}

/**
 * Validates each row against the schema, independently.
 *
 * Cells are coerced through the same plan-driven path the forms use, because a
 * CSV has only strings: `chain_id` arrives as `'1'` and the schema wants `1`.
 * Sharing `coerceAll` rather than re-deriving the rules here is what makes a row
 * typed into a form and the same row imported from a file produce an identical
 * value — and it is why an empty cell becomes absent rather than `''`, so
 * optional columns stay optional.
 */
export function validateRows<T>(
  rows: readonly Record<string, unknown>[],
  schema: z.ZodType<T>,
): RowResult<T>[] {
  const plans = planObject(schema);

  return rows.map((raw, index) => {
    const result = schema.safeParse(coerceAll(plans, raw));
    const line = index + 1;

    if (result.success) {
      return { line, raw, ok: true, value: result.data };
    }

    return {
      line,
      raw,
      ok: false,
      errors: result.error.issues.map((issue) => ({
        field: issue.path.map(String).join('.') || '(row)',
        message: issue.message,
      })),
    };
  });
}
