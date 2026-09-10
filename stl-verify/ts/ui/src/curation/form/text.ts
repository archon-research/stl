/**
 * Rendering an unknown value as text.
 *
 * A single helper because the alternative kept reappearing: `String(value)` on
 * something typed `unknown`. That is not a style nit — a jsonb attribute or a
 * mis-coerced form value reaching a text input renders as `[object Object]`, and
 * then gets *submitted* as that string. The failure looks like a typo and is a
 * data defect, so the coercion is narrowed per primitive and anything else
 * becomes the explicit placeholder.
 */
export function asText(value: unknown): string {
  if (typeof value === 'string') {
    return value;
  }

  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }

  return '';
}

/** As `asText`, but for a display cell where absence should be visible. */
export function asCell(value: unknown, absent = '—'): string {
  if (value === undefined || value === null || value === '') {
    return absent;
  }

  if (typeof value === 'boolean') {
    return value ? 'yes' : 'no';
  }

  if (typeof value === 'string' || typeof value === 'number') {
    return String(value);
  }

  // An object attribute is real (a payload cluster), so it is shown rather than
  // hidden — as JSON, which is at least readable.
  return JSON.stringify(value);
}
