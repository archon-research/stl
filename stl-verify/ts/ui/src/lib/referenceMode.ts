// LOCAL COMPARISON HARNESS — not intended to ship as-is.
//
// Flips every endpoint that supports it onto `reference=true`, so the same UI
// can be opened twice and compared side by side:
//
//   http://localhost:5173/            → source: "self"      (STL's own model)
//   http://localhost:5173/?reference  → source: "reference" (Sky's figures)
//
// Read once at module load rather than per call, so every request in a session
// shares one provenance — a page mixing the two would be the one thing the
// `source` field exists to prevent.
const params = new URLSearchParams(globalThis.location?.search ?? '');
const raw = params.get('reference');

export const REFERENCE_MODE = raw !== null && raw !== 'false' && raw !== '0';

/** `{ reference: true }` when the flag is on, else nothing to spread. */
export const referenceQuery = REFERENCE_MODE
  ? ({ reference: true } as const)
  : {};
