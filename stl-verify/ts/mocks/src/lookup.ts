/**
 * Reads of fixture tables keyed by something the request supplied.
 *
 * A plain index read answers `constructor`, `__proto__` and `toString` with
 * `Object.prototype`'s members, so a path or query param naming one of them
 * resolves to an inherited value instead of taking the handler's miss branch —
 * a `404` that turns into a `200` carrying a function.
 */

export function ownEntry<T>(
  table: Readonly<Record<string, T>>,
  key: string,
): T | undefined {
  return Object.hasOwn(table, key) ? table[key] : undefined;
}
