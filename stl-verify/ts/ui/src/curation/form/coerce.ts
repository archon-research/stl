import type { FieldPlan } from './introspect.ts';

/**
 * Turns a raw value into what the schema expects, by the field's render plan.
 *
 * Shared by the forms and the file ingest, which face the same problem from two
 * directions: a text input hands back `'42'` and a CSV cell hands back `'42'`,
 * and in both cases the schema wants `42`. Coercing by the *plan* rather than by
 * sniffing the value keeps one rule in one place — and it means a row typed into
 * a form and the same row imported from a file produce an identical value, which
 * is the only way the two paths can share a schema honestly.
 *
 * An empty string becomes absent rather than `0` or `''`, so "not provided"
 * survives the round trip and optional columns stay optional.
 */
function coerce(plan: FieldPlan, raw: unknown): unknown {
  if (plan.widget === 'switch') {
    return raw === true || raw === 'true';
  }

  if (typeof raw !== 'string') {
    return raw;
  }

  const trimmed = raw.trim();
  if (trimmed === '') {
    return undefined;
  }

  if (plan.widget === 'number') {
    const n = Number(trimmed);

    return Number.isFinite(n) ? n : trimmed;
  }

  if (plan.widget === 'json') {
    try {
      return JSON.parse(trimmed);
    } catch {
      // Left as the string it is: the schema reports "expected object", which
      // is a better error than a parse exception escaping the render.
      return trimmed;
    }
  }

  return trimmed;
}

export function coerceAll(
  plans: readonly FieldPlan[],
  values: Readonly<Record<string, unknown>>,
): Record<string, unknown> {
  const out: Record<string, unknown> = {};

  for (const plan of plans) {
    const value = coerce(plan, values[plan.name]);
    if (value !== undefined) {
      out[plan.name] = value;
    }
  }

  return out;
}
