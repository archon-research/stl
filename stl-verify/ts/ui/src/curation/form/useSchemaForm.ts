import { useCallback, useMemo, useRef, useState } from 'react';
import * as z from 'zod';

import { coerceAll } from './coerce.ts';
import {
  type FieldPlan,
  initialValues,
  planObject,
  planSections,
} from './introspect.ts';

/**
 * A headless, schema-driven form hook.
 *
 * Shaped deliberately like TanStack Form and Mantine's `useForm` — `values`,
 * `errors`, `touched`, `getField`, `handleSubmit` — so that swapping this for
 * either is a mechanical change rather than a rewrite. It is hand-rolled here
 * for two reasons, both of which are reversible:
 *
 * 1. The interesting work in this domain is not field-state management. It is
 *    the schema-to-widget plan, the severity-tiered shape gaps, and the
 *    relational narrowing — none of which a form library provides, and all of
 *    which would still have to be written on top of one.
 * 2. A spike that adds no dependency can be deleted without a migration.
 *
 * What would justify moving to TanStack Form: nested field arrays with their own
 * validation (the composite security workflow is heading there), or
 * cross-form linked state. Both are places a library has already paid for the
 * edge cases. `docs` records that trigger.
 *
 * ## The validation model
 *
 * Two levels, because they answer different questions at different moments:
 *
 * - **Per field, on blur** — the field's own schema, unwrapped. Fast, and it
 *   cannot produce a confusing error about a field the user has not reached.
 * - **Whole object, on every change** — the full schema including every
 *   `.check()` cross-field rule. Run always, *displayed* only for fields the
 *   user has touched (and for everything once submit has been attempted). That
 *   is what makes `isValid` honest from the first keystroke while keeping the
 *   form quiet: the approval rule can know it is unsatisfied long before it is
 *   fair to say so.
 */

type SchemaFormErrors = Readonly<Record<string, string>>;

export type FieldBinding = {
  plan: FieldPlan;
  value: unknown;
  /** Present only when the field is touched, or submit has been attempted. */
  error: string | undefined;
  touched: boolean;
  setValue: (value: unknown) => void;
  onBlur: () => void;
};

export type SchemaFormOptions<T> = {
  schema: z.ZodType<T>;
  /** Seeds the form; anything absent falls back to the schema's own default. */
  initial?: Readonly<Record<string, unknown>>;
  onSubmit: (value: T) => Promise<void> | void;
};

export type SchemaForm<T> = {
  plans: readonly FieldPlan[];
  sections: readonly { group: string; fields: readonly FieldPlan[] }[];
  /** The raw, uncoerced form state — what the controls are bound to. */
  values: Readonly<Record<string, unknown>>;
  /** Every error the schema reports, keyed by dotted path, touched or not. */
  allErrors: SchemaFormErrors;
  /** The errors it is fair to show right now. */
  errors: SchemaFormErrors;
  touched: Readonly<Record<string, boolean>>;
  isValid: boolean;
  isDirty: boolean;
  isSubmitting: boolean;
  submitAttempted: boolean;
  submitError: string | undefined;
  /** The parsed value when the form is valid, for a live preview of the append. */
  parsed: T | undefined;
  getField: (name: string) => FieldBinding;
  setFieldValue: (name: string, value: unknown) => void;
  touchField: (name: string) => void;
  handleSubmit: () => Promise<void>;
  reset: () => void;
};

/**
 * Flattens zod issues to one message per path.
 *
 * First issue wins. A field with three unmet constraints has one root cause
 * nearly always, and stacking them reads as three separate problems.
 */
function issuesToErrors(error: z.ZodError): SchemaFormErrors {
  const errors: Record<string, string> = {};

  for (const issue of error.issues) {
    const path = issue.path.map(String).join('.') || '_root';
    errors[path] ??= messageFor(issue);
  }

  return errors;
}

/**
 * The message a curator should read.
 *
 * An empty required field reaches zod as `invalid_type` with `received:
 * undefined`, whose default message is "Invalid input: expected string,
 * received undefined" — which describes the parser's disappointment rather than
 * what the reader has to do. Every other issue keeps its own message, since
 * those were written for this form.
 */
function messageFor(issue: z.core.$ZodIssue): string {
  const isMissing =
    issue.code === 'invalid_type' &&
    (issue as { input?: unknown }).input === undefined;

  return isMissing ? 'Required' : issue.message;
}

export function useSchemaForm<T>(options: SchemaFormOptions<T>): SchemaForm<T> {
  const { schema, initial, onSubmit } = options;

  const plans = useMemo(() => planObject(schema), [schema]);
  const sections = useMemo(() => planSections(plans), [plans]);

  const seed = useMemo(() => {
    const base = initialValues(plans);

    return initial === undefined ? base : { ...base, ...initial };
  }, [plans, initial]);

  const seedRef = useRef(seed);
  const [values, setValues] = useState<Record<string, unknown>>(seed);
  const [touched, setTouched] = useState<Record<string, boolean>>({});
  const [submitAttempted, setSubmitAttempted] = useState(false);
  const [isSubmitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | undefined>(undefined);

  // The full-schema run, on every change. It carries the cross-field checks, so
  // it is also what `isValid` and the append preview read.
  const result = useMemo(
    () => schema.safeParse(coerceAll(plans, values)),
    [schema, plans, values],
  );

  const allErrors = useMemo(
    () => (result.success ? {} : issuesToErrors(result.error)),
    [result],
  );

  const errors = useMemo(() => {
    if (submitAttempted) {
      return allErrors;
    }

    const visible: Record<string, string> = {};
    for (const [path, message] of Object.entries(allErrors)) {
      // A cross-field error is anchored on one path but caused by another, so it
      // shows once the field it is anchored on has been visited — which is the
      // point at which naming it is useful rather than premature.
      if (touched[path.split('.')[0] ?? path]) {
        visible[path] = message;
      }
    }

    return visible;
  }, [allErrors, touched, submitAttempted]);

  const isDirty = useMemo(
    () => plans.some((p) => values[p.name] !== seedRef.current[p.name]),
    [plans, values],
  );

  const setFieldValue = useCallback((name: string, value: unknown) => {
    setValues((prev) => ({ ...prev, [name]: value }));
    setSubmitError(undefined);
  }, []);

  const touchField = useCallback((name: string) => {
    setTouched((prev) => (prev[name] ? prev : { ...prev, [name]: true }));
  }, []);

  const getField = useCallback(
    (name: string): FieldBinding => {
      const plan = plans.find((p) => p.name === name);
      if (plan === undefined) {
        throw new Error(`no such field on this schema: ${name}`);
      }

      return {
        plan,
        value: values[name],
        error: errors[name],
        touched: touched[name] ?? false,
        setValue: (value: unknown) => setFieldValue(name, value),
        onBlur: () => touchField(name),
      };
    },
    [plans, values, errors, touched, setFieldValue, touchField],
  );

  const handleSubmit = useCallback(async () => {
    setSubmitAttempted(true);
    setSubmitError(undefined);

    if (!result.success) {
      return;
    }

    setSubmitting(true);
    try {
      await onSubmit(result.data);
    } catch (error) {
      // Surfaced rather than thrown: a rejected append is an expected outcome of
      // a governed write path (a shape the client does not know about, a
      // permission it does not have), and the form is where the curator can act
      // on it.
      setSubmitError(error instanceof Error ? error.message : String(error));
    } finally {
      setSubmitting(false);
    }
  }, [result, onSubmit]);

  const reset = useCallback(() => {
    setValues(seedRef.current);
    setTouched({});
    setSubmitAttempted(false);
    setSubmitError(undefined);
  }, []);

  return {
    plans,
    sections,
    values,
    allErrors,
    errors,
    touched,
    isValid: result.success,
    isDirty,
    isSubmitting,
    submitAttempted,
    submitError,
    parsed: result.success ? result.data : undefined,
    getField,
    setFieldValue,
    touchField,
    handleSubmit,
    reset,
  };
}
