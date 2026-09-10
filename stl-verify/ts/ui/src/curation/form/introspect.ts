// zod's runtime metadata lives on `_zod`, so the underscore is the library's
// spelling and not ours. Scoped to this file, which is the only one that reaches
// into it.
// oxlint-disable no-underscore-dangle
import * as z from 'zod';

import {
  type FieldUi,
  type FieldWidget,
  fieldUi,
} from '../schema/primitives.ts';
import type { RecordType } from '../schema/vocabularies.ts';

/**
 * Zod schema to render plan.
 *
 * The plan is the seam of the whole approach: introspection happens here and
 * nowhere else, so the renderer never touches a zod internal and the schemas
 * never mention a component. That matters more than it looks — `_zod.def` is not
 * a public API, and the day it changes shape this is the one file that breaks
 * rather than every field renderer.
 *
 * The widget is *inferred* from the type and *overridable* through `ui()`. The
 * inference covers the boring 80% (a string is a text box, an enum is a select,
 * a boolean is a switch) and the override covers what no type can express: that
 * a string holding `em-…` wants a reference picker, that another holding a
 * decimal wants a numeric keypad but not a `number` input, that a long one wants
 * a textarea.
 */

type FieldOption = { value: string; label: string };

export type FieldPlan = {
  name: string;
  widget: FieldWidget;
  label: string;
  /** Absent from the input rather than nullable — drives the required marker. */
  required: boolean;
  help?: string;
  placeholder?: string;
  options?: readonly FieldOption[];
  minLength?: number;
  maxLength?: number;
  min?: number;
  max?: number;
  pattern?: string;
  group: string;
  order: number;
  targetKinds?: readonly RecordType[];
  conceptClass?: string;
  narrowerThan?: string;
  engineAssigned: boolean;
  /**
   * The field's own schema, unwrapped of optional/default, for validating one
   * field on blur without running the whole object — and without losing the
   * cross-field checks, which stay on the object and run on submit.
   */
  schema: z.ZodType;
  defaultValue?: unknown;
};

type Def = {
  type: string;
  checks?: readonly { _zod: { def: Record<string, unknown> } }[];
  entries?: Record<string, string | number>;
  innerType?: z.ZodType;
  options?: readonly z.ZodType[];
  format?: string;
  defaultValue?: unknown;
  keyType?: z.ZodType;
  valueType?: z.ZodType;
};

/**
 * Reaches `_zod.def`, which is not a public API.
 *
 * A type predicate rather than an assertion, so the shape is *checked* at the
 * one point the internals are touched. When a zod upgrade moves `def`, this
 * throws with a message naming the cause instead of every field renderer
 * receiving `undefined`.
 */
function hasZodInternals(
  value: z.ZodType,
): value is z.ZodType & { _zod: { def: Def } } {
  const candidate: { _zod?: { def?: unknown } } = value;

  return typeof candidate._zod?.def === 'object' && candidate._zod.def !== null;
}

function defOf(schema: z.ZodType): Def {
  if (!hasZodInternals(schema)) {
    throw new Error(
      'zod internals moved: _zod.def is not where introspection expects it',
    );
  }

  return schema._zod.def;
}

/** The wrappers a field may carry, and what they say about it. */
type Unwrapped = {
  base: z.ZodType;
  optional: boolean;
  nullable: boolean;
  defaultValue?: unknown;
};

/**
 * Peels `optional`, `nullable` and `default` off a field.
 *
 * A field with a default is not required of the *user* even though the parsed
 * type has it, which is why `defaultValue` is carried out rather than collapsed
 * into `optional`: the form seeds the input with it, and the required marker
 * stays off.
 */
function unwrap(schema: z.ZodType): Unwrapped {
  let base = schema;
  let optional = false;
  let nullable = false;
  let defaultValue: unknown;

  for (;;) {
    const def = defOf(base);

    if (def.type === 'optional' && def.innerType !== undefined) {
      optional = true;
      base = def.innerType;
      continue;
    }

    if (def.type === 'nullable' && def.innerType !== undefined) {
      nullable = true;
      base = def.innerType;
      continue;
    }

    if (def.type === 'default' && def.innerType !== undefined) {
      defaultValue = def.defaultValue;
      base = def.innerType;
      continue;
    }

    break;
  }

  return {
    base,
    optional,
    nullable,
    ...(defaultValue !== undefined && { defaultValue }),
  };
}

function readChecks(def: Def) {
  const out: {
    minLength?: number;
    maxLength?: number;
    min?: number;
    max?: number;
    pattern?: string;
  } = {};

  // Narrowed rather than asserted, and only set when actually read: a check's
  // payload key differs per check kind, so a wrong guess would otherwise land as
  // an explicit `undefined` bound rather than an absent one.
  const set = (
    key: 'minLength' | 'maxLength' | 'min' | 'max',
    value: unknown,
  ) => {
    if (typeof value === 'number') {
      out[key] = value;
    }
  };

  for (const check of def.checks ?? []) {
    const c = check._zod.def;

    switch (c['check']) {
      case 'min_length':
        set('minLength', c['minimum']);
        break;
      case 'max_length':
        set('maxLength', c['maximum']);
        break;
      case 'length_equals':
        set('minLength', c['length']);
        set('maxLength', c['length']);
        break;
      case 'greater_than':
        set('min', c['value']);
        break;
      case 'less_than':
        set('max', c['value']);
        break;
      case 'string_format':
        if (typeof c['prefix'] === 'string') {
          out.pattern = `starts with ${c['prefix']}`;
        }
        break;
      default:
        break;
    }
  }

  return out;
}

/**
 * The widget a type implies, before any `ui()` override.
 *
 * A union falls back to text deliberately: the only union in these schemas is
 * `valid_to` (a date or the literal `infinity`), and no single control expresses
 * that honestly — a date picker would have to invent a spelling for the
 * sentinel. Text plus the field's help line is the truthful default, and the
 * override is available where a better control exists.
 */
function inferWidget(base: z.ZodType, ui: FieldUi): FieldWidget {
  if (ui.widget !== undefined) {
    return ui.widget;
  }

  const def = defOf(base);

  switch (def.type) {
    case 'enum':
      return 'select';
    case 'boolean':
      return 'switch';
    case 'number':
      return 'number';
    case 'record':
    case 'object':
      return 'json';
    case 'string':
      return def.format === 'date' ? 'date' : 'text';
    default:
      return 'text';
  }
}

function enumOptions(base: z.ZodType): readonly FieldOption[] | undefined {
  const def = defOf(base);
  if (def.type !== 'enum' || def.entries === undefined) {
    return undefined;
  }

  return Object.values(def.entries).map((v) => ({
    value: String(v),
    label: String(v),
  }));
}

/** Turns `snake_case` into a sentence when no explicit label was given. */
function humanise(name: string): string {
  const words = name.replaceAll('_', ' ').trim();

  return words.charAt(0).toUpperCase() + words.slice(1);
}

/** Builds the plan for one named field. */
function planField(name: string, schema: z.ZodType): FieldPlan {
  const hint = fieldUi(schema);
  const { base, optional, defaultValue } = unwrap(schema);
  const checks = readChecks(defOf(base));
  const options = enumOptions(base);

  return {
    name,
    widget: inferWidget(base, hint),
    label: hint.label ?? humanise(name),
    required: !optional && defaultValue === undefined,
    ...(hint.help !== undefined && { help: hint.help }),
    ...(hint.placeholder !== undefined && { placeholder: hint.placeholder }),
    ...(options !== undefined && { options }),
    ...checks,
    group: hint.group ?? 'Details',
    order: hint.order ?? 99,
    ...(hint.targetKinds !== undefined && { targetKinds: hint.targetKinds }),
    ...(hint.conceptClass !== undefined && { conceptClass: hint.conceptClass }),
    ...(hint.narrowerThan !== undefined && { narrowerThan: hint.narrowerThan }),
    engineAssigned: hint.engineAssigned ?? false,
    schema: base,
    ...(defaultValue !== undefined && { defaultValue }),
  };
}

/**
 * The object schema a resource is written with, as an ordered, grouped plan.
 *
 * `.check()` wraps the object without changing its shape, so the shape is read
 * off the inner object when the top level is a wrapper. Without this the plan
 * for every resource here would be empty, since all of them carry cross-field
 * checks.
 */
export function planObject(schema: z.ZodType): readonly FieldPlan[] {
  const shape = objectShape(schema);
  if (shape === undefined) {
    return [];
  }

  const plans = Object.entries(shape).map(([name, field]) =>
    planField(name, field),
  );

  // Groups run in the order they first appear in the schema, not alphabetically.
  // Declaration order is meaningful — `id` is declared first and the provenance
  // block last — whereas sorting by name opens a security form on its Credit
  // section and buries Identity in the middle.
  const groupRank = new Map<string, number>();
  for (const plan of plans) {
    if (!groupRank.has(plan.group)) {
      groupRank.set(plan.group, groupRank.size);
    }
  }

  return plans.sort(
    (a, b) =>
      (groupRank.get(a.group) ?? 0) - (groupRank.get(b.group) ?? 0) ||
      a.order - b.order,
  );
}

function objectShape(schema: z.ZodType): Record<string, z.ZodType> | undefined {
  if (hasShape(schema)) {
    return schema.shape;
  }

  const def = defOf(schema);

  return def.innerType === undefined ? undefined : objectShape(def.innerType);
}

/** `shape` lives on `ZodObject`, not on `ZodType`, so it is tested for. */
function hasShape(
  schema: z.ZodType,
): schema is z.ZodType & { shape: Record<string, z.ZodType> } {
  return (
    'shape' in schema &&
    typeof schema.shape === 'object' &&
    schema.shape !== null
  );
}

/** The plan grouped into sections, in the order the groups first appear. */
export function planSections(
  plans: readonly FieldPlan[],
): readonly { group: string; fields: readonly FieldPlan[] }[] {
  const groups = new Map<string, FieldPlan[]>();

  for (const plan of plans) {
    const existing = groups.get(plan.group);
    if (existing === undefined) {
      groups.set(plan.group, [plan]);
    } else {
      existing.push(plan);
    }
  }

  return [...groups].map(([group, fields]) => ({ group, fields }));
}

/** The values a fresh form starts from, honouring every declared default. */
export function initialValues(
  plans: readonly FieldPlan[],
): Record<string, unknown> {
  const values: Record<string, unknown> = {};

  for (const plan of plans) {
    if (plan.defaultValue !== undefined) {
      values[plan.name] = plan.defaultValue;
      continue;
    }

    // A switch with no value renders as off but submits as absent, which for a
    // required boolean is a validation error the user cannot see the cause of.
    values[plan.name] = plan.widget === 'switch' ? false : '';
  }

  return values;
}
