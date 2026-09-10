import {
  Field,
  Select,
  Switch,
  TextInput,
  Textarea,
} from '@archon-research/design-system';
import type { ReactNode } from 'react';

import { css } from '#styled-system/css';
import { toggleSwitch } from '#styled-system/recipes';

import type { FieldWidget } from '../schema/primitives.ts';
import { ReferencePicker } from './ReferencePicker.tsx';
import { asText } from './text.ts';
import type { FieldBinding } from './useSchemaForm.ts';

/**
 * The field-component manifest: one entry per widget, and the only place that
 * knows which control renders which kind of field.
 *
 * It is a lookup rather than a switch inside the renderer for the reason the
 * design system keeps `designSystemComponentManifest` as data — a table can be
 * read by something other than the renderer. Here that pays off twice: the
 * override mechanism in `SchemaForm` replaces entries in this map (so a caller
 * overriding one field does not fork the renderer), and a completeness check
 * over `FieldWidget` is a type error rather than a runtime fallthrough.
 *
 * `satisfies Record<FieldWidget, …>` is doing that work: adding a widget to the
 * union without adding it here fails to compile.
 */
export type FieldComponent = (props: {
  binding: FieldBinding;
  /** Present for reference fields, which resolve against the node reads. */
  children?: ReactNode;
}) => ReactNode;

const switchStyles = toggleSwitch();

const switchRow = css({
  display: 'flex',
  alignItems: 'center',
  gap: '3',
  justifyContent: 'space-between',
});

const switchLabel = css({
  fontSize: 'sm',
  color: 'text.default',
});

const helpText = css({
  fontSize: 'xs',
  color: 'text.muted',
  marginTop: '1',
});

const errorText = css({
  fontSize: 'xs',
  color: 'text.critical',
  marginTop: '1',
});

const monospace = css({
  fontFamily: 'mono',
  minHeight: '20',
});

/** The frame the design system's own `TextInput` builds in, for controls that
 * do not carry one. */
function FieldFrame({
  binding,
  children,
}: {
  binding: FieldBinding;
  children: ReactNode;
}) {
  const { plan, error } = binding;

  return (
    <Field.Root required={plan.required} invalid={error !== undefined}>
      <Field.Label>{plan.label}</Field.Label>
      {children}
      {error === undefined ? (
        plan.help !== undefined && <span className={helpText}>{plan.help}</span>
      ) : (
        <span className={errorText}>{error}</span>
      )}
    </Field.Root>
  );
}

function TextField({ binding }: { binding: FieldBinding }) {
  const { plan, value, error, setValue, onBlur } = binding;

  return (
    <TextInput
      label={plan.label}
      required={plan.required}
      invalid={error !== undefined}
      {...(error === undefined
        ? plan.help !== undefined && { helperText: plan.help }
        : { errorText: error })}
      {...(plan.placeholder !== undefined && { placeholder: plan.placeholder })}
      {...(plan.maxLength !== undefined && { maxLength: plan.maxLength })}
      value={asText(value)}
      onChange={(event) => setValue(event.target.value)}
      onBlur={onBlur}
    />
  );
}

function DecimalField({ binding }: { binding: FieldBinding }) {
  const { plan, value, error, setValue, onBlur } = binding;

  return (
    <TextInput
      label={plan.label}
      required={plan.required}
      invalid={error !== undefined}
      {...(error === undefined
        ? plan.help !== undefined && { helperText: plan.help }
        : { errorText: error })}
      // Deliberately not `type="number"`: an exact decimal is a string all the
      // way to the database (numeric(30,18), never a float), and a number input
      // would let the browser reformat it — dropping trailing zeros and
      // switching to exponential form on long values, both of which change the
      // stored text.
      inputMode="decimal"
      value={asText(value)}
      onChange={(event) => setValue(event.target.value)}
      onBlur={onBlur}
    />
  );
}

function NumberField({ binding }: { binding: FieldBinding }) {
  const { plan, value, error, setValue, onBlur } = binding;

  return (
    <TextInput
      label={plan.label}
      required={plan.required}
      invalid={error !== undefined}
      {...(error === undefined
        ? plan.help !== undefined && { helperText: plan.help }
        : { errorText: error })}
      type="number"
      {...(plan.min !== undefined && { min: plan.min })}
      {...(plan.max !== undefined && { max: plan.max })}
      value={asText(value)}
      onChange={(event) => setValue(event.target.value)}
      onBlur={onBlur}
    />
  );
}

function DateField({ binding }: { binding: FieldBinding }) {
  const { plan, value, error, setValue, onBlur } = binding;

  return (
    <TextInput
      label={plan.label}
      required={plan.required}
      invalid={error !== undefined}
      {...(error === undefined
        ? plan.help !== undefined && { helperText: plan.help }
        : { errorText: error })}
      type="date"
      value={asText(value)}
      onChange={(event) => setValue(event.target.value)}
      onBlur={onBlur}
    />
  );
}

function TextareaField({ binding }: { binding: FieldBinding }) {
  const { plan, value, error, setValue, onBlur } = binding;

  return (
    <Textarea
      label={plan.label}
      required={plan.required}
      invalid={error !== undefined}
      {...(error === undefined
        ? plan.help !== undefined && { helperText: plan.help }
        : { errorText: error })}
      rows={3}
      value={asText(value)}
      onChange={(event) => setValue(event.target.value)}
      onBlur={onBlur}
    />
  );
}

function SelectField({ binding }: { binding: FieldBinding }) {
  const { plan, value, setValue, onBlur } = binding;

  return (
    <FieldFrame binding={binding}>
      <Select
        value={asText(value)}
        onChange={(event) => setValue(event.target.value)}
        onBlur={onBlur}
      >
        {/* An explicit empty option, because a select with no value silently
            reports its first one — which for a governed vocabulary means a row
            classified by accident rather than left for curation. */}
        <option value="">{plan.required ? 'Choose…' : '— none —'}</option>
        {(plan.options ?? []).map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </Select>
    </FieldFrame>
  );
}

function SwitchField({ binding }: { binding: FieldBinding }) {
  const { plan, value, error, setValue } = binding;

  return (
    <Field.Root invalid={error !== undefined}>
      <div className={switchRow}>
        <Switch.Root
          checked={value === true}
          onCheckedChange={(details) => setValue(details.checked)}
          className={switchRow}
        >
          <Switch.Label className={switchLabel}>{plan.label}</Switch.Label>
          <Switch.Control className={switchStyles.root}>
            <Switch.Thumb className={switchStyles.thumb} />
          </Switch.Control>
          <Switch.HiddenInput />
        </Switch.Root>
      </div>
      {error === undefined ? (
        plan.help !== undefined && <span className={helpText}>{plan.help}</span>
      ) : (
        <span className={errorText}>{error}</span>
      )}
    </Field.Root>
  );
}

function JsonField({ binding }: { binding: FieldBinding }) {
  const { plan, value, error, setValue, onBlur } = binding;
  const text =
    typeof value === 'string' ? value : JSON.stringify(value ?? {}, null, 2);

  return (
    <Textarea
      label={plan.label}
      required={plan.required}
      invalid={error !== undefined}
      {...(error === undefined
        ? plan.help !== undefined && { helperText: plan.help }
        : { errorText: error })}
      className={monospace}
      rows={5}
      value={text}
      onChange={(event) => setValue(event.target.value)}
      onBlur={onBlur}
    />
  );
}

function ReadonlyField({ binding }: { binding: FieldBinding }) {
  return (
    <TextInput
      label={binding.plan.label}
      readOnly
      {...(binding.plan.help !== undefined && {
        helperText: binding.plan.help,
      })}
      value={asText(binding.value)}
    />
  );
}

export const fieldManifest = {
  text: TextField,
  textarea: TextareaField,
  select: SelectField,
  switch: SwitchField,
  number: NumberField,
  decimal: DecimalField,
  date: DateField,
  reference: ReferencePicker,
  json: JsonField,
  readonly: ReadonlyField,
} satisfies Record<FieldWidget, FieldComponent>;
