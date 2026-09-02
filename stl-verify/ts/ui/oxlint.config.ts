import boundariesConfig from '@archon-research/oxlint-config/design-system-boundaries';

const config = {
  ...boundariesConfig,
  // The preset has no reason to know about a test runner, so the vitest rules
  // are inert until named here -- and they are the only gate on the unit tests.
  plugins: [...boundariesConfig.plugins, 'vitest'],
  categories: {
    correctness: 'error',
    suspicious: 'error',
  },
  rules: {
    ...boundariesConfig.rules,
    'no-console': 'error',

    // The one rule the preset does not name that this app still wants.
    // `restriction` would reach it, and would also carry `max-lines` and
    // friends. 0 violations, so it is a ratchet rather than a backlog.
    'typescript/no-explicit-any': 'error',

    // The lint half of the React Compiler's own analysis, and the reason the
    // compiler is wired into the build at all. Off, not passing: the one
    // finding it had that the compiler would actually miscompile -- a ref
    // written during render in `RiskDetailDrawer` -- is fixed, and the 5 that
    // remain are all `EffectSetState`/`EffectDerivationsOfState` on debounce
    // timers and derived state. Each is a real refactor with its own
    // behavioural risk, so they are a cleanup PR rather than a blocker. Turn
    // this to 'error' once they are gone; `--max-warnings=0` means 'warn'
    // would fail CI just the same.
    'react/react-compiler': 'off',

    // Type-aware rules (`--type-aware` in the lint script). The promise-safety
    // family is the reason the flag is on at all: without it these sit in the
    // effective config reading as coverage while never executing.
    'typescript/no-floating-promises': 'error',
    'typescript/no-misused-promises': 'error',
    'typescript/await-thenable': 'error',
    'typescript/no-base-to-string': 'error',

    // Narrowing a response type by assertion was how this app described every
    // envelope, which is exactly why it is a rule and not a count: each one
    // claimed a shape the schema had not promised, and a schema change would
    // not have contradicted a single one of them. One survives, carrying a
    // disable comment that names what removes it (VEC-686); a second would need
    // the same, which is the point -- an exception has to be argued for.
    'typescript/no-unsafe-type-assertion': 'error',

    // The style rules `--type-aware` also switches on. Off for now, with the
    // count each currently reports across src/ -- turning one on is a cleanup
    // PR of that size, not a side effect of enabling promise safety.
    // Fires on the standard effect shape: one branch returns a cleanup, the
    // early-out returns nothing. 4 findings.
    'typescript/consistent-return': 'off',
    // Genuinely dead assertions, safe to remove but spread across the query
    // selects, the chart builder and the allocation route. 8 findings.
    'typescript/no-unnecessary-type-assertion': 'off',
    // 2 findings, both on router helpers where the parameter documents intent.
    'typescript/no-unnecessary-type-parameters': 'off',
  },
  overrides: [
    {
      // The regression checks are CLIs: their reports go to stdout.
      files: ['scripts/**'],
      rules: {
        'no-console': 'off',
      },
    },
  ],
  ignorePatterns: ['dist', 'src/generated'],
};

export default config;
