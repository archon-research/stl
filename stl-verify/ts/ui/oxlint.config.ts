import boundariesConfig from '@archon-research/oxlint-config/design-system-boundaries';

const presetRules = boundariesConfig.rules ?? {};

/**
 * Raise a preset rule's severity without restating its options. A bare severity
 * string replaces the whole entry, which would silently drop the restricted
 * paths the boundaries preset exists to carry.
 */
const denied = (name: keyof typeof presetRules) => {
  const entry = presetRules[name];

  return Array.isArray(entry) ? ['error', ...entry.slice(1)] : 'error';
};

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
    ...presetRules,
    // The preset ships the design-system boundary at 'warn'. oxlint exits 0 on
    // warnings, so left alone it can never fail a build or block a commit.
    'no-restricted-imports': denied('no-restricted-imports'),
    'no-console': 'error',

    // Not reachable from `correctness` + `suspicious`: oxlint files these under
    // `pedantic` and `restriction`, which also carry `max-lines` and friends.
    // Named individually so the categories stay narrow. All three measured at
    // 0 violations when enabled, so they are ratchets, not a backlog.
    'react/rules-of-hooks': 'error',
    'import/no-cycle': 'error',
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

    // The style rules `--type-aware` also switches on. Off for now, with the
    // count each currently reports across src/ -- turning one on is a cleanup
    // PR of that size, not a side effect of enabling promise safety.
    //
    // Narrowing an API envelope union by assertion is the app's normal shape
    // (the response schema cannot express which arm a query selects), so this
    // one is closer to a design change than a cleanup. 33 findings, of which 14
    // are the unit tests asserting fixture literals into response types; app
    // code carries 19, down from 25, because the query migration collected the
    // envelope narrowing into `queries.ts`'s selects instead of spreading it.
    'typescript/no-unsafe-type-assertion': 'off',
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
