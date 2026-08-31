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
    // one is closer to a design change than a cleanup. 25 findings.
    'typescript/no-unsafe-type-assertion': 'off',
    // Fires on the standard effect shape: one branch returns a cleanup, the
    // early-out returns nothing. 14 findings.
    'typescript/consistent-return': 'off',
    // Genuinely dead assertions, safe to remove but all in App.tsx and the
    // chart hooks. 9 findings.
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
