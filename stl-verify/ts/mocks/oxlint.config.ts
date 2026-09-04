import baseConfig from '@archon-research/oxlint-config/base';

const config = {
  ...baseConfig,
  categories: {
    correctness: 'error',
    suspicious: 'error',
  },
  rules: {
    // Spread rather than replaced: `base` ships `rules: {}` so that a preset
    // rule added later is not silently dropped by the overrides below.
    ...baseConfig.rules,
    'no-console': 'error',

    // Ratchet (ORB-377), matching ui/oxlint.config.ts: 0 violations at the
    // rule's own default (10), so no local threshold is needed here.
    'import/max-dependencies': 'error',

    // Type-aware rules (`--type-aware` in the lint script). Kept in step with
    // ui/oxlint.config.ts so a floating promise is caught on either side of the
    // workspace; see that file for why the style rules below are off.
    'typescript/no-floating-promises': 'error',
    'typescript/no-misused-promises': 'error',
    'typescript/await-thenable': 'error',
    'typescript/no-base-to-string': 'error',
    'typescript/no-unsafe-type-assertion': 'error',
    'typescript/consistent-return': 'off',
    'typescript/no-unnecessary-type-assertion': 'off',
    'typescript/no-unnecessary-type-parameters': 'off',
  },
  overrides: [
    {
      // The self-test is a CLI: its report goes to stdout.
      files: ['scripts/**'],
      rules: {
        'no-console': 'off',
        // 6 findings, all bridging openapi-fetch's generics (`MaybeOptionalInit`,
        // `Extract<Rows[number], ...>`) in the harness's own plumbing rather
        // than describing a response. The fixtures and handlers are what the
        // rule is guarding, and they are clean.
        'typescript/no-unsafe-type-assertion': 'off',
      },
    },
  ],
};

export default config;
