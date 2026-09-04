import boundariesConfig from '@archon-research/oxlint-config/design-system-boundaries';

const config = {
  ...boundariesConfig,
  plugins: [...boundariesConfig.plugins, 'vitest'],
  categories: {
    correctness: 'error',
    suspicious: 'error',
  },
  rules: {
    ...boundariesConfig.rules,
    'no-console': 'error',
    'typescript/no-explicit-any': 'error',

    // Set just above the largest thing here today, so nothing needs fixing but
    // nothing new may exceed them. Lower over time.
    'max-lines-per-function': ['error', { max: 500 }],
    'max-lines': ['error', { max: 950 }],
    'import/max-dependencies': ['error', { max: 21 }],

    'react/react-compiler': 'off',

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
      files: ['scripts/**'],
      rules: {
        'no-console': 'off',
      },
    },
    {
      // Length tracks how much surface a test covers, so it gets a looser
      // limit rather than none; the per-function limit still applies.
      files: ['**/*.test.ts', '**/*.test.tsx'],
      rules: {
        'max-lines': ['error', { max: 1150 }],
      },
    },
  ],
  ignorePatterns: ['dist', 'src/generated'],
};

export default config;
