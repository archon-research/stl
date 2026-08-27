import boundariesConfig from '@archon-research/oxlint-config/design-system-boundaries';

const config = {
  ...boundariesConfig,
  categories: {
    correctness: 'error',
    suspicious: 'error',
  },
  rules: {
    ...(boundariesConfig.rules ?? {}),
    'import/no-unassigned-import': 'off',
    'no-console': 'error',
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
