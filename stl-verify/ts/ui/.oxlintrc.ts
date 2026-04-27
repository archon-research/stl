import reactConfig from '@archon-research/oxlint-config/react';

const config = {
  ...reactConfig,
  categories: {
    correctness: 'error',
    suspicious: 'error',
  },
  rules: {
    ...(reactConfig.rules ?? {}),
    'import/no-unassigned-import': 'off',
  },
  ignorePatterns: ['dist', 'src/generated'],
};

export default config;
