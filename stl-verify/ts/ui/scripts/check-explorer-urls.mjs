import assert from 'node:assert/strict';

import { createServer } from 'vite';

const ADDRESS = '0x1111111111111111111111111111111111111111';
const TX_HASH = `0x${'2'.repeat(64)}`;

const explorerCases = [
  [1, 'https://etherscan.io'],
  [10, 'https://optimistic.etherscan.io'],
  [137, 'https://polygonscan.com'],
  [324, 'https://explorer.zksync.io'],
  [130, 'https://uniscan.xyz'],
  [8453, 'https://basescan.org'],
  [42161, 'https://arbiscan.io'],
  [43114, 'https://snowtrace.io'],
];

const vite = await createServer({
  appType: 'custom',
  logLevel: 'error',
  server: { middlewareMode: true },
});

try {
  const { getExplorerUrl } = await vite.ssrLoadModule('/src/lib/dashboard.ts');

  for (const [chainId, explorerBase] of explorerCases) {
    assert.equal(
      getExplorerUrl(chainId, ADDRESS),
      `${explorerBase}/address/${ADDRESS}`,
    );
    assert.equal(
      getExplorerUrl(chainId, TX_HASH, 'tx'),
      `${explorerBase}/tx/${TX_HASH}`,
    );
  }

  assert.equal(getExplorerUrl(999999, ADDRESS), null);
} finally {
  await vite.close();
}
