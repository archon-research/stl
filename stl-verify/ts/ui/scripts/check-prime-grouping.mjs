import assert from 'node:assert/strict';

import { createServer } from 'vite';

function buildPrimeRow(overrides) {
  return {
    address: '0x0000000000000000000000000000000000000000',
    chain: 'mainnet',
    chain_id: 1,
    id: overrides.address ?? '0x0000000000000000000000000000000000000000',
    name: 'spark',
    prime_vault_address: '0x691a6c29e9e96dd897718305427ad5d534db16ba',
    role: 'alm',
    ...overrides,
  };
}

async function main() {
  const vite = await createServer({
    appType: 'custom',
    logLevel: 'error',
    server: { middlewareMode: true },
  });

  try {
    const { groupPrimesByVault } = await vite.ssrLoadModule(
      '/src/lib/dashboard.ts',
    );

    const sparkMainnet = buildPrimeRow({
      address: '0xaaaa000000000000000000000000000000000a',
      chain: 'mainnet',
      chain_id: 1,
    });
    const sparkAvalanche = buildPrimeRow({
      address: '0xbbbb000000000000000000000000000000000b',
      chain: 'avalanche-c',
      chain_id: 43114,
    });
    const sparkBase = buildPrimeRow({
      address: '0xcccc000000000000000000000000000000000c',
      chain: 'base',
      chain_id: 8453,
    });

    // Three rows for the same prime collapse to a single grouped entry.
    {
      const groups = groupPrimesByVault([
        sparkMainnet,
        sparkAvalanche,
        sparkBase,
      ]);
      assert.equal(groups.length, 1);
      assert.equal(groups[0].chainCount, 3);
    }

    // The grouped entry lists every proxy address of the prime.
    {
      const [group] = groupPrimesByVault([
        sparkMainnet,
        sparkAvalanche,
        sparkBase,
      ]);
      assert.deepEqual(
        [...group.proxyAddresses].sort(),
        [
          sparkMainnet.address,
          sparkAvalanche.address,
          sparkBase.address,
        ].sort(),
      );
    }

    // A null prime_vault_address still yields an entry, keyed on name instead
    // of vanishing.
    {
      const noVault = buildPrimeRow({
        address: '0xdddd000000000000000000000000000000000d',
        name: 'grove',
        prime_vault_address: null,
      });
      const groups = groupPrimesByVault([noVault]);
      assert.equal(groups.length, 1);
      assert.equal(groups[0].key, 'grove');
      assert.equal(groups[0].vaultAddress, null);
    }

    // The primary proxy is the mainnet row even when it is not first in the
    // input order.
    {
      const [group] = groupPrimesByVault([
        sparkAvalanche,
        sparkBase,
        sparkMainnet,
      ]);
      assert.equal(group.primaryProxyAddress, sparkMainnet.address);
    }

    // With no mainnet row present, the primary proxy falls back to the first
    // proxy address in ascending order, so the pick is deterministic.
    {
      const [group] = groupPrimesByVault([sparkBase, sparkAvalanche]);
      const expected = [sparkBase.address, sparkAvalanche.address].sort()[0];
      assert.equal(group.primaryProxyAddress, expected);
    }
  } finally {
    await vite.close();
  }
}

// vite's dev server installs its own process-wide uncaughtException/
// unhandledRejection handlers (for HMR resilience) that log and swallow
// errors instead of letting Node exit non-zero. Awaiting main() and handling
// its rejection explicitly here — rather than letting an assertion failure
// propagate as an unhandled rejection — is what makes a failing assertion
// actually fail this script.
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
