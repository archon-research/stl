import assert from 'node:assert/strict';

import { createServer } from 'vite';

/** The allocation-row fields these invariants actually key, label or link off. */
type AllocationRowFixture = {
  chain_id: number | null;
  network: string | null;
  receipt_token_id: string | null;
  underlying_token_id: string | null;
  symbol: string;
};

/** One entry of the network filter, as `buildNetworkOptions` returns it. */
type NetworkOption = {
  value: string;
  count: number;
};

/**
 * Reference rows can name a chain STL has no id for, which arrives as a null
 * `chain_id` with the upstream name in `network`. Every invariant below was a
 * real defect at some point in that change, and none is visible from a type.
 */
async function main() {
  const vite = await createServer({
    appType: 'custom',
    logLevel: 'error',
    server: { middlewareMode: true },
    // Same reason as check-metrics-grid: one source file is loaded, and the
    // dependency scan's background rolldown pass over index.html segfaults
    // intermittently, failing the step for an unrelated reason.
    optimizeDeps: { noDiscovery: true },
  });

  try {
    const {
      allocationNetworkKey,
      buildNetworkOptions,
      getAllocationKey,
      getChainLabel,
      getExplorerUrl,
    } = await vite.ssrLoadModule('/src/shared/lib/dashboard.ts');

    const row = (
      overrides: Partial<AllocationRowFixture> = {},
    ): AllocationRowFixture => ({
      chain_id: 1,
      network: null,
      receipt_token_id: null,
      underlying_token_id: null,
      symbol: 'X',
      ...overrides,
    });

    // The case that prompted this: keyed on chain_id alone, every unindexed
    // chain shares the key `null` and the network filter shows them as one.
    const plume = row({ chain_id: null, network: 'plume' });
    const robinhood = row({ chain_id: null, network: 'robinhood' });
    assert.notEqual(
      allocationNetworkKey(plume),
      allocationNetworkKey(robinhood),
      'two unindexed chains must not collapse into one network',
    );
    assert.equal(allocationNetworkKey(row({ chain_id: 1 })), '1');
    assert.equal(
      allocationNetworkKey(row({ chain_id: null, network: null })),
      allocationNetworkKey(row({ chain_id: null, network: null })),
      'an unnamed unindexed chain still keys consistently',
    );

    // A chain id must never collide with a network name: `net:` is the guard.
    assert.notEqual(
      allocationNetworkKey(row({ chain_id: null, network: '1' })),
      allocationNetworkKey(row({ chain_id: 1 })),
      'a network literally named "1" is not chain 1',
    );

    // Row identity: two direct holdings of the same asset on different
    // unindexed chains are two rows, and React keys off this.
    assert.notEqual(
      getAllocationKey(plume),
      getAllocationKey(robinhood),
      'row keys must stay unique across unindexed chains',
    );

    // Labels: the upstream slug is the only name such a row carries.
    assert.equal(getChainLabel(null, undefined, 'plume'), 'Plume');
    assert.equal(getChainLabel(undefined, undefined, 'robinhood'), 'Robinhood');
    assert.equal(getChainLabel(null, undefined, null), 'Unknown chain');
    assert.equal(getChainLabel(null, undefined, ''), 'Unknown chain');
    // 0 is off-chain custody, which is not the same thing as an unknown chain.
    assert.equal(getChainLabel(0, undefined, 'ethereum'), 'Off-chain');
    assert.equal(getChainLabel(1), 'Ethereum');

    // The explorer link is the dangerous one: mainnet's explorer renders a page
    // for any address, so a null chain must suppress the link, not default it.
    assert.equal(
      getExplorerUrl(null, '0x' + '11'.repeat(20)),
      null,
      'a chain with no id must not borrow another chain’s explorer',
    );
    assert.ok(getExplorerUrl(1, '0x' + '11'.repeat(20))?.includes('etherscan'));

    // The filter has three producers of a network value and one consumer, and
    // they must agree exactly; a mismatch reads as "the filter selects nothing"
    // or, worse, as an unfiltered query behind an active-looking chip.
    const mixed = [
      row({ chain_id: 1, symbol: 'A' }),
      row({ chain_id: 1, symbol: 'B' }),
      row({ chain_id: 8453, symbol: 'C' }),
      plume,
      robinhood,
    ];
    const options: NetworkOption[] = buildNetworkOptions(mixed);
    assert.deepEqual(
      new Set(options.map((option) => option.value)),
      new Set(mixed.map(allocationNetworkKey)),
      'every option value must be a key the filter predicate computes',
    );
    assert.deepEqual(
      options.map((option) => option.count),
      [2, 1, 1, 1],
      'counts accumulate per network',
    );
    // Indexed chains keep their chain-id order, so adding an unmapped chain
    // does not reshuffle the list for every other prime.
    assert.deepEqual(
      options.map((option) => option.value),
      ['1', '8453', 'net:plume', 'net:robinhood'],
      'indexed chains sort by id and lead the unmapped ones',
    );

    console.log(
      'unindexed chains: distinct keys, agreeing filter options, labels fall back, no borrowed explorer.',
    );
  } finally {
    await vite.close();
  }
}

await main();
