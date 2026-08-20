import assert from 'node:assert/strict';

import { createServer } from 'vite';

async function main() {
  const vite = await createServer({
    appType: 'custom',
    logLevel: 'error',
    server: { middlewareMode: true },
    // Same reason as check-prime-grouping: only one source file is loaded, and
    // the dependency scan's background rolldown pass over index.html segfaults
    // intermittently, failing the step for an unrelated reason.
    optimizeDeps: { noDiscovery: true },
  });

  try {
    const { balancedColumns } = await vite.ssrLoadModule(
      '/src/lib/dashboard.ts',
    );

    // The case that prompted this: six cards over four columns read as 4 + 2,
    // which leaves the second row half empty next to a full first row.
    assert.equal(balancedColumns(6, 4), 3, 'six cards should be 3 + 3');
    assert.equal(
      balancedColumns(6, 2),
      2,
      'six cards at two columns is 2 + 2 + 2',
    );

    // A row that cannot be filled stays short rather than stretching: five over
    // three is 3 then 2, and grid leaves the last cell empty, so those two keep
    // the width of the three above them.
    assert.equal(balancedColumns(5, 4), 3, 'five cards should be 3 + 2');
    assert.equal(balancedColumns(7, 4), 4, 'seven cards should be 4 + 3');
    assert.equal(balancedColumns(8, 4), 4, 'eight cards should be 4 + 4');

    // A count that already fits one row keeps it, so nothing is wrapped early.
    assert.equal(balancedColumns(4, 4), 4);
    assert.equal(balancedColumns(3, 4), 3);
    assert.equal(balancedColumns(2, 4), 2);

    // Never zero columns: `repeat(0, ...)` is invalid and would drop the grid.
    assert.equal(balancedColumns(1, 4), 1);
    assert.equal(balancedColumns(0, 4), 1);

    // Balanced, never wider than the breakpoint allows.
    for (let count = 1; count <= 12; count += 1) {
      for (const maxColumns of [1, 2, 3, 4]) {
        const columns = balancedColumns(count, maxColumns);
        assert.ok(
          columns >= 1 && columns <= maxColumns,
          `balancedColumns(${count}, ${maxColumns}) = ${columns} is out of range`,
        );
        assert.equal(
          Math.ceil(count / columns),
          Math.ceil(count / maxColumns),
          `balancedColumns(${count}, ${maxColumns}) should not add a row`,
        );
      }
    }

    const { encumbranceSeverity } = await vite.ssrLoadModule(
      '/src/lib/dashboard.ts',
    );

    // The Sky Atlas defines these, so they are pinned rather than left to a
    // constant someone can nudge: at or above 100% is a Low Severity Breach,
    // above 103% is a High Severity Breach.
    assert.equal(encumbranceSeverity(0.99), 'none');
    assert.equal(
      encumbranceSeverity(1),
      'low',
      '100% is a breach, not a warning',
    );
    assert.equal(encumbranceSeverity(1.0299), 'low');
    assert.equal(encumbranceSeverity(1.04), 'high');

    // Exactly 103% falls outside both written definitions ("below 103%" and
    // "above 103%" each exclude it); read as high, the conservative side.
    assert.equal(encumbranceSeverity(1.03), 'high');

    // Absence is not a breach.
    for (const value of [null, undefined, Number.NaN]) {
      assert.equal(encumbranceSeverity(value), 'none');
    }

    console.log(
      'metrics grid columns balance rows, and Atlas breach levels hold.',
    );
  } finally {
    await vite.close();
  }
}

await main();
