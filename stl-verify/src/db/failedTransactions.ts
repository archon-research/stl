/**
 * Persistence helpers for failed liquidation call traces
 */

import type { Database } from "bun:sqlite";
import type { ChainId } from "../config/chains";
import type { FailedLiquidationRow } from "../services/failedTransactions";

export function saveFailedLiquidations(
  db: Database,
  chainId: ChainId,
  rows: FailedLiquidationRow[]
): void {
  if (rows.length === 0) return;

  const stmt = db.query(`
    INSERT OR IGNORE INTO failed_liquidation_calls (
      chain_id, block_number, tx_hash, from_address, to_address, error, gas_used
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  db.transaction(() => {
    for (const row of rows) {
      stmt.run(
        chainId,
        row.blockNumber,
        row.txHash,
        row.from,
        row.to,
        row.error,
        row.gasUsed
      );
    }
  })();
}
