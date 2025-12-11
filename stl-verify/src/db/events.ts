/**
 * Database operations for SparkLend events
 */

import type { Database } from "bun:sqlite";
import type { ChainId } from "../config/chains";
import type { SparkLendEvents } from "../services/events";

/**
 * Save SparkLend events to the database
 */
export function saveEventsToDatabase(
  db: Database,
  chainId: ChainId,
  events: SparkLendEvents
): void {
  const insertSupply = db.query(`
    INSERT OR IGNORE INTO supply_events 
    (chain_id, block_number, transaction_hash, log_index, reserve, user, on_behalf_of, amount, referral_code)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertBorrow = db.query(`
    INSERT OR IGNORE INTO borrow_events 
    (chain_id, block_number, transaction_hash, log_index, reserve, user, on_behalf_of, amount, interest_rate_mode, borrow_rate, referral_code)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertLiquidation = db.query(`
    INSERT OR IGNORE INTO liquidation_call_events 
    (chain_id, block_number, transaction_hash, log_index, collateral_asset, debt_asset, user, debt_to_cover, liquidated_collateral_amount, liquidator, receive_atoken)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertWithdraw = db.query(`
    INSERT OR IGNORE INTO withdraw_events 
    (chain_id, block_number, transaction_hash, log_index, reserve, user, to_address, amount)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertRepay = db.query(`
    INSERT OR IGNORE INTO repay_events 
    (chain_id, block_number, transaction_hash, log_index, reserve, user, repayer, amount, use_atokens)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  // Use Bun's transaction API
  db.transaction(() => {
    for (const event of events.supply) {
      insertSupply.run(
        chainId,
        event.blockNumber,
        event.transactionHash,
        event.logIndex,
        event.reserve,
        event.user,
        event.onBehalfOf,
        event.amount,
        event.referralCode
      );
    }
    for (const event of events.borrow) {
      insertBorrow.run(
        chainId,
        event.blockNumber,
        event.transactionHash,
        event.logIndex,
        event.reserve,
        event.user,
        event.onBehalfOf,
        event.amount,
        event.interestRateMode,
        event.borrowRate,
        event.referralCode
      );
    }
    for (const event of events.liquidationCall) {
      insertLiquidation.run(
        chainId,
        event.blockNumber,
        event.transactionHash,
        event.logIndex,
        event.collateralAsset,
        event.debtAsset,
        event.user,
        event.debtToCover,
        event.liquidatedCollateralAmount,
        event.liquidator,
        event.receiveAToken ? 1 : 0
      );
    }
    for (const event of events.withdraw) {
      insertWithdraw.run(
        chainId,
        event.blockNumber,
        event.transactionHash,
        event.logIndex,
        event.reserve,
        event.user,
        event.to,
        event.amount
      );
    }
    for (const event of events.repay) {
      insertRepay.run(
        chainId,
        event.blockNumber,
        event.transactionHash,
        event.logIndex,
        event.reserve,
        event.user,
        event.repayer,
        event.amount,
        event.useATokens ? 1 : 0
      );
    }
  })();
}

export function getTopLiquidators(
  db: Database,
  chainId: ChainId,
  limit: number = 10
): Array<{ liquidator: string; totalLiquidated: bigint }> {
  const query = db.query(`
    SELECT liquidator, count(*) AS total_liquidated
    FROM liquidation_call_events
    WHERE chain_id = ?
    GROUP BY liquidator
    ORDER BY total_liquidated DESC
    LIMIT ?
  `);

  const results: Array<{ liquidator: string; totalLiquidated: bigint }> = [];
  for (const row of query.iterate(chainId, limit)) {
    results.push({
      liquidator: row.liquidator,
      totalLiquidated: row.total_liquidated
    });
  }
  return results;
}