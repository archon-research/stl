import { onchainTable } from "ponder";
import { Protocol } from "@/schema/common/protocol";
import { Token } from "@/schema/common/token";
import { User } from "@/schema/common/user";
import { createUserPositionBreakdownTable } from "@/schema/aave-v3/user-position-breakdown";

/**
 * Sparklend Snapshot Schema Factory
 *
 * Generates chain-specific snapshot and health factor tables.
 * All snapshot tables reference normalized Protocol, Token, and User tables.
 */

export function createSparklendSnapshotTables(chainName: string) {
  const prefix = `Sparklend${chainName}`;

  return {
    // User's supply position with SCALED balances for interest calculation
    UserScaledSupplyPosition: onchainTable(`${prefix}UserScaledSupplyPosition`, (t) => ({
      id: t.text().primaryKey(), // `sparklend-${chain}-${userId}-${reserveId}-${blockNumber}`
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      userId: t.text().notNull().references(() => User.id), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      blockNumber: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
      scaledBalance: t.bigint().notNull(),
      isCollateral: t.boolean().notNull(),
      lastLiquidityIndex: t.bigint().notNull(),
    })),

    // User's borrow position with SCALED balances for interest calculation
    UserScaledBorrowPosition: onchainTable(`${prefix}UserScaledBorrowPosition`, (t) => ({
      id: t.text().primaryKey(), // `sparklend-${chain}-${userId}-${reserveId}-${blockNumber}`
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      userId: t.text().notNull().references(() => User.id), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      blockNumber: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
      scaledVariableDebt: t.bigint().notNull(),
      lastVariableBorrowIndex: t.bigint().notNull(),
    })),

    // User's eMode category
    UserEModeCategory: onchainTable(`${prefix}UserEModeCategory`, (t) => ({
      id: t.text().primaryKey(), // `sparklend-${chain}-${userId}-${blockNumber}`
      protocolId: t.text().notNull().references(() => Protocol.id),
      userId: t.text().notNull().references(() => User.id), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      blockNumber: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
      categoryId: t.integer().notNull(),
    })),

    // Health factor history for a specific user
    UserHealthFactorHistory: onchainTable(`${prefix}UserHealthFactorHistory`, (t) => ({
      id: t.text().primaryKey(), // `sparklend-${chain}-${userId}-${blockNumber}`
      protocolId: t.text().notNull().references(() => Protocol.id),
      userId: t.text().notNull().references(() => User.id), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      healthFactor: t.bigint().notNull(),
      totalCollateralBase: t.bigint().notNull(),
      totalDebtBase: t.bigint().notNull(),
      blockNumber: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
    })),

    // Detailed position breakdown for a user at a specific block (using shared factory)
    UserPositionBreakdown: createUserPositionBreakdownTable(`${prefix}UserPositionBreakdown`),
  };
}
