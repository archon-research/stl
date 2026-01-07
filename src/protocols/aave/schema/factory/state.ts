import { onchainTable } from "ponder";
import { Protocol } from "@/schema/common/protocol";
import { Token } from "@/schema/common/token";
import { User } from "@/schema/common/user";

/**
 * Aave State Schema Factory
 *
 * Generates chain-specific state tracking tables for both Core and Horizon markets.
 * All state tables reference normalized Protocol, Token, and User tables.
 */

export function createAaveStateTables(chainName: string, market: "Core" | "Horizon") {
  const prefix = `Aave${chainName}${market}`;

  return {
    // User's supply position for a specific asset
    UserSupplyPosition: onchainTable(`${prefix}UserSupplyPosition`, (t) => ({
      id: t.text().primaryKey(), // `aave-${market}-${chain}-${userId}-${reserveId}`
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      userId: t.text().notNull().references(() => User.id), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      balance: t.bigint().notNull(), // Current supply balance (in asset units)
      isCollateral: t.boolean().notNull(), // Whether this supply is used as collateral
      lastUpdateBlockNumber: t.bigint().notNull(),
      lastUpdateTimestamp: t.bigint().notNull(),
    })),

    // User's borrow position for a specific asset
    UserBorrowPosition: onchainTable(`${prefix}UserBorrowPosition`, (t) => ({
      id: t.text().primaryKey(), // `aave-${market}-${chain}-${userId}-${reserveId}`
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      userId: t.text().notNull().references(() => User.id), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      stableDebt: t.bigint().notNull(), // Stable rate borrow amount
      variableDebt: t.bigint().notNull(), // Variable rate borrow amount
      lastUpdateBlockNumber: t.bigint().notNull(),
      lastUpdateTimestamp: t.bigint().notNull(),
    })),

    // Track all active users (anyone who has ever interacted with the protocol)
    // This is market-specific (user may be active in Core but not Horizon)
    ActiveUser: onchainTable(`${prefix}ActiveUser`, (t) => ({
      id: t.text().primaryKey(), // `${protocolId}-${userId}`
      protocolId: t.text().notNull().references(() => Protocol.id),
      userId: t.text().notNull().references(() => User.id), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      firstSeenBlock: t.bigint().notNull(),
      firstSeenTimestamp: t.bigint().notNull(),
      lastActivityBlock: t.bigint().notNull(),
      lastActivityTimestamp: t.bigint().notNull(),
      hasActivePosition: t.boolean().notNull(), // true if user currently has supply or borrow
    })),
  };
}
