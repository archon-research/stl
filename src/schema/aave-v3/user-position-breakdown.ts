import { onchainTable } from "ponder";
import { Protocol } from "@/schema/common/protocol";
import { Token } from "@/schema/common/token";
import { User } from "@/schema/common/user";

/**
 * Factory for User Position Breakdown Table
 * 
 * Creates detailed position breakdown snapshots for users at specific blocks.
 * Used by protocols following Aave V3 architecture (Sparklend, Aave Core, Aave Horizon).
 * 
 * Each protocol/chain combination gets its own table:
 * - SparklendMainnetUserPositionBreakdown
 * - SparklendGnosisUserPositionBreakdown
 * - AaveMainnetCoreUserPositionBreakdown
 * - AaveMainnetHorizonUserPositionBreakdown
 * 
 * @param tableName - Full table name (e.g., "SparklendMainnetUserPositionBreakdown")
 * @returns Ponder table definition
 */
export function createUserPositionBreakdownTable(tableName: string) {
  return onchainTable(tableName, (t) => ({
    // ID format: `${protocolId}-${userId}-${blockNumber}-${reserveId}-${type}`
    // Example: "sparklend-mainnet-mainnet-0x1234...abcd-17000000-mainnet-0x5678...ef01-collateral"
    id: t.text().primaryKey(),
    
    // Foreign Keys
    protocolId: t.text().notNull().references(() => Protocol.id), // FK to Protocol
    reserveId: t.text().notNull().references(() => Token.id),     // FK to Token (underlying asset)
    userId: t.text().notNull().references(() => User.id),         // FK to User
    
    // Raw address for convenience
    user: t.hex().notNull(),
    
    // Snapshot metadata
    blockNumber: t.bigint().notNull(),
    timestamp: t.bigint().notNull(),
    
    // Position details
    positionType: t.text().notNull(), // 'collateral' or 'debt'
    amount: t.text().notNull(),       // Position amount (as string for precision)
    price: t.text().notNull(),        // Asset price at snapshot time (as string for precision)
    valueUSD: t.text().notNull(),     // USD value (amount * price, as string for precision)
    
    // Risk parameters (for collateral positions)
    liquidationThreshold: t.integer(), // Liquidation threshold (null for debt positions)
  }));
}

