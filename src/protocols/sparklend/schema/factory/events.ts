import { onchainTable } from "ponder";
import { Protocol } from "@/schema/common/protocol";
import { Token } from "@/schema/common/token";
import { User } from "@/schema/common/user";
import { ReserveConfig } from "@/schema/aave-v3/reserve-config";

/**
 * Sparklend Event Schema Factory
 *
 * Generates chain-specific event tables to keep different chains isolated.
 * Each chain gets its own set of tables (e.g., SparklendMainnetBorrow, SparklendGnosisBorrow)
 * 
 * All events reference normalized Protocol, Token, and User tables for data consistency.
 */

export function createSparklendEventTables(chainName: string) {
  const prefix = `Sparklend${chainName}`;

  return {
    // BackUnbacked Event
    BackUnbacked: onchainTable(`${prefix}BackUnbacked`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      backerId: t.text().notNull().references(() => User.id), // FK to User
      backer: t.hex().notNull(), // Raw address for convenience
      amount: t.bigint().notNull(),
      fee: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    // Borrow Event
    Borrow: onchainTable(`${prefix}Borrow`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      userId: t.text().notNull().references(() => User.id), // FK to User (initiator)
      onBehalfOfId: t.text().notNull().references(() => User.id), // FK to User (beneficiary)
      user: t.hex().notNull(), // Raw address for convenience
      onBehalfOf: t.hex().notNull(), // Raw address for convenience
      amount: t.bigint().notNull(),
      interestRateMode: t.integer().notNull(),
      borrowRate: t.bigint().notNull(),
      referralCode: t.integer().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    // FlashLoan Event
    FlashLoan: onchainTable(`${prefix}FlashLoan`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      initiatorId: t.text().notNull().references(() => User.id), // FK to User
      target: t.hex().notNull(),
      initiator: t.hex().notNull(), // Raw address for convenience
      amount: t.bigint().notNull(),
      interestRateMode: t.integer().notNull(),
      premium: t.bigint().notNull(),
      referralCode: t.integer().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    // IsolationModeTotalDebtUpdated Event
    IsolationModeTotalDebtUpdated: onchainTable(`${prefix}IsolationModeTotalDebtUpdated`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      totalDebt: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    // LiquidationCall Event
    LiquidationCall: onchainTable(`${prefix}LiquidationCall`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      collateralReserveId: t.text().notNull().references(() => Token.id),
      debtReserveId: t.text().notNull().references(() => Token.id),
      userId: t.text().notNull().references(() => User.id), // FK to User (liquidated user)
      liquidatorId: t.text().notNull().references(() => User.id), // FK to User (liquidator)
      user: t.hex().notNull(), // Raw address for convenience
      debtToCover: t.bigint().notNull(),
      liquidatedCollateralAmount: t.bigint().notNull(),
      liquidator: t.hex().notNull(), // Raw address for convenience
      receiveAToken: t.boolean().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    // MintedToTreasury Event (no user, treasury is protocol-controlled)
    MintedToTreasury: onchainTable(`${prefix}MintedToTreasury`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      amountMinted: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    // MintUnbacked Event
    MintUnbacked: onchainTable(`${prefix}MintUnbacked`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      userId: t.text().notNull().references(() => User.id), // FK to User (initiator)
      onBehalfOfId: t.text().notNull().references(() => User.id), // FK to User (beneficiary)
      user: t.hex().notNull(), // Raw address for convenience
      onBehalfOf: t.hex().notNull(), // Raw address for convenience
      amount: t.bigint().notNull(),
      referralCode: t.integer().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    // Repay Event
    Repay: onchainTable(`${prefix}Repay`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      userId: t.text().notNull().references(() => User.id), // FK to User (borrower)
      repayerId: t.text().notNull().references(() => User.id), // FK to User (repayer)
      user: t.hex().notNull(), // Raw address for convenience
      repayer: t.hex().notNull(), // Raw address for convenience
      amount: t.bigint().notNull(),
      useATokens: t.boolean().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    // ReserveDataUpdated Event (no user, protocol-wide reserve update)
    ReserveDataUpdated: onchainTable(`${prefix}ReserveDataUpdated`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      reserveConfigurationId: t.text().notNull().references(() => ReserveConfig.id),
      liquidityRate: t.bigint().notNull(),
      stableBorrowRate: t.bigint().notNull(),
      variableBorrowRate: t.bigint().notNull(),
      liquidityIndex: t.bigint().notNull(),
      variableBorrowIndex: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    // ReserveUsedAsCollateralDisabled Event
    ReserveUsedAsCollateralDisabled: onchainTable(`${prefix}ReserveUsedAsCollateralDisabled`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      userId: t.text().notNull().references(() => User.id), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    // ReserveUsedAsCollateralEnabled Event
    ReserveUsedAsCollateralEnabled: onchainTable(`${prefix}ReserveUsedAsCollateralEnabled`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      userId: t.text().notNull().references(() => User.id), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    // Supply Event
    Supply: onchainTable(`${prefix}Supply`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      userId: t.text().notNull().references(() => User.id), // FK to User (initiator)
      onBehalfOfId: t.text().notNull().references(() => User.id), // FK to User (beneficiary)
      user: t.hex().notNull(), // Raw address for convenience
      onBehalfOf: t.hex().notNull(), // Raw address for convenience
      amount: t.bigint().notNull(),
      referralCode: t.integer().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    // SwapBorrowRateMode Event
    SwapBorrowRateMode: onchainTable(`${prefix}SwapBorrowRateMode`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      userId: t.text().notNull().references(() => User.id), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      interestRateMode: t.integer().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    // UserEModeSet Event
    UserEModeSet: onchainTable(`${prefix}UserEModeSet`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      userId: t.text().notNull().references(() => User.id), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      categoryId: t.integer().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    // Withdraw Event
    Withdraw: onchainTable(`${prefix}Withdraw`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull().references(() => Protocol.id),
      reserveId: t.text().notNull().references(() => Token.id),
      userId: t.text().notNull().references(() => User.id), // FK to User
      toId: t.text().notNull().references(() => User.id), // FK to User (recipient)
      user: t.hex().notNull(), // Raw address for convenience
      to: t.hex().notNull(), // Raw address for convenience
      amount: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),
  };
}
