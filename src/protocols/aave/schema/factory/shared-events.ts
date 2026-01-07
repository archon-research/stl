/**
 * Aave Shared Event Tables Factory
 * 
 * These events are emitted by both Core and Horizon markets.
 * Tables are generated per chain and per market (e.g., AaveMainnetCoreBorrow, AaveMainnetHorizonBorrow).
 * All events reference normalized Protocol, Token, and User tables.
 */

import { onchainTable } from "ponder";

export function createAaveSharedEventTables(chainName: string, market: "Core" | "Horizon") {
  const prefix = `Aave${chainName}${market}`;

  return {
    Borrow: onchainTable(`${prefix}Borrow`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      userId: t.text().notNull(), // FK to User (initiator)
      onBehalfOfId: t.text().notNull(), // FK to User (beneficiary)
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

    DeficitCovered: onchainTable(`${prefix}DeficitCovered`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      callerId: t.text().notNull(), // FK to User
      caller: t.hex().notNull(), // Raw address for convenience
      amountCovered: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    DeficitCreated: onchainTable(`${prefix}DeficitCreated`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      userId: t.text().notNull(), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      amountCreated: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    FlashLoan: onchainTable(`${prefix}FlashLoan`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      initiatorId: t.text().notNull(), // FK to User
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

    IsolationModeTotalDebtUpdated: onchainTable(`${prefix}IsolationModeTotalDebtUpdated`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      totalDebt: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    LiquidationCall: onchainTable(`${prefix}LiquidationCall`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      collateralReserveId: t.text().notNull(),
      debtReserveId: t.text().notNull(),
      userId: t.text().notNull(), // FK to User (liquidated user)
      liquidatorId: t.text().notNull(), // FK to User (liquidator)
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

    MintedToTreasury: onchainTable(`${prefix}MintedToTreasury`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      amountMinted: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    Repay: onchainTable(`${prefix}Repay`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      userId: t.text().notNull(), // FK to User (borrower)
      repayerId: t.text().notNull(), // FK to User (repayer)
      user: t.hex().notNull(), // Raw address for convenience
      repayer: t.hex().notNull(), // Raw address for convenience
      amount: t.bigint().notNull(),
      useATokens: t.boolean().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    ReserveDataUpdated: onchainTable(`${prefix}ReserveDataUpdated`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      reserveConfigurationId: t.text().notNull(),
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

    ReserveUsedAsCollateralDisabled: onchainTable(`${prefix}ReserveUsedAsCollateralDisabled`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      userId: t.text().notNull(), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    ReserveUsedAsCollateralEnabled: onchainTable(`${prefix}ReserveUsedAsCollateralEnabled`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      userId: t.text().notNull(), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    Supply: onchainTable(`${prefix}Supply`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      userId: t.text().notNull(), // FK to User (initiator)
      onBehalfOfId: t.text().notNull(), // FK to User (beneficiary)
      user: t.hex().notNull(), // Raw address for convenience
      onBehalfOf: t.hex().notNull(), // Raw address for convenience
      amount: t.bigint().notNull(),
      referralCode: t.integer().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    UserEModeSet: onchainTable(`${prefix}UserEModeSet`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      userId: t.text().notNull(), // FK to User
      user: t.hex().notNull(), // Raw address for convenience
      categoryId: t.integer().notNull(),
      timestamp: t.bigint().notNull(),
      transactionHash: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      logIndex: t.bigint().notNull(),
    })),

    Withdraw: onchainTable(`${prefix}Withdraw`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      userId: t.text().notNull(), // FK to User
      toId: t.text().notNull(), // FK to User (recipient)
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
