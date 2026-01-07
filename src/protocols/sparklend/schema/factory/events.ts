import { onchainTable } from "ponder";

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
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      backerId: t.text().notNull(), // FK to User
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

    // FlashLoan Event
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

    // IsolationModeTotalDebtUpdated Event
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

    // LiquidationCall Event
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

    // MintedToTreasury Event (no user, treasury is protocol-controlled)
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

    // MintUnbacked Event
    MintUnbacked: onchainTable(`${prefix}MintUnbacked`, (t) => ({
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

    // Repay Event
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

    // ReserveDataUpdated Event (no user, protocol-wide reserve update)
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

    // ReserveUsedAsCollateralDisabled Event
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

    // ReserveUsedAsCollateralEnabled Event
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

    // Supply Event
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

    // SwapBorrowRateMode Event
    SwapBorrowRateMode: onchainTable(`${prefix}SwapBorrowRateMode`, (t) => ({
      id: t.text().primaryKey(),
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      userId: t.text().notNull(), // FK to User
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
      protocolId: t.text().notNull(),
      userId: t.text().notNull(), // FK to User
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
