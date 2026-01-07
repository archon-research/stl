/**
 * Aave V3 Core Pool Event Handlers (Mainnet)
 * 
 * Handles all events from the Aave V3 Core market on Ethereum Mainnet.
 */

import { ponder } from "ponder:registry";
import * as schema from "@aave/schema/chains/mainnet";
import { ensureProtocol, ensureReserve, createReserveConfiguration, getReserveId, ensureUser } from "@/db/helpers";
import { AAVE_V3_CORE_POOL_ADDRESS, CHAIN_IDS } from "@/constants";

export function registerAaveMainnetCoreHandlers() {
  const pool = "AaveMainnetCorePool";
  const CHAIN_IDENTIFIER = "mainnet"; // Lowercase string used in IDs and DB references
  const CHAIN_ID = CHAIN_IDS.mainnet; // Numeric chain ID (1)
  const CHAIN_DISPLAY_NAME = "Mainnet"; // Display name
  const PROTOCOL_TYPE = "aave-core";
  const PROTOCOL_ID = `${PROTOCOL_TYPE}-${CHAIN_IDENTIFIER}`;
  
  const getTxHash = (event: any) => event.transaction?.hash || event.block.hash;

  // ===========================
  // SHARED EVENTS (Core + Horizon)
  // ===========================

  ponder.on(`${pool}:Borrow`, async ({ event, context }) => {
    // Ensure protocol exists
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");
    
    // Ensure reserve exists (metadata fetched from contract)
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);
    const onBehalfOfId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.onBehalfOf, event.block.number, event.block.timestamp);

    await context.db.insert(schema.AaveMainnetCoreSharedEvents.Borrow).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      userId,
      onBehalfOfId,
      user: event.args.user,
      onBehalfOf: event.args.onBehalfOf,
      amount: event.args.amount,
      interestRateMode: Number(event.args.interestRateMode),
      borrowRate: event.args.borrowRate,
      referralCode: Number(event.args.referralCode),
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  ponder.on(`${pool}:DeficitCovered`, async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const callerId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.caller, event.block.number, event.block.timestamp);

    await context.db.insert(schema.AaveMainnetCoreSharedEvents.DeficitCovered).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      callerId,
      caller: event.args.caller,
      amountCovered: event.args.amountCovered,
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  ponder.on(`${pool}:DeficitCreated`, async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.debtAsset,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);

    await context.db.insert(schema.AaveMainnetCoreSharedEvents.DeficitCreated).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      userId,
      user: event.args.user,
      amountCreated: event.args.amountCreated,
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  ponder.on(`${pool}:FlashLoan`, async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.asset,
      event.block.number,
      event.block.timestamp
    );

    const initiatorId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.initiator, event.block.number, event.block.timestamp);

    await context.db.insert(schema.AaveMainnetCoreSharedEvents.FlashLoan).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      initiatorId,
      target: event.args.target,
      initiator: event.args.initiator,
      amount: event.args.amount,
      interestRateMode: Number(event.args.interestRateMode),
      premium: event.args.premium,
      referralCode: Number(event.args.referralCode),
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  ponder.on(`${pool}:IsolationModeTotalDebtUpdated`, async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.asset,
      event.block.number,
      event.block.timestamp
    );

    await context.db.insert(schema.AaveMainnetCoreSharedEvents.IsolationModeTotalDebtUpdated).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      totalDebt: event.args.totalDebt,
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  ponder.on(`${pool}:LiquidationCall`, async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");
    
    const collateralReserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.collateralAsset,
      event.block.number,
      event.block.timestamp
    );
    
    const debtReserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.debtAsset,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);
    const liquidatorId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.liquidator, event.block.number, event.block.timestamp);

    await context.db.insert(schema.AaveMainnetCoreSharedEvents.LiquidationCall).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      collateralReserveId,
      debtReserveId,
      userId,
      liquidatorId,
      user: event.args.user,
      debtToCover: event.args.debtToCover,
      liquidatedCollateralAmount: event.args.liquidatedCollateralAmount,
      liquidator: event.args.liquidator,
      receiveAToken: event.args.receiveAToken,
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  ponder.on(`${pool}:MintedToTreasury`, async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    await context.db.insert(schema.AaveMainnetCoreSharedEvents.MintedToTreasury).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      amountMinted: event.args.amountMinted,
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  ponder.on(`${pool}:Repay`, async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);
    const repayerId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.repayer, event.block.number, event.block.timestamp);

    await context.db.insert(schema.AaveMainnetCoreSharedEvents.Repay).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      userId,
      repayerId,
      user: event.args.user,
      repayer: event.args.repayer,
      amount: event.args.amount,
      useATokens: event.args.useATokens,
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  ponder.on(`${pool}:ReserveDataUpdated`, async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );
    
    // Create ReserveConfiguration entry
    const reserveConfigurationId = await createReserveConfiguration(
      context,
      PROTOCOL_ID,
      reserveId,
      event.block.number,
      event.block.timestamp,
      getTxHash(event),
      event.args.liquidityRate,
      event.args.stableBorrowRate,
      event.args.variableBorrowRate,
      event.args.liquidityIndex,
      event.args.variableBorrowIndex
    );

    await context.db.insert(schema.AaveMainnetCoreSharedEvents.ReserveDataUpdated).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      reserveConfigurationId,
      liquidityRate: event.args.liquidityRate,
      stableBorrowRate: event.args.stableBorrowRate,
      variableBorrowRate: event.args.variableBorrowRate,
      liquidityIndex: event.args.liquidityIndex,
      variableBorrowIndex: event.args.variableBorrowIndex,
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  ponder.on(`${pool}:ReserveUsedAsCollateralDisabled`, async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);

    await context.db.insert(schema.AaveMainnetCoreSharedEvents.ReserveUsedAsCollateralDisabled).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      userId,
      user: event.args.user,
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  ponder.on(`${pool}:ReserveUsedAsCollateralEnabled`, async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);

    await context.db.insert(schema.AaveMainnetCoreSharedEvents.ReserveUsedAsCollateralEnabled).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      userId,
      user: event.args.user,
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  ponder.on(`${pool}:Supply`, async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);
    const onBehalfOfId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.onBehalfOf, event.block.number, event.block.timestamp);

    await context.db.insert(schema.AaveMainnetCoreSharedEvents.Supply).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      userId,
      onBehalfOfId,
      user: event.args.user,
      onBehalfOf: event.args.onBehalfOf,
      amount: event.args.amount,
      referralCode: Number(event.args.referralCode),
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  ponder.on(`${pool}:UserEModeSet`, async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);

    await context.db.insert(schema.AaveMainnetCoreSharedEvents.UserEModeSet).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      userId,
      user: event.args.user,
      categoryId: Number(event.args.categoryId),
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  ponder.on(`${pool}:Withdraw`, async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);
    const toId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.to, event.block.number, event.block.timestamp);

    await context.db.insert(schema.AaveMainnetCoreSharedEvents.Withdraw).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      userId,
      toId,
      user: event.args.user,
      to: event.args.to,
      amount: event.args.amount,
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    });
  });

  // ===========================
  // CORE-SPECIFIC EVENTS
  // ===========================

  ponder.on(`${pool}:PositionManagerApproved`, async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);

    await context.db.insert(schema.AaveMainnetCoreUniqueEvents.PositionManagerApproved).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      userId,
      user: event.args.user,
      positionManager: event.args.positionManager,
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  ponder.on(`${pool}:PositionManagerRevoked`, async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, AAVE_V3_CORE_POOL_ADDRESS, "Aave Core");

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);

    await context.db.insert(schema.AaveMainnetCoreUniqueEvents.PositionManagerRevoked).values({
      id: `aave-mainnet-core-${event.id}`,
      protocolId: PROTOCOL_ID,
      userId,
      user: event.args.user,
      positionManager: event.args.positionManager,
      timestamp: event.block.timestamp,
      transactionHash: getTxHash(event),
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });
}
