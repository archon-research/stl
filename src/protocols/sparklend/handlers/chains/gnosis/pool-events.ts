import { ponder } from "ponder:registry";
import {
  SparklendGnosisBackUnbacked,
  SparklendGnosisBorrow,
  SparklendGnosisFlashLoan,
  SparklendGnosisIsolationModeTotalDebtUpdated,
  SparklendGnosisLiquidationCall,
  SparklendGnosisMintUnbacked,
  SparklendGnosisMintedToTreasury,
  SparklendGnosisRepay,
  SparklendGnosisReserveDataUpdated,
  SparklendGnosisReserveUsedAsCollateralDisabled,
  SparklendGnosisReserveUsedAsCollateralEnabled,
  SparklendGnosisSupply,
  SparklendGnosisSwapBorrowRateMode,
  SparklendGnosisUserEModeSet,
  SparklendGnosisWithdraw,
  SparklendGnosisUserSupplyPosition,
  SparklendGnosisUserBorrowPosition,
  SparklendGnosisActiveUser,
  SparklendGnosisUserScaledSupplyPosition,
  SparklendGnosisUserScaledBorrowPosition,
  SparklendGnosisUserEModeCategory,
} from "@sparklend/schema/chains/gnosis";
import { ensureProtocol, ensureReserve, createReserveConfiguration, ensureUser } from "@/db/helpers";
import { SPARKLEND_GNOSIS_POOL_ADDRESS, CHAIN_IDS } from "@/constants";
import {
  handleSupplyChange,
  handleWithdrawChange,
  handleBorrowChange,
  handleRepayChange,
  handleCollateralToggle,
  handleLiquidation,
} from "@sparklend/utils/position-service";
import {
  trackScaledSupply,
  trackScaledWithdraw,
  trackScaledBorrow,
  trackScaledRepay,
  trackCollateralEnabled,
  trackCollateralDisabled,
  trackEModeSet,
} from "@sparklend/utils/position-tracker";
import {
  trackActiveUser,
} from "@sparklend/utils/health-factor-snapshot";

/**
 * Register Sparklend Gnosis Pool Event Handlers
 */
export function registerSparklendGnosisPoolEventHandlers() {
  const CHAIN_IDENTIFIER = "gnosis"; // Lowercase string used in IDs and DB references
  const CHAIN_ID = CHAIN_IDS.gnosis; // Numeric chain ID (100)
  const CHAIN_DISPLAY_NAME = "Gnosis"; // Display name
  const PROTOCOL_TYPE = "sparklend";
  const PROTOCOL_ID = `${PROTOCOL_TYPE}-${CHAIN_IDENTIFIER}`;

  // BackUnbacked Event
  ponder.on("SparklendGnosisPool:BackUnbacked", async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, SPARKLEND_GNOSIS_POOL_ADDRESS, "Sparklend");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const backerId = await ensureUser(
      context,
      CHAIN_IDENTIFIER,
      event.args.backer,
      event.block.number,
      event.block.timestamp
    );

    const txHash = event.transaction?.hash || event.block.hash;

    await context.db.insert(SparklendGnosisBackUnbacked).values({
      id: `sparklend-${CHAIN_IDENTIFIER}-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      backerId,
      backer: event.args.backer,
      amount: event.args.amount,
      fee: event.args.fee,
      timestamp: event.block.timestamp,
      transactionHash: txHash,
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  // Borrow Event
  ponder.on("SparklendGnosisPool:Borrow", async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, SPARKLEND_GNOSIS_POOL_ADDRESS, "Sparklend");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);
    const onBehalfOfId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.onBehalfOf, event.block.number, event.block.timestamp);

    const txHash = event.transaction?.hash || event.block.hash;
    const blockNumber = event.block.number;
    const timestamp = event.block.timestamp;

    await context.db.insert(SparklendGnosisBorrow).values({
      id: `sparklend-${CHAIN_IDENTIFIER}-${event.id}`,
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
      timestamp,
      transactionHash: txHash,
      blockNumber,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();

    await handleBorrowChange(
      context,
      CHAIN_IDENTIFIER,
      SparklendGnosisUserBorrowPosition,
      PROTOCOL_ID,
      reserveId,
      event.args.onBehalfOf,
      event.args.amount,
      Number(event.args.interestRateMode),
      blockNumber,
      timestamp
    );

    await trackScaledBorrow(
      context,
      CHAIN_IDENTIFIER,
      SparklendGnosisUserScaledBorrowPosition,
      SparklendGnosisReserveDataUpdated,
      PROTOCOL_ID,
      reserveId,
      event.args.onBehalfOf,
      event.args.amount,
      Number(event.args.interestRateMode),
      blockNumber,
      timestamp
    );

    await trackActiveUser(
      context,
      CHAIN_IDENTIFIER,
      PROTOCOL_ID,
      SparklendGnosisActiveUser,
      event.args.onBehalfOf,
      blockNumber,
      timestamp
    );
  });

  // FlashLoan Event
  ponder.on("SparklendGnosisPool:FlashLoan", async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, SPARKLEND_GNOSIS_POOL_ADDRESS, "Sparklend");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.asset,
      event.block.number,
      event.block.timestamp
    );

    const initiatorId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.initiator, event.block.number, event.block.timestamp);

    const txHash = event.transaction?.hash || event.block.hash;

    await context.db.insert(SparklendGnosisFlashLoan).values({
      id: `sparklend-${CHAIN_IDENTIFIER}-${event.id}`,
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
      transactionHash: txHash,
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  // IsolationModeTotalDebtUpdated Event
  ponder.on("SparklendGnosisPool:IsolationModeTotalDebtUpdated", async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, SPARKLEND_GNOSIS_POOL_ADDRESS, "Sparklend");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.asset,
      event.block.number,
      event.block.timestamp
    );

    const txHash = event.transaction?.hash || event.block.hash;

    await context.db.insert(SparklendGnosisIsolationModeTotalDebtUpdated).values({
      id: `sparklend-${CHAIN_IDENTIFIER}-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      totalDebt: event.args.totalDebt,
      timestamp: event.block.timestamp,
      transactionHash: txHash,
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  // LiquidationCall Event
  ponder.on("SparklendGnosisPool:LiquidationCall", async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, SPARKLEND_GNOSIS_POOL_ADDRESS, "Sparklend");
    
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

    const txHash = event.transaction?.hash || event.block.hash;
    const blockNumber = event.block.number;
    const timestamp = event.block.timestamp;

    await context.db.insert(SparklendGnosisLiquidationCall).values({
      id: `sparklend-${CHAIN_IDENTIFIER}-${event.id}`,
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
      timestamp,
      transactionHash: txHash,
      blockNumber,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();

    await handleLiquidation(
      context,
      SparklendGnosisUserSupplyPosition,
      SparklendGnosisUserBorrowPosition,
      PROTOCOL_ID,
      collateralReserveId,
      debtReserveId,
      event.args.user,
      event.args.liquidatedCollateralAmount,
      event.args.debtToCover,
      blockNumber,
      timestamp
    );

    await trackScaledWithdraw(
      context,
      CHAIN_IDENTIFIER,
      SparklendGnosisUserScaledSupplyPosition,
      SparklendGnosisReserveDataUpdated,
      PROTOCOL_ID,
      collateralReserveId,
      event.args.user,
      event.args.liquidatedCollateralAmount,
      blockNumber,
      timestamp
    );

    await trackScaledRepay(
      context,
      CHAIN_IDENTIFIER,
      SparklendGnosisUserScaledBorrowPosition,
      SparklendGnosisReserveDataUpdated,
      PROTOCOL_ID,
      debtReserveId,
      event.args.user,
      event.args.debtToCover,
      blockNumber,
      timestamp
    );

    await trackActiveUser(context, CHAIN_IDENTIFIER, PROTOCOL_ID, SparklendGnosisActiveUser, event.args.user, blockNumber, timestamp);
    await trackActiveUser(context, CHAIN_IDENTIFIER, PROTOCOL_ID, SparklendGnosisActiveUser, event.args.liquidator, blockNumber, timestamp);
  });

  // MintUnbacked Event
  ponder.on("SparklendGnosisPool:MintUnbacked", async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, SPARKLEND_GNOSIS_POOL_ADDRESS, "Sparklend");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);
    const onBehalfOfId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.onBehalfOf, event.block.number, event.block.timestamp);

    const txHash = event.transaction?.hash || event.block.hash;

    await context.db.insert(SparklendGnosisMintUnbacked).values({
      id: `sparklend-${CHAIN_IDENTIFIER}-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      userId,
      onBehalfOfId,
      user: event.args.user,
      onBehalfOf: event.args.onBehalfOf,
      amount: event.args.amount,
      referralCode: Number(event.args.referralCode),
      timestamp: event.block.timestamp,
      transactionHash: txHash,
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  // MintedToTreasury Event
  ponder.on("SparklendGnosisPool:MintedToTreasury", async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, SPARKLEND_GNOSIS_POOL_ADDRESS, "Sparklend");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const txHash = event.transaction?.hash || event.block.hash;

    await context.db.insert(SparklendGnosisMintedToTreasury).values({
      id: `sparklend-${CHAIN_IDENTIFIER}-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      amountMinted: event.args.amountMinted,
      timestamp: event.block.timestamp,
      transactionHash: txHash,
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  // Repay Event
  ponder.on("SparklendGnosisPool:Repay", async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, SPARKLEND_GNOSIS_POOL_ADDRESS, "Sparklend");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);
    const repayerId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.repayer, event.block.number, event.block.timestamp);

    const txHash = event.transaction?.hash || event.block.hash;
    const blockNumber = event.block.number;
    const timestamp = event.block.timestamp;

    await context.db.insert(SparklendGnosisRepay).values({
      id: `sparklend-${CHAIN_IDENTIFIER}-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      userId,
      repayerId,
      user: event.args.user,
      repayer: event.args.repayer,
      amount: event.args.amount,
      useATokens: event.args.useATokens,
      timestamp,
      transactionHash: txHash,
      blockNumber,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();

    await handleRepayChange(
      context,
      SparklendGnosisUserBorrowPosition,
      PROTOCOL_ID,
      reserveId,
      event.args.user,
      event.args.amount,
      blockNumber,
      timestamp
    );

    await trackScaledRepay(
      context,
      CHAIN_IDENTIFIER,
      SparklendGnosisUserScaledBorrowPosition,
      SparklendGnosisReserveDataUpdated,
      PROTOCOL_ID,
      reserveId,
      event.args.user,
      event.args.amount,
      blockNumber,
      timestamp
    );

    await trackActiveUser(context, CHAIN_IDENTIFIER, PROTOCOL_ID, SparklendGnosisActiveUser, event.args.user, blockNumber, timestamp);
  });

  // ReserveDataUpdated Event
  ponder.on("SparklendGnosisPool:ReserveDataUpdated", async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, SPARKLEND_GNOSIS_POOL_ADDRESS, "Sparklend");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );
    
    // Create ReserveConfiguration entry
    const txHash = event.transaction?.hash || event.block.hash;
    const reserveConfigurationId = await createReserveConfiguration(
      context,
      PROTOCOL_ID,
      reserveId,
      event.block.number,
      event.block.timestamp,
      txHash,
      event.args.liquidityRate,
      event.args.stableBorrowRate,
      event.args.variableBorrowRate,
      event.args.liquidityIndex,
      event.args.variableBorrowIndex
    );

    const blockNumber = event.block.number;
    const timestamp = event.block.timestamp;

    await context.db.insert(SparklendGnosisReserveDataUpdated).values({
      id: `sparklend-${CHAIN_IDENTIFIER}-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      reserveConfigurationId,
      liquidityRate: event.args.liquidityRate,
      stableBorrowRate: event.args.stableBorrowRate,
      variableBorrowRate: event.args.variableBorrowRate,
      liquidityIndex: event.args.liquidityIndex,
      variableBorrowIndex: event.args.variableBorrowIndex,
      timestamp,
      transactionHash: txHash,
      blockNumber,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  // ReserveUsedAsCollateralDisabled Event
  ponder.on("SparklendGnosisPool:ReserveUsedAsCollateralDisabled", async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, SPARKLEND_GNOSIS_POOL_ADDRESS, "Sparklend");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);

    const txHash = event.transaction?.hash || event.block.hash;
    const blockNumber = event.block.number;
    const timestamp = event.block.timestamp;

    await context.db.insert(SparklendGnosisReserveUsedAsCollateralDisabled).values({
      id: `sparklend-${CHAIN_IDENTIFIER}-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      userId,
      user: event.args.user,
      timestamp,
      transactionHash: txHash,
      blockNumber,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();

    await handleCollateralToggle(
      context,
      SparklendGnosisUserSupplyPosition,
      PROTOCOL_ID,
      reserveId,
      event.args.user,
      false,
      blockNumber,
      timestamp
    );

    await trackCollateralDisabled(
      context,
      CHAIN_IDENTIFIER,
      SparklendGnosisUserScaledSupplyPosition,
      PROTOCOL_ID,
      reserveId,
      event.args.user,
      blockNumber,
      timestamp
    );

    await trackActiveUser(context, CHAIN_IDENTIFIER, PROTOCOL_ID, SparklendGnosisActiveUser, event.args.user, blockNumber, timestamp);
  });

  // ReserveUsedAsCollateralEnabled Event
  ponder.on("SparklendGnosisPool:ReserveUsedAsCollateralEnabled", async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, SPARKLEND_GNOSIS_POOL_ADDRESS, "Sparklend");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);

    const txHash = event.transaction?.hash || event.block.hash;
    const blockNumber = event.block.number;
    const timestamp = event.block.timestamp;

    await context.db.insert(SparklendGnosisReserveUsedAsCollateralEnabled).values({
      id: `sparklend-${CHAIN_IDENTIFIER}-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      userId,
      user: event.args.user,
      timestamp,
      transactionHash: txHash,
      blockNumber,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();

    await handleCollateralToggle(
      context,
      SparklendGnosisUserSupplyPosition,
      PROTOCOL_ID,
      reserveId,
      event.args.user,
      true,
      blockNumber,
      timestamp
    );

    await trackCollateralEnabled(
      context,
      CHAIN_IDENTIFIER,
      SparklendGnosisUserScaledSupplyPosition,
      PROTOCOL_ID,
      reserveId,
      event.args.user,
      blockNumber,
      timestamp
    );

    await trackActiveUser(context, CHAIN_IDENTIFIER, PROTOCOL_ID, SparklendGnosisActiveUser, event.args.user, blockNumber, timestamp);
  });

  // Supply Event
  ponder.on("SparklendGnosisPool:Supply", async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, SPARKLEND_GNOSIS_POOL_ADDRESS, "Sparklend");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);
    const onBehalfOfId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.onBehalfOf, event.block.number, event.block.timestamp);

    const txHash = event.transaction?.hash || event.block.hash;
    const blockNumber = event.block.number;
    const timestamp = event.block.timestamp;

    await context.db.insert(SparklendGnosisSupply).values({
      id: `sparklend-${CHAIN_IDENTIFIER}-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      userId,
      onBehalfOfId,
      user: event.args.user,
      onBehalfOf: event.args.onBehalfOf,
      amount: event.args.amount,
      referralCode: Number(event.args.referralCode),
      timestamp,
      transactionHash: txHash,
      blockNumber,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();

    await handleSupplyChange(
      context,
      CHAIN_IDENTIFIER,
      SparklendGnosisUserSupplyPosition,
      PROTOCOL_ID,
      reserveId,
      event.args.onBehalfOf,
      event.args.amount,
      blockNumber,
      timestamp
    );

    await trackScaledSupply(
      context,
      CHAIN_IDENTIFIER,
      SparklendGnosisUserScaledSupplyPosition,
      SparklendGnosisReserveDataUpdated,
      PROTOCOL_ID,
      reserveId,
      event.args.onBehalfOf,
      event.args.amount,
      blockNumber,
      timestamp
    );

    await trackActiveUser(context, CHAIN_IDENTIFIER, PROTOCOL_ID, SparklendGnosisActiveUser, event.args.onBehalfOf, blockNumber, timestamp);
  });

  // SwapBorrowRateMode Event
  ponder.on("SparklendGnosisPool:SwapBorrowRateMode", async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, SPARKLEND_GNOSIS_POOL_ADDRESS, "Sparklend");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);

    const txHash = event.transaction?.hash || event.block.hash;

    await context.db.insert(SparklendGnosisSwapBorrowRateMode).values({
      id: `sparklend-${CHAIN_IDENTIFIER}-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      userId,
      user: event.args.user,
      interestRateMode: Number(event.args.interestRateMode),
      timestamp: event.block.timestamp,
      transactionHash: txHash,
      blockNumber: event.block.number,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();
  });

  // UserEModeSet Event
  ponder.on("SparklendGnosisPool:UserEModeSet", async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, SPARKLEND_GNOSIS_POOL_ADDRESS, "Sparklend");

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);

    const txHash = event.transaction?.hash || event.block.hash;
    const blockNumber = event.block.number;
    const timestamp = event.block.timestamp;

    await context.db.insert(SparklendGnosisUserEModeSet).values({
      id: `sparklend-${CHAIN_IDENTIFIER}-${event.id}`,
      protocolId: PROTOCOL_ID,
      userId,
      user: event.args.user,
      categoryId: Number(event.args.categoryId),
      timestamp,
      transactionHash: txHash,
      blockNumber,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();

    await trackEModeSet(
      context,
      CHAIN_IDENTIFIER,
      SparklendGnosisUserEModeCategory,
      PROTOCOL_ID,
      event.args.user,
      Number(event.args.categoryId),
      blockNumber,
      timestamp
    );

    await trackActiveUser(context, CHAIN_IDENTIFIER, PROTOCOL_ID, SparklendGnosisActiveUser, event.args.user, blockNumber, timestamp);
  });

  // Withdraw Event
  ponder.on("SparklendGnosisPool:Withdraw", async ({ event, context }) => {
    await ensureProtocol(context, PROTOCOL_TYPE, CHAIN_IDENTIFIER, CHAIN_ID, CHAIN_DISPLAY_NAME, SPARKLEND_GNOSIS_POOL_ADDRESS, "Sparklend");
    
    const reserveId = await ensureReserve(
      context,
      CHAIN_IDENTIFIER,
      event.args.reserve,
      event.block.number,
      event.block.timestamp
    );

    const userId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.user, event.block.number, event.block.timestamp);
    const toId = await ensureUser(context, CHAIN_IDENTIFIER, event.args.to, event.block.number, event.block.timestamp);

    const txHash = event.transaction?.hash || event.block.hash;
    const blockNumber = event.block.number;
    const timestamp = event.block.timestamp;

    await context.db.insert(SparklendGnosisWithdraw).values({
      id: `sparklend-${CHAIN_IDENTIFIER}-${event.id}`,
      protocolId: PROTOCOL_ID,
      reserveId,
      userId,
      toId,
      user: event.args.user,
      to: event.args.to,
      amount: event.args.amount,
      timestamp,
      transactionHash: txHash,
      blockNumber,
      logIndex: BigInt(event.log.logIndex),
    }).onConflictDoNothing();

    await handleWithdrawChange(
      context,
      SparklendGnosisUserSupplyPosition,
      PROTOCOL_ID,
      reserveId,
      event.args.user,
      event.args.amount,
      blockNumber,
      timestamp
    );

    await trackScaledWithdraw(
      context,
      CHAIN_IDENTIFIER,
      SparklendGnosisUserScaledSupplyPosition,
      SparklendGnosisReserveDataUpdated,
      PROTOCOL_ID,
      reserveId,
      event.args.user,
      event.args.amount,
      blockNumber,
      timestamp
    );

    await trackActiveUser(context, CHAIN_IDENTIFIER, PROTOCOL_ID, SparklendGnosisActiveUser, event.args.user, blockNumber, timestamp);
  });
}
