import { ponder } from "ponder:registry";
// import * as schema from "@aave/schema/chains/mainnet";
// import { handleSpTokenTransfer } from "@sparklend/utils/transfer-tracker";

/**
 * Register Aave Mainnet Core Market AToken Transfer Handlers
 * 
 * TODO: Uncomment and implement once AaveMainnetCoreAToken addresses are added to contracts.ts
 * This will enable portfolio snapshot tracking similar to Sparklend.
 */
export function registerAaveMainnetCoreATokenTransferHandlers() {
  // const chainId = "mainnet";

  // TODO: Uncomment when AaveMainnetCoreAToken is configured
  // ponder.on("AaveMainnetCoreAToken:Transfer", async ({ event, context }) => {
  //   const blockNumber = BigInt(event.block.number);
  //   const timestamp = BigInt(event.block.timestamp);

  //   await handleSpTokenTransfer(
  //     context,
  //     chainId,
  //     schema.AaveMainnetCoreUserScaledSupplyPosition,
  //     schema.AaveMainnetCoreUserSupplyPosition,
  //     schema.AaveMainnetCoreReserveDataUpdated,
  //     schema.AaveMainnetCoreActiveUser,
  //     event.log.address as `0x${string}`,
  //     event.args.from,
  //     event.args.to,
  //     event.args.value,
  //     blockNumber,
  //     timestamp
  //   );
  // });
}

